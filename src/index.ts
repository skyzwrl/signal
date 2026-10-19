type Handler<T extends unknown[]> = (...args: T) => void;

export interface Connection {
	readonly connected: boolean;
	disconnect(): void;
}

class ConnectionList<T extends unknown[]> {
	public head?: ConnectionNode<T>;
	public tail?: ConnectionNode<T>;
	public lastId = 0;
}

class ConnectionNode<T extends unknown[]> implements Connection {
	public connected = true;
	public prev?: ConnectionNode<T>;
	public next?: ConnectionNode<T>;

	public constructor(
		public readonly list: ConnectionList<T>,
		public readonly handler: Handler<T>,
		public readonly id: number,
	) {}

	public disconnect(): void {
		if (!this.connected) {
			return;
		}
		this.connected = false;

		const { list, prev: before, next: after } = this;
		if (before) {
			before.next = after;
		} else {
			list.head = after;
		}
		if (after) {
			after.prev = before;
		} else {
			list.tail = before;
		}

		// keep next so a fire paused on this node (or yet to reach it) can still walk the rest
		this.prev = undefined;
	}
}

// handlers that return without yielding hand the thread back, so a fire usually reuses one coroutine
// for every handler. args travel as the table fire already packed instead of being repacked per call
let freeRunnerThread: thread | undefined;

function runHandler(handler: Callback, args: unknown[]): void {
	const runnerThread = freeRunnerThread;
	freeRunnerThread = undefined;
	handler(...args);
	// not reached if the handler errored, that thread is dead and gets replaced next fire
	freeRunnerThread = runnerThread;
}

function runnerLoop(handler: Callback, args: unknown[]): void {
	runHandler(handler, args);
	while (true) {
		const [nextHandler, nextArgs] = coroutine.yield();
		runHandler(nextHandler as Callback, nextArgs as unknown[]);
	}
}

function runIfConnected<T extends unknown[]>(node: ConnectionNode<T>, args: T): void {
	if (node.connected) {
		node.handler(...args);
	}
}

export class Signal<T extends unknown[] = []> {
	private readonly list = new ConnectionList<T>();

	public connect(handler: Handler<T>): Connection {
		const list = this.list;
		list.lastId += 1;

		const node = new ConnectionNode(list, handler, list.lastId);
		const tail = list.tail;
		if (tail) {
			tail.next = node;
			node.prev = tail;
		} else {
			list.head = node;
		}
		list.tail = node;

		return node;
	}

	public once(handler: Handler<T>): Connection {
		const connection = this.connect((...args) => {
			connection.disconnect();
			handler(...args);
		});
		return connection;
	}

	public wait(): T {
		const thread = coroutine.running();
		this.once((...args) => {
			task.spawn(thread, args);
		});
		const [args] = coroutine.yield();
		return args as T;
	}

	public fire(...args: T): void {
		const lastId = this.list.lastId;
		let node = this.list.head;

		// ids only grow and new nodes go on the tail, so anything newer than lastId was connected mid fire
		while (node && node.id <= lastId) {
			if (node.connected) {
				if (freeRunnerThread === undefined) {
					freeRunnerThread = coroutine.create(runnerLoop);
				}
				task.spawn(freeRunnerThread, node.handler, args);
			}
			node = node.next;
		}
	}

	public fireDeferred(...args: T): void {
		let node = this.list.head;
		while (node) {
			task.defer(runIfConnected, node, args);
			node = node.next;
		}
	}

	public disconnectAll(): void {
		let node = this.list.head;
		while (node) {
			node.connected = false;
			node.prev = undefined;
			node = node.next;
		}
		this.list.head = undefined;
		this.list.tail = undefined;
	}

	public destroy(): void {
		this.disconnectAll();
	}
}
