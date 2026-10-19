/// <reference types="@rbxts/testez/globals" />

import { Connection, Signal } from "..";

// deferred work runs in the order it was queued, so this resumes after everything deferred so far
function flushDeferred(): void {
	const thread = coroutine.running();
	task.defer(() => task.spawn(thread));
	coroutine.yield();
}

export = () => {
	describe("connect and fire", () => {
		it("passes the fired arguments to handlers", () => {
			const signal = new Signal<[name: string, amount: number, flag: boolean]>();
			let received: [string, number, boolean] | undefined;

			signal.connect((name, amount, flag) => {
				received = [name, amount, flag];
			});
			signal.fire("a", 2, true);

			expect(received).to.be.ok();
			expect(received![0]).to.equal("a");
			expect(received![1]).to.equal(2);
			expect(received![2]).to.equal(true);
		});

		it("passes tables by reference", () => {
			const signal = new Signal<[payload: object]>();
			const payload = {};
			let received: object | undefined;

			signal.connect((value) => {
				received = value;
			});
			signal.fire(payload);

			expect(received).to.equal(payload);
		});

		it("runs handlers in the order they were connected", () => {
			const signal = new Signal();
			const order: number[] = [];

			for (let index = 1; index <= 5; index++) {
				signal.connect(() => order.push(index));
			}
			signal.fire();

			expect(order.join(",")).to.equal("1,2,3,4,5");
		});
	});

	describe("disconnect", () => {
		it("stops calling a handler once disconnected", () => {
			const signal = new Signal();
			let calls = 0;

			const connection = signal.connect(() => calls++);
			signal.fire();
			connection.disconnect();
			connection.disconnect();
			signal.fire();

			expect(calls).to.equal(1);
			expect(connection.connected).to.equal(false);
		});

		it("keeps firing the remaining handlers when one disconnects itself", () => {
			const signal = new Signal();
			const order: string[] = [];

			signal.connect(() => order.push("first"));
			const selfDisconnecting: Connection = signal.connect(() => {
				selfDisconnecting.disconnect();
				order.push("second");
			});
			signal.connect(() => order.push("third"));

			signal.fire();
			signal.fire();

			expect(order.join(",")).to.equal("first,second,third,first,third");
		});

		it("skips a handler disconnected earlier in the same fire", () => {
			const signal = new Signal();
			let laterCalled = false;

			signal.connect(() => later.disconnect());
			const later = signal.connect(() => {
				laterCalled = true;
			});
			signal.fire();

			expect(laterCalled).to.equal(false);
		});

		it("does not run handlers connected during a fire until the next one", () => {
			const signal = new Signal();
			let lateCalls = 0;
			let connectedLate = false;

			signal.connect(() => {
				if (!connectedLate) {
					connectedLate = true;
					signal.connect(() => lateCalls++);
				}
			});

			signal.fire();
			expect(lateCalls).to.equal(0);

			signal.fire();
			expect(lateCalls).to.equal(1);
		});

		it("does not run new handlers when the old tail is disconnected mid-fire", () => {
			const signal = new Signal();
			let newCalled = false;

			signal.connect(() => {
				tail.disconnect();
				signal.connect(() => {
					newCalled = true;
				});
			});
			const tail = signal.connect(() => {});

			signal.fire();

			expect(newCalled).to.equal(false);
		});

		it("disconnects everything with disconnectAll, including mid-fire", () => {
			const signal = new Signal();
			let secondCalled = false;

			const first = signal.connect(() => signal.disconnectAll());
			const second = signal.connect(() => {
				secondCalled = true;
			});
			signal.fire();

			expect(secondCalled).to.equal(false);
			expect(first.connected).to.equal(false);
			expect(second.connected).to.equal(false);
		});

		it("accepts new connections after destroy", () => {
			const signal = new Signal();
			let calls = 0;

			signal.connect(() => {});
			signal.destroy();
			signal.connect(() => calls++);
			signal.fire();

			expect(calls).to.equal(1);
		});
	});

	describe("once and wait", () => {
		it("runs once handlers a single time, even when fired from inside the handler", () => {
			const signal = new Signal();
			let calls = 0;

			const connection = signal.once(() => {
				calls++;
				signal.fire();
			});
			signal.fire();
			signal.fire();

			expect(calls).to.equal(1);
			expect(connection.connected).to.equal(false);
		});

		it("resumes wait with the next fire's arguments", () => {
			const signal = new Signal<[status: string, code: number]>();
			let result: [string, number] | undefined;

			task.spawn(() => {
				result = signal.wait();
			});

			expect(result).never.to.be.ok();
			signal.fire("done", 42);

			expect(result).to.be.ok();
			expect(result![0]).to.equal("done");
			expect(result![1]).to.equal(42);
		});
	});

	describe("fireDeferred", () => {
		it("runs handlers on the next resumption cycle", () => {
			const signal = new Signal<[value: string]>();
			let received: string | undefined;

			signal.connect((value) => {
				received = value;
			});
			signal.fireDeferred("later");

			expect(received).never.to.be.ok();
			flushDeferred();
			expect(received).to.equal("later");
		});

		it("skips handlers disconnected before they ran", () => {
			const signal = new Signal();
			let called = false;

			const connection = signal.connect(() => {
				called = true;
			});
			signal.fireDeferred();
			connection.disconnect();
			flushDeferred();

			expect(called).to.equal(false);
		});
	});

	describe("threads", () => {
		it("lets other handlers run while one is yielded", () => {
			const signal = new Signal();
			const order: string[] = [];
			let slowThread: thread | undefined;

			signal.connect(() => {
				order.push("slow start");
				slowThread = coroutine.running();
				coroutine.yield();
				order.push("slow end");
			});
			signal.connect(() => order.push("fast"));

			signal.fire();
			expect(order.join(",")).to.equal("slow start,fast");

			task.spawn(slowThread!);
			expect(order.join(",")).to.equal("slow start,fast,slow end");
		});

		it("reuses one thread for handlers that do not yield", () => {
			const signal = new Signal();
			const threads: thread[] = [];

			for (let index = 0; index < 3; index++) {
				signal.connect(() => threads.push(coroutine.running()));
			}
			signal.fire();

			expect(threads.size()).to.equal(3);
			expect(threads[0]).to.equal(threads[1]);
			expect(threads[1]).to.equal(threads[2]);
		});

		it("moves on to a fresh thread when a handler yields", () => {
			const signal = new Signal();
			let yieldingThread: thread | undefined;
			let nextThread: thread | undefined;

			signal.connect(() => {
				yieldingThread = coroutine.running();
				coroutine.yield();
			});
			signal.connect(() => {
				nextThread = coroutine.running();
			});
			signal.fire();

			expect(yieldingThread).to.be.ok();
			expect(nextThread).to.be.ok();
			expect(nextThread).never.to.equal(yieldingThread);
		});
	});
};
