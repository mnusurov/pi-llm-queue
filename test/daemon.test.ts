import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { connectClient, startDaemon, QUEUE_KEY, type Client, type Daemon } from "./harness.ts";

describe("daemon socket protocol", () => {
	let d: Daemon;
	let a: Client;
	let b: Client;

	before(async () => {
		d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		a = await connectClient(d.socketPath);
		b = await connectClient(d.socketPath);
	});

	after(() => d.stop());

	it("serialises two sessions: second waits while first holds the slot, then is granted on release", async () => {
		a.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "A", role: "parent" });
		assert.deepEqual(await a.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "A" });

		b.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "B", role: "parent" });
		await assert.rejects(b.next(300), /no message from daemon within/);

		a.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "A" });
		assert.deepEqual(await b.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "B" });
	});

	it("grants queued sessions in arrival order (FIFO)", async () => {
		const c = await connectClient(d.socketPath);
		const e = await connectClient(d.socketPath);

		b.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "B", role: "parent" });
		c.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "C", role: "parent" });
		e.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "D", role: "parent" });
		await assert.rejects(c.next(200), /no message from daemon within/);

		const order = [];
		const clients = [b, c, e];
		for (let i = 0; i < 3; i++) {
			const granted = await clients[i].next(3000);
			order.push(granted.sessionKey);
			clients[i].send({ op: "release", queueKey: QUEUE_KEY, sessionKey: granted.sessionKey });
		}
		assert.deepEqual(order, ["B", "C", "D"]);
		c.close();
		e.close();
	});

	it("drops a queued session that disconnects, so the next waiter takes its place", async () => {
		const holder = await connectClient(d.socketPath);
		const impatient = await connectClient(d.socketPath);
		const patient = await connectClient(d.socketPath);

		holder.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "H", role: "parent" });
		assert.deepEqual(await holder.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "H" });

		patient.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "P", role: "parent" });
		impatient.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "I", role: "parent" });
		await assert.rejects(impatient.next(200), /no message from daemon within/);
		impatient.close();

		holder.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "H" });
		assert.deepEqual(await patient.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "P" });
		holder.close();
		patient.close();
	});
});
