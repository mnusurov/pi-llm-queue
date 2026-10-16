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

	it("releases a held slot when its holder disconnects", async () => {
		const holder = await connectClient(d.socketPath);
		const waiter = await connectClient(d.socketPath);

		holder.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "HH", role: "parent" });
		assert.deepEqual(await holder.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "HH" });

		waiter.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "W", role: "parent" });
		await assert.rejects(waiter.next(200), /no message from daemon within/);

		holder.close(); // disconnect while holding: the slot must free for W
		assert.deepEqual(await waiter.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "W" });
		waiter.close();
	});

	it("answers renew with ok", async () => {
		const c = await connectClient(d.socketPath);
		c.send({ op: "renew", queueKey: QUEUE_KEY, sessionKey: "A" });
		assert.deepEqual(await c.next(3000), { op: "ok" });
		c.close();
	});

	it("drops a queued request on cancel without granting it later", async () => {
		const holder = await connectClient(d.socketPath);
		const canceller = await connectClient(d.socketPath);
		const next = await connectClient(d.socketPath);

		holder.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "H", role: "parent" });
		assert.deepEqual(await holder.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "H" });

		canceller.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "X", role: "parent" });
		next.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "N", role: "parent" });
		await assert.rejects(next.next(200), /no message from daemon within/);

		canceller.send({ op: "cancel", queueKey: QUEUE_KEY, sessionKey: "X" });
		holder.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "H" });
		// X was cancelled: N (queued after X) takes the slot, not X.
		assert.deepEqual(await next.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "N" });
		await assert.rejects(canceller.next(300), /no message from daemon within/);
		holder.close();
		canceller.close();
		next.close();
	});
});

describe("queue policy resolution", () => {
	it("applies a provider-name queue key to that provider's endpoint", async () => {
		const d = startDaemon(
			{ defaults: { maxStreams: 1 }, queues: { zen: { maxStreams: 2 } } },
			{ providers: { zen: { baseUrl: QUEUE_KEY, api: "openai-completions" } } },
		);
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);

		a.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "A", role: "parent" });
		b.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "B", role: "parent" });
		const granted = [(await a.next(3000)).sessionKey, (await b.next(3000)).sessionKey];
		assert.deepEqual(granted.sort(), ["A", "B"], "both slots of the resolved policy are usable at once");

		a.close();
		b.close();
		d.stop();
	});

	it("status reports queues without creating one for a keyless request", async () => {
		const d = startDaemon();
		const c = await connectClient(d.socketPath);
		c.send({ op: "status" });
		assert.deepEqual((await c.next(3000)).queues, []);
		c.close();
		d.stop();
	});
});
