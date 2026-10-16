import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { connectClient, sleep, startDaemon, QUEUE_KEY, type Client, type Daemon } from "./harness.ts";

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

// Lease lifecycle: TTL reclaim, heartbeat, and the session-switch quanta. Asserted through
// the socket seam only. Daemons/clients are registered for cleanup so a failing assert
// cannot leave them alive and hang the runner.
describe("lease lifecycle and session switch", () => {
	const daemons: Daemon[] = [];
	const clients: Client[] = [];

	const startQuanta = (quanta: Record<string, number>) => {
		const d = startDaemon({
			defaults: { maxStreams: 1, leaseTtlMs: 60000, idleQuantumMs: 60000, monopolyRequests: 1000, monopolyMs: 60000, ...quanta },
			queues: {},
		});
		daemons.push(d);
		return d;
	};
	const open = async (d: Daemon) => {
		const c = await connectClient(d.socketPath);
		clients.push(c);
		return c;
	};
	const acquire = (c: Client, sessionKey: string) => c.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey, role: "parent" });
	const beat = (c: Client, sessionKey: string) => c.send({ op: "renew", queueKey: QUEUE_KEY, sessionKey });
	const granted = (sessionKey: string) => ({ op: "granted", queueKey: QUEUE_KEY, sessionKey });

	after(() => {
		for (const c of clients) c.close();
		for (const d of daemons) d.stop();
	});

	it("reclaims a crashed session's lease after the TTL and grants the next waiter", async () => {
		const d = startQuanta({ leaseTtlMs: 300 });
		const a = await open(d); // live socket, abandoned lease: never renews
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B");
		await assert.rejects(b.next(300), /no message from daemon within/);

		assert.deepEqual(await b.next(5000), granted("B"));
	});

	it("keeps a lease alive while its client heartbeats, and reclaims it when heartbeats stop", async () => {
		const d = startQuanta({ leaseTtlMs: 400 });
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B");

		for (let i = 0; i < 10; i++) {
			await sleep(150); // ~3.7 TTLs of heartbeats
			a.send({ op: "renew" });
		}
		await assert.rejects(b.next(300), /no message from daemon within/, "heartbeats keep A's slot");

		for (let i = 0; i < 6; i++) await sleep(150); // heartbeats stop: the TTL reclaims
		assert.deepEqual(await b.next(5000), granted("B"));
	});

	it("idle quantum: a holder that stops requesting yields the slot to a waiter", async () => {
		const d = startQuanta({ idleQuantumMs: 300 });
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B");
		await assert.rejects(b.next(200), /no message from daemon within/);

		assert.deepEqual(await b.next(5000), granted("B"));
	});

	it("activity beats retain the slot across a long tool execution (no idle eviction)", async () => {
		const d = startQuanta({ idleQuantumMs: 400 });
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B");

		for (let i = 0; i < 12; i++) {
			await sleep(150); // ~4.5 idle quanta of activity
			beat(a, "A");
		}
		await assert.rejects(b.next(400), /no message from daemon within/, "an active holder keeps the slot");
	});

	it("monopoly quantum: N requests force a session switch", async () => {
		const d = startQuanta({ monopolyRequests: 3 });
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B");
		await assert.rejects(b.next(200), /no message from daemon within/);

		for (let i = 0; i < 3; i++) {
			beat(a, "A");
			await sleep(150);
		}
		assert.deepEqual(await b.next(5000), granted("B"));
	});

	it("monopoly time quantum: M seconds of monopoly forces a session switch", async () => {
		const d = startQuanta({ monopolyMs: 400 });
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B");

		for (let i = 0; i < 10; i++) {
			await sleep(150); // keeps TTL and idle alive: only the time quantum can switch
			beat(a, "A");
		}
		assert.deepEqual(await b.next(5000), granted("B"));
	});

	it("a monopoly-evicted session is granted again once the queue drains", async () => {
		const d = startQuanta({ monopolyRequests: 2 });
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		beat(a, "A");
		beat(a, "A");
		for (let i = 0; i < 2; i++) assert.deepEqual(await a.next(3000), { op: "ok" }); // drain beat replies
		acquire(b, "B");
		assert.deepEqual(await b.next(5000), granted("B"));

		b.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "B" });
		acquire(a, "A"); // A's next run re-acquires: the monopoly count resets on grant
		assert.deepEqual(await a.next(5000), granted("A"));
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
