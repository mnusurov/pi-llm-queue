import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";
import { closeAllClients, connectClient, sleep, startDaemon, stopAllDaemons, DAEMON, QUEUE_KEY, type Client, type Daemon } from "./harness.ts";

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
		assert.deepEqual(await c.next(3000), { op: "ok", queueKey: QUEUE_KEY, sessionKey: "A", granted: false });
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
	const QUEUE_KEY2 = "http://model.test/v2";

	after(() => {
		for (const c of clients) c.close();
		for (const d of daemons) d.stop();
		closeAllClients();
		stopAllDaemons();
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

	it("answers a keyed beat with the authoritative grant state", async () => {
		const d = startQuanta({});
		const a = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		beat(a, "A");
		assert.deepEqual(await a.next(3000), { op: "ok", queueKey: QUEUE_KEY, sessionKey: "A", granted: true }, "a beat on a held slot is acknowledged");

		a.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "A" });
		assert.deepEqual(await a.next(3000), { op: "ok" });
		beat(a, "A");
		assert.deepEqual(await a.next(3000), { op: "ok", queueKey: QUEUE_KEY, sessionKey: "A", granted: false }, "a beat on a lost slot tells the client to re-queue");
	});

	it("releases every lease of a client that disconnects (no phantom holder)", async () => {
		const d = startQuanta({});
		const multi = await open(d); // one client holding two sessions on two endpoints
		const w1 = await open(d);
		const w2 = await open(d);

		acquire(multi, "M1");
		assert.deepEqual(await multi.next(3000), granted("M1"));
		multi.send({ op: "acquire", queueKey: QUEUE_KEY2, sessionKey: "M2", role: "parent" });
		assert.deepEqual(await multi.next(3000), { op: "granted", queueKey: QUEUE_KEY2, sessionKey: "M2" });

		acquire(w1, "W1");
		w2.send({ op: "acquire", queueKey: QUEUE_KEY2, sessionKey: "W2", role: "parent" });
		await assert.rejects(w1.next(200), /no message from daemon within/);

		multi.close(); // disconnect while holding BOTH leases: both slots must free
		assert.deepEqual(await w1.next(5000), granted("W1"));
		assert.deepEqual(await w2.next(5000), { op: "granted", queueKey: QUEUE_KEY2, sessionKey: "W2" });
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
		for (let i = 0; i < 2; i++) assert.deepEqual(await a.next(3000), { op: "ok", queueKey: QUEUE_KEY, sessionKey: "A", granted: true }); // drain beat replies
		acquire(b, "B");
		assert.deepEqual(await b.next(5000), granted("B"));

		b.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "B" });
		acquire(a, "A"); // A's next run re-acquires: the monopoly count resets on grant
		assert.deepEqual(await a.next(5000), granted("A"));
	});
});

describe("priority tiers and aging", () => {
	const daemons: Daemon[] = [];
	const clients: Client[] = [];
	const open = async (d: Daemon) => {
		const c = await connectClient(d.socketPath);
		clients.push(c);
		return c;
	};
	const acquire = (c: Client, sessionKey: string, role: string) => c.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey, role });
	const release = (c: Client, sessionKey: string) => c.send({ op: "release", queueKey: QUEUE_KEY, sessionKey });
	const granted = (sessionKey: string) => ({ op: "granted", queueKey: QUEUE_KEY, sessionKey });

	after(() => {
		for (const c of clients) c.close();
		for (const d of daemons) d.stop();
		closeAllClients();
		stopAllDaemons();
	});

	it("grants a queued parent ahead of an earlier-queued child", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		daemons.push(d);
		const h = await open(d);
		const child = await open(d);
		const parent = await open(d);

		acquire(h, "H", "parent");
		assert.deepEqual(await h.next(3000), granted("H"));
		acquire(child, "C", "child");
		acquire(parent, "P", "parent");
		await assert.rejects(parent.next(200), /no message from daemon within/);

		release(h, "H");
		assert.deepEqual(await parent.next(3000), granted("P"), "parent outranks the earlier-queued child");
		release(parent, "P");
		assert.deepEqual(await child.next(3000), granted("C"));
	});

	it("grants queued parents in arrival order (FIFO within tier)", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		daemons.push(d);
		const h = await open(d);
		const a = await open(d);
		const b = await open(d);
		const e = await open(d);

		acquire(h, "H", "parent");
		assert.deepEqual(await h.next(3000), granted("H"));
		acquire(a, "A", "parent");
		acquire(b, "B", "parent");
		acquire(e, "C", "parent");
		await assert.rejects(a.next(200), /no message from daemon within/);

		const order: string[] = [];
		release(h, "H");
		for (const cl of [a, b, e]) {
			const g = await cl.next(3000);
			order.push(g.sessionKey);
			release(cl, g.sessionKey);
		}
		assert.deepEqual(order, ["A", "B", "C"]);
	});

	it("boosts a child that has waited past agingAfterMs ahead of a parent", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1, agingAfterMs: 300, agingAfterSlots: 1000 }, queues: {} });
		daemons.push(d);
		const h = await open(d);
		const child = await open(d);
		const parent = await open(d);

		acquire(h, "H", "parent");
		assert.deepEqual(await h.next(3000), granted("H"));
		acquire(child, "C", "child");
		acquire(parent, "P", "parent");
		await sleep(400); // child ages past agingAfterMs

		release(h, "H");
		assert.deepEqual(await child.next(3000), granted("C"), "aged child jumps ahead of the parent");
	});

	it("boosts the longest-waiting child after agingAfterSlots consecutive parent grants", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1, agingAfterMs: 100000, agingAfterSlots: 2 }, queues: {} });
		daemons.push(d);
		const h = await open(d);
		const child = await open(d);
		const p1 = await open(d);
		const p2 = await open(d);
		const p3 = await open(d);

		acquire(h, "H", "parent");
		assert.deepEqual(await h.next(3000), granted("H"));
		acquire(child, "C", "child");
		acquire(p1, "P1", "parent");
		acquire(p2, "P2", "parent");
		acquire(p3, "P3", "parent");
		await assert.rejects(child.next(200), /no message from daemon within/);

		release(h, "H");
		// H's direct grant is streak 1, P1's queued grant is streak 2 → the child is boosted next.
		assert.deepEqual(await p1.next(3000), granted("P1"));
		release(p1, "P1");
		// streak reached agingAfterSlots: the child is boosted ahead of the remaining parents
		assert.deepEqual(await child.next(3000), granted("C"), "aged by parent-grant streak");
		release(child, "C");
		assert.deepEqual(await p2.next(3000), granted("P2"));
		release(p2, "P2");
		assert.deepEqual(await p3.next(3000), granted("P3"));
	});

	it("counts direct (free-slot) parent grants toward the aging streak", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1, agingAfterMs: 100000, agingAfterSlots: 2 }, queues: {} });
		daemons.push(d);
		const p1 = await open(d);
		const p2 = await open(d);
		const child = await open(d);
		const p3 = await open(d);

		acquire(p1, "P1", "parent");
		assert.deepEqual(await p1.next(3000), granted("P1")); // streak 1 (fast path)
		release(p1, "P1");
		acquire(p2, "P2", "parent");
		assert.deepEqual(await p2.next(3000), granted("P2")); // streak 2 (fast path)
		acquire(child, "C", "child");
		acquire(p3, "P3", "parent");
		await assert.rejects(child.next(200), /no message from daemon within/);

		release(p2, "P2");
		assert.deepEqual(await child.next(3000), granted("C"), "fast-path grants aged the child past P3");
	});

	it("re-tiers a queued waiter on the role op (promote a child ahead of a parent)", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		daemons.push(d);
		const h = await open(d);
		const child = await open(d);
		const parent = await open(d);

		acquire(h, "H", "parent");
		assert.deepEqual(await h.next(3000), granted("H"));
		acquire(child, "C", "child");
		acquire(parent, "P", "parent");
		await assert.rejects(parent.next(200), /no message from daemon within/);

		child.send({ op: "role", sessionKey: "C", role: "parent" }); // promote the queued child
		release(h, "H");
		assert.deepEqual(await child.next(3000), granted("C"), "promoted child now leads the parent tier (enqueued first)");
	});
});

describe("queue policy resolution", () => {
	after(() => {
		closeAllClients();
		stopAllDaemons();
	});

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

describe("per-endpoint queues, groups and hot-reload", () => {
	after(() => {
		closeAllClients();
		stopAllDaemons();
	});

	const GPU = "http://gpu.test/v1";
	const ZEN = "http://zen.test/v1";
	const modelsJson = { providers: { strata: { baseUrl: GPU }, llama: { baseUrl: GPU }, zen: { baseUrl: ZEN } } };
	const acquire = (c: Client, queueKey: string, sessionKey: string) => c.send({ op: "acquire", queueKey, sessionKey, role: "parent" });
	const release = (c: Client, queueKey: string, sessionKey: string) => c.send({ op: "release", queueKey, sessionKey });
	const granted = (queueKey: string, sessionKey: string) => ({ op: "granted", queueKey, sessionKey });

	it("serialises two providers on one baseUrl through one queue", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [GPU]: { maxStreams: 1 } } }, modelsJson);
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);

		acquire(a, GPU, "A");
		assert.deepEqual(await a.next(3000), granted(GPU, "A"));
		acquire(b, GPU, "B");
		await assert.rejects(b.next(200), /no message from daemon within/, "second provider on the endpoint waits");

		a.send({ op: "status" });
		const status = await a.next(3000);
		assert.equal(status.queues.length, 1, "strata and llama on one endpoint share one queue");
		assert.deepEqual(status.queues[0].holders, ["A"]);

		a.close();
		b.close();
		d.stop();
	});

	it("lets a baseUrl key win over a provider-name key naming the same endpoint", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { zen: { maxStreams: 1 }, [ZEN]: { maxStreams: 2 } } }, modelsJson);
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);

		acquire(a, ZEN, "A");
		acquire(b, ZEN, "B");
		assert.deepEqual(await a.next(3000), granted(ZEN, "A"));
		assert.deepEqual(await b.next(3000), granted(ZEN, "B"), "the endpoint entry's maxStreams 2 applies");

		a.close();
		b.close();
		d.stop();
	});

	it("merges endpoints into one queue with an explicit group", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { gpu: { match: [GPU, ZEN], maxStreams: 1 } } }, modelsJson);
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);

		acquire(a, GPU, "A");
		assert.deepEqual(await a.next(3000), granted(GPU, "A"), "the reply echoes the key the client asked with");
		a.send({ op: "status" });
		assert.deepEqual((await a.next(3000)).queues.map((q: any) => q.queueKey), ["gpu"], "status reports the group");

		acquire(b, ZEN, "B");
		await assert.rejects(b.next(200), /no message from daemon within/, "the merged endpoint shares the group's single slot");

		release(a, GPU, "A");
		assert.deepEqual(await b.next(3000), granted(ZEN, "B"));

		a.close();
		b.close();
		d.stop();
	});

	it("resolves provider names in a group's match list to their endpoints", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { merged: { match: ["strata", "zen"], maxStreams: 1 } } }, modelsJson);
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);

		acquire(a, GPU, "A");
		assert.deepEqual(await a.next(3000), granted(GPU, "A"));
		acquire(b, ZEN, "B");
		await assert.rejects(b.next(200), /no message from daemon within/);

		a.close();
		b.close();
		d.stop();
	});

	it("overrides baseUrl keying with a group", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [GPU]: { maxStreams: 3 }, gpu: { match: [GPU], maxStreams: 1 } } }, modelsJson);
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);

		acquire(a, GPU, "A");
		assert.deepEqual(await a.next(3000), granted(GPU, "A"));
		acquire(b, GPU, "B");
		await assert.rejects(b.next(200), /no message from daemon within/, "the group's maxStreams 1 wins over the endpoint's 3");

		a.close();
		b.close();
		d.stop();
	});

	it("leaves baseUrl keying alone for a disabled group", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { gpu: { match: [GPU, ZEN], enabled: false }, [GPU]: { maxStreams: 1 } } }, modelsJson);
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);

		acquire(a, GPU, "A");
		assert.deepEqual(await a.next(3000), granted(GPU, "A"));
		acquire(b, GPU, "B");
		await assert.rejects(b.next(200), /no message from daemon within/, "the endpoint's own maxStreams 1 still serialises it");

		a.close();
		b.close();
		d.stop();
	});

	it("passes every session through a queue whose entry is disabled", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [GPU]: { enabled: false } } }, modelsJson);
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);
		const c = await connectClient(d.socketPath);

		acquire(a, GPU, "A");
		acquire(b, GPU, "B");
		acquire(c, GPU, "C");
		assert.deepEqual(await a.next(3000), granted(GPU, "A"));
		assert.deepEqual(await b.next(3000), granted(GPU, "B"), "no queueing for a disabled endpoint");
		assert.deepEqual(await c.next(3000), granted(GPU, "C"));

		a.close();
		b.close();
		c.close();
		d.stop();
	});

	it("hot-reloads a config change without restarting the daemon", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [GPU]: { maxStreams: 1 } } }, modelsJson);
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);

		acquire(a, GPU, "A");
		assert.deepEqual(await a.next(3000), granted(GPU, "A"));
		acquire(b, GPU, "B");
		await assert.rejects(b.next(200), /no message from daemon within/);

		writeFileSync(d.configPath, JSON.stringify({ defaults: { maxStreams: 1 }, queues: { [GPU]: { maxStreams: 2 } } }));
		assert.deepEqual(await b.next(5000), granted(GPU, "B"), "the reloaded maxStreams frees a second slot");

		a.close();
		b.close();
		d.stop();
	});

	it("hot-reloads a new group without leaving the endpoint's old slot pool behind", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [GPU]: { maxStreams: 1 } } }, modelsJson);
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);

		acquire(a, GPU, "A");
		assert.deepEqual(await a.next(3000), granted(GPU, "A"));
		acquire(b, GPU, "B");
		await assert.rejects(b.next(200), /no message from daemon within/);

		writeFileSync(d.configPath, JSON.stringify({ defaults: { maxStreams: 1 }, queues: { gpu: { match: [GPU], maxStreams: 1 } } }));
		await sleep(600); // reload debounce + queue rebuild
		release(a, GPU, "A");
		assert.deepEqual(await b.next(5000), granted(GPU, "B"), "the migrated queue keeps holder and waiter in one slot pool");

		a.close();
		b.close();
		d.stop();
	});
});

describe("observability: status counters", () => {
	after(() => {
		closeAllClients();
		stopAllDaemons();
	});

	const acquire = (c: Client, sessionKey: string, role: string) => c.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey, role });

	it("counts a session switch when the idle quantum evicts a holder", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1, idleQuantumMs: 300 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);

		acquire(a, "A", "parent");
		assert.deepEqual(await a.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "A" });
		acquire(b, "B", "parent");
		assert.deepEqual(await b.next(5000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "B" }, "the idle quantum switched the slot to the waiter");

		a.send({ op: "status" });
		const status = await a.next(3000);
		assert.equal(status.queues[0].switches, 1, "a quantum eviction counts as one session switch");
		assert.deepEqual(status.queues[0].holders, ["B"]);

		a.close();
		b.close();
		d.stop();
	});

	it("counts a plain release handover as a session switch", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);

		acquire(a, "A", "parent");
		assert.deepEqual(await a.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "A" });
		acquire(b, "B", "parent");
		await assert.rejects(b.next(200), /no message from daemon within/);
		a.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "A" });
		assert.deepEqual(await a.next(3000), { op: "ok" }, "the release reply is consumed before status");
		assert.deepEqual(await b.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "B" });

		a.send({ op: "status" });
		assert.equal((await a.next(3000)).queues[0].switches, 1, "the settle-and-handoff switch is the one a normal run makes");

		a.close();
		b.close();
		d.stop();
	});

	it("reports queue depth per tier in status", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);

		acquire(a, "A", "parent");
		assert.deepEqual(await a.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "A" });
		acquire(b, "P", "parent");
		acquire(b, "C", "child");
		await assert.rejects(b.next(200), /no message from daemon within/);

		a.send({ op: "status" });
		const status = await a.next(3000);
		assert.deepEqual(
			status.queues[0].waiting.map((w: any) => [w.sessionKey, w.role]),
			[
				["P", "parent"],
				["C", "child"],
			],
			"status carries the tier of every waiter so /queue can report per-tier depth",
		);

		a.close();
		b.close();
		d.stop();
	});
});

// Idle-exit (#8): the daemon self-terminates after daemonIdleExitMs with zero clients, and a
// reconnect within the grace cancels the exit. Process-level: asserts the spawned daemon exits.
describe("daemon idle-exit", () => {
	after(() => {
		closeAllClients();
		stopAllDaemons();
	});

	const exited = (proc: import("node:child_process").ChildProcess, ms: number) =>
		new Promise<boolean>((resolve) => {
			proc.once("exit", () => resolve(true));
			setTimeout(() => resolve(false), ms);
		});

	it("self-terminates after daemonIdleExitMs once the last client disconnects", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1, daemonIdleExitMs: 300 }, queues: {} });
		const c = await connectClient(d.socketPath);
		c.close(); // last client leaves: the idle timer arms
		assert.ok(await exited(d.proc, 4000), "the daemon exits after daemonIdleExitMs with no clients");
	});

	it("survives a quick relaunch within the idle grace", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1, daemonIdleExitMs: 900 }, queues: {} });
		const c1 = await connectClient(d.socketPath);
		c1.close();
		await sleep(400); // within the grace window
		const c2 = await connectClient(d.socketPath); // relaunch cancels the pending exit
		c2.send({ op: "status" });
		assert.equal((await c2.next(3000)).op, "status", "the daemon survived the idle grace");
		c2.close();
	});
});

// Config schema validation (#9): a bad config surfaces clear errors (stderr / ui.notify)
// and never kills the queue — the daemon serves with defaults, a bad reload keeps the last good config.
describe("config schema validation at the daemon seam", () => {
	after(() => {
		closeAllClients();
		stopAllDaemons();
	});

	it("serves with defaults when the config is invalid at startup", async () => {
		const d = startDaemon({ defaults: { maxStreams: "many" }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);

		a.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "A", role: "parent" });
		assert.deepEqual(await a.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "A" });
		b.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "B", role: "parent" });
		await assert.rejects(b.next(200), /no message from daemon within/, "defaults (maxStreams 1) apply: B queues");

		a.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "A" });
		assert.deepEqual(await b.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "B" });
		a.close();
		b.close();
		d.stop();
	});

	it("keeps the last good config when a reload is invalid", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 2 } } });
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);

		a.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "A", role: "parent" });
		assert.deepEqual(await a.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "A" });
		b.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "B", role: "parent" });
		assert.deepEqual(await b.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "B" }, "two slots from the good config");

		writeFileSync(d.configPath, JSON.stringify({ queues: { [QUEUE_KEY]: { maxStreams: 0 } } }));
		await sleep(600); // reload debounce: the invalid file is rejected
		const c = await connectClient(d.socketPath);
		c.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "C", role: "parent" });
		await assert.rejects(c.next(300), /no message from daemon within/, "maxStreams stays 2, not the invalid 0");

		a.close();
		b.close();
		c.close();
		d.stop();
	});

	// The installed daemon runs under node_modules, where Node refuses type stripping: a .ts
	// import would crash it at spawn and fail-open silently (no serialisation at all).
	it("starts from a node_modules path, where Node refuses type stripping", async () => {
		const dir = mkdtempSync(join(tmpdir(), "llm-queue-nm-"));
		const srcDir = join(dir, "node_modules", "pi-llm-queue", "src");
		mkdirSync(srcDir, { recursive: true });
		copyFileSync(DAEMON, join(srcDir, "daemon.js"));
		copyFileSync(decodeURIComponent(new URL("../src/config.js", import.meta.url).pathname), join(srcDir, "config.js"));
		const configPath = join(dir, "config.json");
		writeFileSync(configPath, JSON.stringify({ defaults: { maxStreams: 1 }, queues: {} }));
		const socketPath = join(dir, "queue.sock");
		const proc = spawn(process.execPath, [join(srcDir, "daemon.js"), "--socket", socketPath, "--config", configPath], { stdio: "ignore" });
		proc.unref();

		const c = await connectClient(socketPath);
		c.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "N", role: "parent" });
		assert.deepEqual(await c.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "N" }, "the installed daemon serves");

		c.close();
		proc.kill();
	});
});
