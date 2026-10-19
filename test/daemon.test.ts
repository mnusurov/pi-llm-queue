import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect } from "node:net";
import { after, before, describe, it } from "node:test";
import { closeAllClients, connectClient, exited, sleep, startDaemon, stopAllDaemons, waitForDaemon, DAEMON, QUEUE_KEY, type Client, type Daemon } from "./harness.ts";

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

// Lease lifecycle: TTL reclaim, heartbeat, and no preemption. Asserted through
// the socket seam only. Daemons/clients are registered for cleanup so a failing assert
// cannot leave them alive and hang the runner.
describe("lease lifecycle and no preemption", () => {
	const daemons: Daemon[] = [];
	const clients: Client[] = [];

	// sweepMs 50: the sweep is the clock these tests wait on, so a short tick makes a TTL-bound
	// test cost milliseconds instead of seconds.
	const start = (leaseTtlMs = 60000, sweepMs = 50) => {
		const d = startDaemon({ defaults: { maxStreams: 1, leaseTtlMs }, queues: {} }, undefined, undefined, sweepMs);
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
		const d = start(1000);
		const a = await open(d); // live socket, abandoned lease: never renews
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B");
		await assert.rejects(b.next(300), /no message from daemon within/);

		assert.deepEqual(await b.next(1500), granted("B"));
	});

	// No preemption (#49): a live holder is never switched away. The slot moves only on
	// voluntary release, abort, disconnect, or the TTL lapse of a dead client.
	it("no preemption: a silent holder keeps the slot and the waiter is granted only on release", async () => {
		const d = start();
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B");
		await assert.rejects(b.next(200), /no message from daemon within/);

		await sleep(300); // 6 sweep ticks of total silence: the old idle quantum evicted here
		await assert.rejects(b.next(200), /no message from daemon within/, "a silent live holder keeps the slot");

		a.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "A" });
		assert.deepEqual(await b.next(1500), granted("B"), "the waiter is granted on voluntary release");
	});

	it("no preemption: many requests over a long run never switch the slot away", async () => {
		const d = start();
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B");

		for (let i = 0; i < 12; i++) {
			beat(a, "A"); // 12 requests: the old monopoly request-count quantum evicted here
			assert.deepEqual(await a.next(3000), { op: "ok", queueKey: QUEUE_KEY, sessionKey: "A", granted: true });
		}
		await sleep(150); // 3 sweep ticks after 12 requests: the old request-count quantum evicted here
		await assert.rejects(b.next(200), /no message from daemon within/, "a busy live holder keeps the slot");

		a.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "A" });
		assert.deepEqual(await b.next(1500), granted("B"));
	});

	// The quanta are gone from the schema, so a pre-#49 config carrying them is rejected outright
	// and the daemon serves defaults: preemption cannot be re-enabled from config.
	it("a config carrying the removed quanta falls back to defaults and never preempts", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1, leaseTtlMs: 60000, idleQuantumMs: 0, monopolyRequests: 1, monopolyMs: 0 }, queues: {} }, undefined, undefined, 50);
		daemons.push(d);
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B");
		for (let i = 0; i < 12; i++) {
			beat(a, "A");
			await a.next(3000);
		}
		await sleep(300); // idle 0 ms + 12 requests + 0 ms monopoly: the old quanta evicted A twice over
		await assert.rejects(b.next(200), /no message from daemon within/, "the rejected config must not restore preemption");

		a.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "A" });
		assert.deepEqual(await b.next(1500), granted("B"));
	});

});

describe("tier ordering (parent > child) and aging", () => {
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

	it("rejects a keyless acquire instead of creating an unnamed queue that holds a slot", async () => {
		const d = startDaemon();
		const c = await connectClient(d.socketPath);
		for (const op of ["acquire", "release", "cancel"]) c.send({ op, sessionKey: "X", role: "parent" });
		for (const op of ["acquire", "release", "cancel"]) {
			const reply = await c.next(3000);
			assert.equal(reply.op, "error");
			assert.match(reply.reason, /queueKey/);
			assert.ok(reply.reason.startsWith(op), `the error names the op (${JSON.stringify(reply)})`);
		}
		c.send({ op: "status" });
		assert.deepEqual((await c.next(3000)).queues, [], "no unnamed queue appeared for the keyless request");
		c.close();
		d.stop();
	});

	it("starts and serves with a provider catalog that does not exist", async () => {
		// models.json is optional for pi, and the extension always passes --models-json.
		const missing = join(mkdtempSync(join(tmpdir(), "llm-queue-nomodels-")), "models.json");
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: {} }, undefined, missing);
		let exitCode: number | null = null;
		d.proc.on("exit", (code) => (exitCode = code));

		const c = await connectClient(d.socketPath);
		c.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "A", role: "parent" });
		assert.equal((await c.next(3000)).op, "granted", "the daemon serves requests normally without a catalog");
		await sleep(400);
		assert.equal(exitCode, null, "a missing models.json must not kill the daemon");
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
		assert.deepEqual(status.queues[0].holders, [{ sessionKey: "A", role: "parent" }], "a Holder with no group label reports its tier");

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

	it("resolves a raw endpoint key through the same group rule the extension uses", async () => {
		// One endpoint, two providers, one group per provider: the daemon's endpoint-level rule and
		// the extension's provider-level rule are one implementation (src/config.js groupKeyFor).
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { g1: { match: ["strata"], maxStreams: 1 }, g2: { match: ["llama"], maxStreams: 1 } } }, modelsJson);
		const a = await connectClient(d.socketPath);

		acquire(a, GPU, "A");
		assert.deepEqual(await a.next(3000), granted(GPU, "A"));
		a.send({ op: "status" });
		const status = await a.next(3000);
		assert.deepEqual(status.queues.map((q: any) => q.queueKey), ["g1"], "the endpoint's first matching group owns the raw client's slot");

		a.close();
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

	it("hot-reloads a new group without leaving the endpoint's old slot set behind", async () => {
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
		assert.deepEqual(await b.next(5000), granted(GPU, "B"), "the migrated queue keeps holder and waiter in one slot set");

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

	it("reports tier and group label for every Holder and Waiter", async () => {
		// A raw socket client builds a dashboard from the reply alone: a Holder is no longer a bare
		// session id, and its reported tier is the one it queued as (direct grant and grantNext).
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);

		a.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "P", role: "parent", group: "interactive" });
		assert.deepEqual(await a.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "P" });
		b.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "C", role: "child", group: "scout" });
		await assert.rejects(b.next(200), /no message from daemon within/, "the child waits behind the parent");

		a.send({ op: "status" });
		const queueOf = (s: any) => s.queues.find((q: any) => q.queueKey === QUEUE_KEY);
		const status = queueOf(await a.next(3000));
		assert.deepEqual(
			[status.holders, status.waiting],
			[
				[{ sessionKey: "P", role: "parent", group: "interactive" }],
				[{ sessionKey: "C", role: "child", group: "scout" }],
			],
			"a Holder carries role and group label exactly like a Waiter",
		);

		a.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "P" });
		assert.deepEqual(await a.next(3000), { op: "ok" });
		assert.deepEqual(await b.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "C" }, "the freed slot goes to the child");

		a.send({ op: "status" });
		assert.deepEqual(
			queueOf(await a.next(3000)).holders,
			[{ sessionKey: "C", role: "child", group: "scout" }],
			"a Holder granted from the waiting list keeps the tier and label it queued as",
		);

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

	it("self-terminates after daemonIdleExitMs once the last client disconnects", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1, daemonIdleExitMs: 300 }, queues: {} });
		const c = await connectClient(d.socketPath);
		c.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "A", role: "parent" }); // a counted client (#60)
		assert.deepEqual(await c.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "A" });
		c.close(); // last client leaves: the idle timer arms
		assert.ok(await exited(d.proc, 4000), "the daemon exits after daemonIdleExitMs with no clients");
	});

	it("survives a quick relaunch within the idle grace", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1, daemonIdleExitMs: 900 }, queues: {} });
		const c1 = await connectClient(d.socketPath);
		c1.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "A", role: "parent" });
		assert.deepEqual(await c1.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "A" });
		c1.close();
		await sleep(400); // within the grace window
		const c2 = await connectClient(d.socketPath);
		c2.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "B", role: "parent" }); // a counted relaunch cancels the pending exit
		assert.deepEqual(await c2.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "B" });
		await sleep(600); // past the 900 ms grace the first close armed
		c2.send({ op: "status" });
		assert.equal((await c2.next(3000)).op, "status", "the daemon survived the idle grace");
		c2.close();
	});
});

// Daemon lifetime (#60): a connection becomes a counted client at its first state-touching op
// (acquire, renew, role, cancel). A `status` poll — the queue observer, a dashboard — changes no
// queue state, so it must not keep the daemon alive. Default daemonIdleExitMs 0: the daemon exits
// the moment the last counted client closes, so every pi restart picks up new daemon code.
describe("daemon lifetime: counted clients and status invisibility (#60)", () => {
	after(() => {
		closeAllClients();
		stopAllDaemons();
	});

	/** A counted client: its acquire is a state-touching op, so the daemon counts it as a client. */
	const counted = async (socketPath: string, sessionKey: string) => {
		const c = await connectClient(socketPath);
		c.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey, role: "parent" });
		assert.deepEqual(await c.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey });
		return c;
	};

	it("exits with the last counted client while a status-only observer stays open", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1, daemonIdleExitMs: 0 }, queues: {} });
		const observer = await connectClient(d.socketPath);
		observer.send({ op: "status" });
		assert.equal((await observer.next(3000)).op, "status", "the observer polls status every 2 s, as the queue observer does");

		const lease = await counted(d.socketPath, "A");
		lease.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "A" });
		assert.deepEqual(await lease.next(3000), { op: "ok" });
		lease.close(); // the last counted client is gone

		assert.ok(await exited(d.proc, 2000), "a status-only connection must not keep the daemon alive");
	});

	it("stays alive while a counted client stays open, and exits when it closes", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1, daemonIdleExitMs: 0 }, queues: {} });
		const holder = await counted(d.socketPath, "H");
		const waiter = await connectClient(d.socketPath);
		waiter.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "W", role: "parent" });
		await assert.rejects(waiter.next(200), /no message from daemon within/, "W waits behind the holder");
		waiter.send({ op: "cancel", queueKey: QUEUE_KEY, sessionKey: "W" });
		waiter.close(); // a counted client leaves, but not the last one

		assert.ok(!(await exited(d.proc, 1000)), "the holder's open counted connection keeps the daemon alive");
		holder.close();
		assert.ok(await exited(d.proc, 2000), "the daemon exits with the last counted client");
	});

	it("counts a bare renew heartbeat, the extension's keeper of a live pi process", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1, daemonIdleExitMs: 0 }, queues: {} });
		const c = await connectClient(d.socketPath);
		c.send({ op: "renew" }); // the extension's connection-wide heartbeat: no lease, no queue state touched
		assert.ok(!(await exited(d.proc, 1000)), "a bare renew counts the connection, so a live pi keeps the daemon");
		c.close();
		assert.ok(await exited(d.proc, 2000), "and the daemon exits when that pi is gone");
	});

	it("bye from the last counted client exits the daemon while its socket stays open", async () => {
		// pi's session_shutdown says "I am leaving" before the process dies: the daemon must arm
		// the idle-exit at the word, not wait for the OS to close the socket — a lingering pi
		// process must not keep stale code resident past the last session's shutdown.
		const d = startDaemon({ defaults: { maxStreams: 1, daemonIdleExitMs: 0 }, queues: {} });
		const c = await counted(d.socketPath, "A");
		c.send({ op: "bye" }); // the socket stays open: the departure is announced, not enforced
		assert.ok(await exited(d.proc, 2000), "bye arms the idle-exit with the connection still open");
	});

	it("a resident value past setTimeout's 2^31-1 cap does not exit at once", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1, daemonIdleExitMs: 31536000000 }, queues: {} });
		const c = await counted(d.socketPath, "A");
		c.close();
		assert.ok(!(await exited(d.proc, 1000)), "Node coerces an unclamped delay past 2^31-1 to 1 ms: \"set it high\" must mean resident, not instant exit");
	});

	it("the documented default (no daemonIdleExitMs) exits at once and frees the socket", async () => {
		// Spawned directly, not through the harness: the harness opts the suite out of the default.
		const dir = mkdtempSync(join(tmpdir(), "llm-queue-60-"));
		const socketPath = join(dir, "queue.sock");
		const configPath = join(dir, "config.json");
		writeFileSync(configPath, JSON.stringify({ defaults: { maxStreams: 1 }, queues: {} }));
		const proc = spawn(process.execPath, [DAEMON, "--socket", socketPath, "--config", configPath], { stdio: "ignore" });
		proc.unref();
		const c = await connectClient(socketPath);
		c.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "A", role: "parent" });
		assert.deepEqual(await c.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "A" });
		c.close();

		assert.ok(await exited(proc, 2000), "the default is exit at once, not a 60 s grace");
		assert.equal(existsSync(socketPath), false, "the exit unlinks the socket, so the next op autostarts a fresh daemon");
		proc.kill();
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
		copyFileSync(decodeURIComponent(new URL("../src/scheduler.js", import.meta.url).pathname), join(srcDir, "scheduler.js"));
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

// Socket framing (finding 13): a unix-socket peer's writes are not line- or character-aligned,
// and a peer that never sends a newline must not grow the daemon without bound.
describe("connection framing and buffer bounds", () => {
	after(() => {
		closeAllClients();
		stopAllDaemons();
	});

	it("parses a message split inside a multi-byte character", async () => {
		const d = startDaemon();
		await waitForDaemon(d.socketPath);
		const payload = Buffer.from(`${JSON.stringify({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "café✅" })}\n`);
		const split = payload.indexOf(0xc3) + 1; // between the two bytes of "é"
		const raw = await connect(d.socketPath);
		const decoder = new TextDecoder();
		const reply = new Promise<any>((resolve, reject) => {
			let buffer = "";
			raw.on("data", (chunk) => {
				buffer += decoder.decode(chunk as BufferSource, { stream: true });
				const line = buffer.split("\n")[0];
				if (line) resolve(JSON.parse(line));
			});
			setTimeout(() => reject(new Error("no reply from daemon")), 3000);
		});

		raw.write(payload.subarray(0, split));
		await sleep(100);
		raw.write(payload.subarray(split));
		assert.equal((await reply).sessionKey, "café✅", "the key survives the split byte-for-byte");

		raw.destroy();
		d.stop();
	});

	it("drops a connection that never sends a newline and keeps serving", async () => {
		const d = startDaemon();
		await waitForDaemon(d.socketPath);
		const flood = await connect(d.socketPath);
		flood.on("error", () => {}); // the daemon destroys the connection mid-write: EPIPE is the expected outcome
		flood.write("x".repeat(200_000)); // no newline: the daemon must cap the line buffer
		await new Promise<void>((resolve) => flood.once("close", resolve));

		const c = await connectClient(d.socketPath);
		c.send({ op: "status" });
		assert.equal((await c.next(3000)).op, "status", "the daemon stays healthy after an oversized peer");
		c.close();
		d.stop();
	});
});

// Idle-yield (#51): an activity-flagged holder silent past idleYieldMs with waiters present
// yields the slot. A lone holder is never disturbed; unflagged (old-client) leases are never yielded.
describe("idle-yield under contention", () => {
	const daemons: Daemon[] = [];
	const clients: Client[] = [];
	const start = (idleYieldMs: number, sweepMs = 50) => {
		const d = startDaemon({ defaults: { maxStreams: 1, idleYieldMs }, queues: {} }, undefined, undefined, sweepMs);
		daemons.push(d);
		return d;
	};
	const open = async (d: Daemon) => {
		const c = await connectClient(d.socketPath);
		clients.push(c);
		return c;
	};
	const acquire = (c: Client, sessionKey: string, activity = true) =>
		c.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey, role: "parent", activity });
	const beat = (c: Client, sessionKey: string) => c.send({ op: "renew", queueKey: QUEUE_KEY, sessionKey });
	const granted = (sessionKey: string) => ({ op: "granted", queueKey: QUEUE_KEY, sessionKey });

	after(() => {
		for (const c of clients) c.close();
		for (const d of daemons) d.stop();
		closeAllClients();
		stopAllDaemons();
	});

	it("yields an idle activity-flagged holder when a waiter is queued", async () => {
		const d = start(300);
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A", true);
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B", true);
		await assert.rejects(b.next(200), /no message from daemon within/);

		// A is silent past idleYieldMs: the sweep yields it, B is granted.
		assert.deepEqual(await b.next(2000), granted("B"), "idle holder yielded, waiter granted");
		// A's next beat answers granted:false (the slot is gone).
		beat(a, "A");
		assert.deepEqual(await a.next(3000), { op: "ok", queueKey: QUEUE_KEY, sessionKey: "A", granted: false }, "yielded holder learns via beat");
	});
});
