// The daemon at the socket seam: protocol framing, lease lifecycle and no preemption (ADR 0002),
// tier ordering and aging, per-endpoint queues and groups, hot-reload, the status counters, the
// idle-exit and the connection lifetime. Real daemon, real socket; the Scheduler's own decisions
// are asserted in memory in test/scheduler.test.ts.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect } from "node:net";
import { after, before, describe, it } from "node:test";
import { closeAllClients, connectClient, exited, pollUntil, sleep, startDaemon, stopAllDaemons, waitForDaemon, DAEMON, QUEUE_KEY, type Client, type Daemon } from "./harness.ts";

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

	it("answers renew with ok", async () => {
		const c = await connectClient(d.socketPath);
		c.send({ op: "renew", queueKey: QUEUE_KEY, sessionKey: "A" });
		assert.deepEqual(await c.next(3000), { op: "ok", queueKey: QUEUE_KEY, sessionKey: "A", granted: false });
		c.close();
	});

	it("rejects a JSON line that is not an object and keeps serving (#81)", async () => {
		// A `null` line used to reach the op handler, throw on msg.op, and exit the daemon — dropping
		// every lease and waiter. The reply proves the parse was rejected; the status proves it lives.
		const c = await connectClient(d.socketPath);
		c.send(null);
		assert.deepEqual(await c.next(3000), { op: "error", reason: "message must be a JSON object" });

		c.send({ op: "status" });
		const status = await c.next(3000);
		assert.equal(status.op, "status", "the daemon survives the malformed line and answers the next op");
		c.close();
	});

});

// Lease lifecycle: TTL reclaim, heartbeat, and no preemption. The scheduling families moved to
// memory (test/scheduler.test.ts); this file keeps the socket smokes that pin the wire path (the
// daemon process, the sweep timer, the config schema rejection). Daemons/clients are registered for
// cleanup so a failing assert cannot leave them alive and hang the runner.
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

	// The quanta are gone from the schema, so a pre-#49 config carrying them is rejected outright
	// and the daemon serves defaults: preemption cannot be re-enabled from config. The scheduling half
	// of this assertion (many beats, no preemption) is a Scheduler property and runs in memory in
	// test/scheduler.test.ts; the driver takes plain resolved policy values, so the schema rejection
	// itself can only be shown on the wire.
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

	it("re-tiers a queued waiter over the socket (the role op)", async () => {
		// The role op's wire smoke (its in-memory counterpart is the promote test in scheduler.test.ts):
		// the op is accepted over the socket and re-tiers a live queued waiter in place, no reply.
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		daemons.push(d);
		const h = await open(d);
		const child = await open(d);
		const parent = await open(d);

		acquire(h, "H", "parent");
		assert.deepEqual(await h.next(3000), granted("H"));
		acquire(child, "C", "child");
		acquire(parent, "P", "parent");
		await assert.rejects(child.next(200), /no message from daemon within/, "as a child, C queues behind P");

		child.send({ op: "role", sessionKey: "C", role: "parent" }); // /queue promote: re-tier the queued waiter, no reply
		release(h, "H");
		assert.deepEqual(await child.next(3000), granted("C"), "promoted, C leads the parent tier (it enqueued first)");
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

	// The daemon's own catalog report (#77): the extension spawns it detached so stderr is discarded,
	// and the message matters when the daemon is run by hand. A present catalog with nothing to
	// resolve names the reason; a valid empty catalog is not called unreadable.
	it("names the reason a provider catalog yields no providers", async () => {
		const dir = mkdtempSync(join(tmpdir(), "llm-queue-catalog-"));
		const configPath = join(dir, "config.json");
		writeFileSync(configPath, JSON.stringify({ defaults: { maxStreams: 1 }, queues: {} }));
		const run = async (contents: string) => {
			const modelsJsonPath = join(dir, `models-${contents.length}.json`);
			writeFileSync(modelsJsonPath, contents);
			const proc = spawn(process.execPath, [DAEMON, "--socket", join(dir, `s-${contents.length}.sock`), "--config", configPath, "--models-json", modelsJsonPath], { stdio: ["ignore", "ignore", "pipe"] });
			let stderr = "";
			proc.stderr.on("data", (chunk) => (stderr += chunk));
			await sleep(120); // the report is written during startup, before the daemon listens: slack, not a condition
			proc.kill();
			return stderr;
		};

		assert.match(await run('{ "providers": '), /models\.json: .* — provider-name queue keys resolve to nothing/, "a half-written catalog names its parse error");
		assert.match(await run('{"providers":{}}'), /models\.json: no providers — provider-name/, "an empty catalog is reported as empty, not unreadable");
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

	it("addresses a raw endpoint key through the same group rule the extension uses", async () => {
		// One endpoint, two providers, one group per provider: the daemon's endpoint-level rule and
		// the extension's provider-level rule are one implementation (src/addressing.js addressEndpoint).
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

	it("echoes each queued request's own key when one session queues on two group members", async () => {
		// Finding 1 (P1): a session acquiring two member keys of one group overwrote a per-session
		// echo, so a queued grant echoed the other key and the client (lease-client matches on its own
		// queueKey) never resolved. The echo is per-request: each grant names the key that request used.
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { gpu: { match: [GPU, ZEN], maxStreams: 1 } } }, modelsJson);
		const holder = await connectClient(d.socketPath);
		const dual = await connectClient(d.socketPath);

		holder.send({ op: "acquire", queueKey: GPU, sessionKey: "H", role: "parent" });
		assert.deepEqual(await holder.next(3000), granted(GPU, "H"));

		dual.send({ op: "acquire", queueKey: GPU, sessionKey: "S", role: "parent" });
		dual.send({ op: "acquire", queueKey: ZEN, sessionKey: "S", role: "parent" });
		await assert.rejects(dual.next(200), /no message from daemon within/, "both member requests queue on the group's single slot");

		holder.send({ op: "release", queueKey: GPU, sessionKey: "H" });
		assert.deepEqual(await dual.next(3000), granted(GPU, "S"), "the first grant echoes the key that request asked with");
		dual.send({ op: "release", queueKey: GPU, sessionKey: "S" });
		assert.deepEqual(await dual.next(3000), granted(ZEN, "S"), "the second grant echoes its own key, not the first's");

		holder.close();
		dual.close();
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

	it("logs a group merge that leaves more live holders than the new maxStreams", async () => {
		// Two endpoint queues, one live holder each, merge into a group with one slot. No-preemption
		// means the daemon cannot end either holder's lease: it must report the transient
		// over-capacity (2/1) and write a config event instead of exceeding the new limit silently.
		const logFile = join(mkdtempSync(join(tmpdir(), "llm-queue-merge-")), "events.jsonl");
		const d = startDaemon({ defaults: { maxStreams: 1, logFile }, queues: { [GPU]: { maxStreams: 1 }, [ZEN]: { maxStreams: 1 } } }, modelsJson);
		const a = await connectClient(d.socketPath);
		const b = await connectClient(d.socketPath);
		const observer = await connectClient(d.socketPath);

		acquire(a, GPU, "A");
		assert.deepEqual(await a.next(3000), granted(GPU, "A"));
		acquire(b, ZEN, "B");
		assert.deepEqual(await b.next(3000), granted(ZEN, "B"));

		writeFileSync(d.configPath, JSON.stringify({ defaults: { maxStreams: 1, logFile }, queues: { gpu: { match: [GPU, ZEN], maxStreams: 1 } } }));
		let status: any;
		await pollUntil(
			async () => {
				observer.send({ op: "status" });
				status = await observer.next(3000);
				return status.queues.length === 1 && status.queues[0].queueKey === "gpu";
			},
			3000,
			"the reload merges both endpoint queues into the group",
		);

		assert.equal(status.queues[0].maxStreams, 1);
		assert.deepEqual(status.queues[0].holders.map((h: any) => h.sessionKey).sort(), ["A", "B"], "both holders stay: no-preemption cannot end a live lease");

		const events = readFileSync(logFile, "utf8")
			.split("\n")
			.filter((line) => line.trim())
			.map((line) => JSON.parse(line));
		const configEvents = events.filter((line) => line.event === "config");
		assert.equal(configEvents.length, 1, "the transient over-capacity is logged, not silent");
		assert.deepEqual(
			{ queueKey: configEvents[0].queueKey, reason: configEvents[0].reason, holders: configEvents[0].holders, maxStreams: configEvents[0].maxStreams },
			{ queueKey: "gpu", reason: "over-capacity", holders: 2, maxStreams: 1 },
		);

		const c = await connectClient(d.socketPath);
		acquire(c, GPU, "C");
		await assert.rejects(c.next(300), /no message from daemon within/, "no new holder is admitted while the queue is over capacity");
		release(a, GPU, "A");
		await assert.rejects(c.next(300), /no message from daemon within/, "one release brings the queue to its limit: C still waits");
		release(b, ZEN, "B");
		assert.deepEqual(await c.next(3000), granted(GPU, "C"), "C is granted once the over-capacity drains below the limit");

		a.close();
		b.close();
		c.close();
		observer.close();
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
		copyFileSync(decodeURIComponent(new URL("../src/addressing.js", import.meta.url).pathname), join(srcDir, "addressing.js"));
		copyFileSync(decodeURIComponent(new URL("../src/scheduler.js", import.meta.url).pathname), join(srcDir, "scheduler.js"));
		copyFileSync(decodeURIComponent(new URL("../src/socket-lines.js", import.meta.url).pathname), join(srcDir, "socket-lines.js"));
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
