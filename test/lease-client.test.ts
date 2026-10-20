import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { spawn, spawnSync } from "node:child_process";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, describe, it } from "node:test";
import { LeaseClient, ensureDaemon } from "../src/lease-client.ts";
import { heartbeatMs } from "../src/config.js";
import { DAEMON, QUEUE_KEY, pollUntil, sleep, startDaemon, stopAllDaemons, waitForDaemon } from "./harness.ts";

const raceWithSleep = (promise: Promise<unknown>, ms: number) =>
	Promise.race([promise.then(() => "resolved"), sleep(ms).then(() => "pending")]);

const QUEUE_KEY2 = "http://model.test/v2";

describe("lease client", () => {
	after(stopAllDaemons);

	it("fails open when the daemon is unreachable", async () => {
		const lease = new LeaseClient({ socketPath: "/tmp/pi-llm-queue-absent.sock" });
		const outcome = await raceWithSleep(lease.acquire(QUEUE_KEY, "S", "parent"), 500);
		assert.equal(outcome, "resolved");
	});

	it("serialises two sessions through the client api", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		await waitForDaemon(d.socketPath);
		const a = new LeaseClient({ socketPath: d.socketPath });
		const b = new LeaseClient({ socketPath: d.socketPath });

		await a.acquire(QUEUE_KEY, "A", "parent");
		const bAcquire = b.acquire(QUEUE_KEY, "B", "parent");
		assert.equal(await raceWithSleep(bAcquire, 300), "pending");

		a.release(QUEUE_KEY, "A");
		assert.equal(await raceWithSleep(bAcquire, 3000), "resolved");

		a.close();
		b.close();
		d.stop();
	});

	it("resolves a group member through the client api (replies echo the requested key)", async () => {
		const GPU = "http://gpu.test/v1";
		const ZEN = "http://zen.test/v1";
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { gpu: { match: [GPU, ZEN], maxStreams: 1 } } });
		await waitForDaemon(d.socketPath);
		const a = new LeaseClient({ socketPath: d.socketPath });
		const b = new LeaseClient({ socketPath: d.socketPath });

		assert.equal(await raceWithSleep(a.acquire(GPU, "A", "parent"), 3000), "resolved", "acquire on a grouped endpoint resolves: the reply echoes the requested key");
		assert.equal(await a.beat(GPU, "A"), true, "an activity beat on a grouped endpoint is granted");

		const bAcquire = b.acquire(ZEN, "B", "parent");
		assert.equal(await raceWithSleep(bAcquire, 300), "pending", "the group's single slot is shared across endpoints");
		a.release(GPU, "A");
		assert.equal(await raceWithSleep(bAcquire, 3000), "resolved");

		a.close();
		b.close();
		d.stop();
	});

	it("does not let a release reply satisfy the next acquire (turn 2+ serialisation)", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		await waitForDaemon(d.socketPath);
		const a = new LeaseClient({ socketPath: d.socketPath });
		const b = new LeaseClient({ socketPath: d.socketPath });

		await a.acquire(QUEUE_KEY, "A", "parent");
		a.release(QUEUE_KEY, "A"); // daemon replies {op:"ok"}: must be dropped, not buffered
		await b.acquire(QUEUE_KEY, "B", "parent"); // B now holds the slot

		// Audit repro: A re-acquires and must NOT resolve instantly on the stale "ok".
		const a2 = a.acquire(QUEUE_KEY, "A", "parent");
		assert.equal(await raceWithSleep(a2, 300), "pending");
		b.release(QUEUE_KEY, "B");
		assert.equal(await raceWithSleep(a2, 3000), "resolved");

		a.close();
		b.close();
		d.stop();
	});

	it("delivers concurrent acquires from one client without hijacking a waiter", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		await waitForDaemon(d.socketPath);
		const c = new LeaseClient({ socketPath: d.socketPath });
		const holder = new LeaseClient({ socketPath: d.socketPath });
		await holder.acquire(QUEUE_KEY, "H", "parent"); // occupy the slot

		// Two concurrent acquires from the same client, both queued behind H.
		const first = c.acquire(QUEUE_KEY, "S1", "parent");
		const second = c.acquire(QUEUE_KEY, "S2", "parent");
		assert.equal(await raceWithSleep(first, 300), "pending");
		assert.equal(await raceWithSleep(second, 300), "pending");

		holder.release(QUEUE_KEY, "H");
		assert.equal(await raceWithSleep(first, 3000), "resolved");
		assert.equal(await raceWithSleep(second, 300), "pending"); // maxStreams 1: second still queued
		c.release(QUEUE_KEY, "S1");
		assert.equal(await raceWithSleep(second, 3000), "resolved");

		c.close();
		holder.close();
		d.stop();
	});

	it("delivers two same-session acquires (queued run + compaction) without wedging", async () => {
		// Audit repro: a queued agent_start plus an out-of-run request share one sessionKey.
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		await waitForDaemon(d.socketPath);
		const c = new LeaseClient({ socketPath: d.socketPath });
		const holder = new LeaseClient({ socketPath: d.socketPath });
		await holder.acquire(QUEUE_KEY, "H", "parent");

		const run = c.acquire(QUEUE_KEY, "S", "parent");
		const compaction = c.acquire(QUEUE_KEY, "S", "parent");
		assert.equal(await raceWithSleep(run, 300), "pending");
		assert.equal(await raceWithSleep(compaction, 300), "pending");

		holder.release(QUEUE_KEY, "H");
		assert.equal(await raceWithSleep(run, 3000), "resolved");
		c.release(QUEUE_KEY, "S");
		assert.equal(await raceWithSleep(compaction, 3000), "resolved");

		c.close();
		holder.close();
		d.stop();
	});

	it("releases a queued acquire to its request when the daemon dies mid-wait", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		await waitForDaemon(d.socketPath);
		const holder = new LeaseClient({ socketPath: d.socketPath });
		await holder.acquire(QUEUE_KEY, "H", "parent");
		const c = new LeaseClient({ socketPath: d.socketPath });
		const queued = c.acquire(QUEUE_KEY, "C", "parent");
		assert.equal(await raceWithSleep(queued, 300), "pending");
		d.stop(); // daemon dies while C is queued
		assert.equal(await raceWithSleep(queued, 3000), "resolved");
		c.close();
		holder.close();
	});

	it("heartbeats keep a held lease alive past its TTL", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1, leaseTtlMs: 1000 }, queues: {} }, undefined, undefined, 50);
		await waitForDaemon(d.socketPath);
		const a = new LeaseClient({ socketPath: d.socketPath, heartbeatMs: 100 });
		const b = new LeaseClient({ socketPath: d.socketPath });

		await a.acquire(QUEUE_KEY, "A", "parent");
		const bAcquire = b.acquire(QUEUE_KEY, "B", "parent");
		assert.equal(await raceWithSleep(bAcquire, 300), "pending");

		await sleep(1200); // ~1.2 TTLs with 24 sweep ticks: A's heartbeat must keep its slot
		assert.equal(await raceWithSleep(bAcquire, 300), "pending", "heartbeats must keep A's lease");

		a.release(QUEUE_KEY, "A");
		assert.equal(await raceWithSleep(bAcquire, 3000), "resolved");

		a.close();
		b.close();
		d.stop();
	});

	it("the default heartbeat ratio keeps a lease alive at the leaseTtlMs floor", async () => {
		// The client heartbeats at config.js heartbeatMs(ttl). At the 1000 ms floor a 1 s floor would
		// equal the TTL, so a sweep landing between two beats would evict a healthy holder.
		const TTL = 1000;
		const d = startDaemon({ defaults: { maxStreams: 1, leaseTtlMs: TTL }, queues: {} }, undefined, undefined, 50);
		await waitForDaemon(d.socketPath);
		const a = new LeaseClient({ socketPath: d.socketPath, heartbeatMs: heartbeatMs(TTL) });
		const b = new LeaseClient({ socketPath: d.socketPath });

		await a.acquire(QUEUE_KEY, "A", "parent");
		const bAcquire = b.acquire(QUEUE_KEY, "B", "parent");
		assert.equal(await raceWithSleep(bAcquire, 300), "pending");

		await sleep(1400); // ~4 TTLs at the floor, with the sweep ticking every 50 ms
		assert.equal(await raceWithSleep(bAcquire, 300), "pending", "the heartbeat stays under the TTL: the holder keeps its slot");

		a.release(QUEUE_KEY, "A");
		assert.equal(await raceWithSleep(bAcquire, 3000), "resolved");

		a.close();
		b.close();
		d.stop();
	});

	it("reassembles a reply split across socket chunks", async () => {
		// Socket reads are not line-aligned: a grant arriving in two writes must still resolve
		// the acquire, not die on a JSON fragment.
		const dir = mkdtempSync(join(tmpdir(), "llm-queue-frag-"));
		const socketPath = join(dir, "frag.sock");
		const server = createServer((conn) => {
			conn.once("data", () => {
				const reply = `${JSON.stringify({ op: "granted", queueKey: QUEUE_KEY, sessionKey: "FRAG" })}\n`;
				conn.write(reply.slice(0, 12));
				setTimeout(() => conn.write(reply.slice(12)), 50);
			});
		});
		await new Promise<void>((resolve) => server.listen(socketPath, () => resolve()));

		const c = new LeaseClient({ socketPath });
		try {
			assert.equal(await raceWithSleep(c.acquire(QUEUE_KEY, "FRAG", "parent"), 2000), "resolved");
		} finally {
			c.close();
			server.close();
		}
	});

	it("cancels a queued acquire when the abort signal fires", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		await waitForDaemon(d.socketPath);
		const holder = new LeaseClient({ socketPath: d.socketPath });
		await holder.acquire(QUEUE_KEY, "H", "parent");
		const c = new LeaseClient({ socketPath: d.socketPath });
		const ac = new AbortController();
		const queued = c.acquire(QUEUE_KEY, "C", "parent", undefined, ac.signal);
		assert.equal(await raceWithSleep(queued, 300), "pending");
		ac.abort();
		assert.equal(await raceWithSleep(queued, 3000), "resolved");
		// The cancel dropped C: releasing H must not grant a phantom C, so a fresh acquirer gets the slot.
		holder.release(QUEUE_KEY, "H");
		const next = new LeaseClient({ socketPath: d.socketPath });
		assert.equal(await raceWithSleep(next.acquire(QUEUE_KEY, "N", "parent"), 3000), "resolved");
		c.close();
		next.close();
		holder.close();
		d.stop();
	});

	it("reports queue position and bearers while an acquire waits, and stops once granted", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		await waitForDaemon(d.socketPath);
		const a = new LeaseClient({ socketPath: d.socketPath });
		const b = new LeaseClient({ socketPath: d.socketPath });
		await a.acquire(QUEUE_KEY, "A", "parent");

		const seen: any[] = [];
		const bAcquire = b.acquire(QUEUE_KEY, "B", "parent", undefined, undefined, (info) => seen.push(info));
		// The first status report fires at 300 ms (lease-client.ts): poll for it with the 1500 ms bound
		// the sleep used. The 2 s poll cadence is the next test's subject — a queued process must live
		// through a 300 ms first poll and a 2 s follow-up; the window below proves reporting stops.
		await pollUntil(() => seen.length > 0, 1500, "waiting for the first status report of the waiting acquire");
		assert.ok(seen.length, "a waiting acquire reports its queue position");
		assert.equal(seen[0].position, 1);
		assert.deepEqual(seen[0].holders, ["A"], "the report names the session holding the slot");

		a.release(QUEUE_KEY, "A");
		assert.equal(await raceWithSleep(bAcquire, 3000), "resolved");
		const reports = seen.length;
		await sleep(2500);
		assert.equal(seen.length, reports, "reporting stops once the slot is granted");

		a.close();
		b.close();
		d.stop();
	});

	it("keeps a headless process alive across status polls while its acquire waits (socket stays ref'd)", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		await waitForDaemon(d.socketPath);
		const holder = new LeaseClient({ socketPath: d.socketPath });
		await holder.acquire(QUEUE_KEY, "H", "parent"); // occupy the single slot

		// The waiting client runs in its own process: only the lease socket keeps it alive, so a
		// status poll that drops the socket ref makes it exit (the --print headless regression).
		const leasePath = decodeURIComponent(new URL("../src/lease-client.ts", import.meta.url).pathname);
		const script = join(mkdtempSync(join(tmpdir(), "llm-queue-child-")), "child.mts");
		writeFileSync(
			script,
			[
				`import { LeaseClient } from ${JSON.stringify(leasePath)};`,
				`const lease = new LeaseClient({ socketPath: process.argv[2] });`,
				`const reason = await lease.acquire(${JSON.stringify(QUEUE_KEY)}, "CHILD", "parent", undefined, undefined, () => {});`,
				"console.log(`GRANTED ${reason}`);",
				"lease.close();",
			].join("\n"),
		);
		const child = spawn(process.execPath, [script, d.socketPath], { stdio: ["ignore", "pipe", "pipe"] });
		let out = "";
		child.stdout.on("data", (c) => (out += c));
		child.stderr.on("data", (c) => (out += c));
		let exited: number | null = null;
		child.on("exit", (code) => (exited = code));

		// Long enough for the 300 ms first poll and one 2 s follow-up to drop-and-restore the ref.
		await sleep(2500);
		assert.equal(exited, null, `the queued process exited while waiting (out=${JSON.stringify(out)})`);

		holder.release(QUEUE_KEY, "H");
		const code = await new Promise<number | null>((resolve) => child.on("exit", resolve));
		assert.equal(code, 0);
		assert.match(out, /GRANTED granted/, "the child was granted its slot once it freed");

		holder.close();
		d.stop();
	});

	it("fails closed (hard error) when failOpen is false and the daemon is unreachable", async () => {
		const lease = new LeaseClient({ socketPath: "/tmp/pi-llm-queue-absent.sock", failOpen: false });
		await assert.rejects(lease.acquire(QUEUE_KEY, "S", "parent"), /failOpen is false/);
	});

	it("fails closed when the daemon dies mid-wait and failOpen is false", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		await waitForDaemon(d.socketPath);
		const holder = new LeaseClient({ socketPath: d.socketPath });
		await holder.acquire(QUEUE_KEY, "H", "parent"); // occupy the slot
		const c = new LeaseClient({ socketPath: d.socketPath, failOpen: false });
		const queued = c.acquire(QUEUE_KEY, "C", "parent");
		assert.equal(await raceWithSleep(queued, 300), "pending", "C is queued behind H");
		d.stop(); // daemon dies mid-wait: failOpen:false must hard-error, not pass through
		await assert.rejects(queued, /failOpen is false/);
		holder.close();
	});

	it("warns a parent past queueTimeoutMs but keeps it queued; a child waits unlimited", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		await waitForDaemon(d.socketPath);
		const holder = new LeaseClient({ socketPath: d.socketPath });
		await holder.acquire(QUEUE_KEY, "H", "parent"); // occupy the single slot

		const parent = new LeaseClient({ socketPath: d.socketPath, queueTimeoutMs: 300 });
		const child = new LeaseClient({ socketPath: d.socketPath, queueTimeoutMs: 300 });
		let warned = false;
		const p = parent.acquire(QUEUE_KEY, "P", "parent", undefined, undefined, undefined, () => { warned = true; });
		const c = child.acquire(QUEUE_KEY, "C", "child");

		await sleep(400); // past queueTimeoutMs
		assert.ok(warned, "the parent is warned past queueTimeoutMs");
		assert.equal(await raceWithSleep(p, 200), "pending", "the parent stays queued past queueTimeoutMs (warn-only)");
		assert.equal(await raceWithSleep(c, 200), "pending", "child default is unlimited: queueTimeoutMs never applies to a child");

		holder.release(QUEUE_KEY, "H");
		assert.equal(await raceWithSleep(p, 3000), "resolved", "the parent is granted once the slot frees");
		parent.release(QUEUE_KEY, "P");
		assert.equal(await raceWithSleep(c, 3000), "resolved", "the child is granted once the parent releases");

		parent.close();
		child.close();
		holder.close();
		d.stop();
	});

	it("keeps a queued acquire alive when a concurrent beat on the same socket finishes", async () => {
		// ref/unref on a Node socket is one switch, not a counter: any second op that unrefs frees
		// the process to exit while the queued grant is still outstanding (audit finding 5).
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		await waitForDaemon(d.socketPath);
		const holder = new LeaseClient({ socketPath: d.socketPath });
		await holder.acquire(QUEUE_KEY, "H", "parent"); // occupy the single slot

		const leasePath = decodeURIComponent(new URL("../src/lease-client.ts", import.meta.url).pathname);
		const script = join(mkdtempSync(join(tmpdir(), "llm-queue-child-ref-")), "child.mts");
		writeFileSync(
			script,
			[
				`import { LeaseClient } from ${JSON.stringify(leasePath)};`,
				`const lease = new LeaseClient({ socketPath: process.argv[2] });`,
				`const OTHER = ${JSON.stringify(QUEUE_KEY2)};`,
				`await lease.acquire(OTHER, "CHILD", "parent");`,
				`const queued = lease.acquire(${JSON.stringify(QUEUE_KEY)}, "CHILD", "parent");`,
				`await lease.beat(OTHER, "CHILD"); // a finished op: must not free the socket under the queued acquire`,
				"console.log(`GRANTED ${await queued}`);",
				"lease.close();",
			].join("\n"),
		);
		const child = spawn(process.execPath, [script, d.socketPath], { stdio: ["ignore", "pipe", "pipe"] });
		let out = "";
		child.stdout.on("data", (c) => (out += c));
		child.stderr.on("data", (c) => (out += c));
		let exited: number | null = null;
		child.on("exit", (code) => (exited = code));

		await sleep(2500);
		assert.equal(exited, null, `the queued process exited after the concurrent beat (out=${JSON.stringify(out)})`);

		holder.release(QUEUE_KEY, "H");
		const code = await new Promise<number | null>((resolve) => child.on("exit", resolve));
		assert.equal(code, 0);
		assert.match(out, /GRANTED granted/, "the child was granted its slot once it freed");

		holder.close();
		d.stop();
	});
});

describe("daemon autostart", () => {
	const dir = mkdtempSync(join(tmpdir(), "llm-queue-autostart-"));
	const socketPath = join(dir, "queue.sock");
	const configPath = join(dir, "config.json");
	writeFileSync(configPath, JSON.stringify({ defaults: { maxStreams: 1 }, queues: {} }));

	after(() => spawnSync("pkill", ["-f", socketPath]));

	it("starts the daemon detached on demand", async () => {
		const lease = new LeaseClient({ socketPath, daemonPath: DAEMON, configPath });
		assert.equal((await lease.status())?.op, "status");
		lease.close();
	});

	it("reclaims a socket left by a dead daemon", async () => {
		// No lockfile: the daemon's own start() resolves a live socket (the second daemon exits 0)
		// and reclaims a stale socket file, so the client needs no liveness probe of its own.
		spawnSync("pkill", ["-f", socketPath]);
		await sleep(300);
		const lease = new LeaseClient({ socketPath, daemonPath: DAEMON, configPath });
		assert.equal((await lease.status())?.op, "status");
		lease.close();
	});
});
