import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, describe, it } from "node:test";
import { LeaseClient, ensureDaemon } from "../src/lease-client.ts";
import { DAEMON, QUEUE_KEY, sleep, startDaemon, stopAllDaemons, waitForDaemon } from "./harness.ts";

const raceWithSleep = (promise: Promise<unknown>, ms: number) =>
	Promise.race([promise.then(() => "resolved"), sleep(ms).then(() => "pending")]);

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
		const d = startDaemon({ defaults: { maxStreams: 1, leaseTtlMs: 400 }, queues: {} });
		await waitForDaemon(d.socketPath);
		const a = new LeaseClient({ socketPath: d.socketPath, heartbeatMs: 100 });
		const b = new LeaseClient({ socketPath: d.socketPath });

		await a.acquire(QUEUE_KEY, "A", "parent");
		const bAcquire = b.acquire(QUEUE_KEY, "B", "parent");
		assert.equal(await raceWithSleep(bAcquire, 300), "pending");

		await sleep(1500); // ~3.7 TTLs: A's heartbeat must keep its slot
		assert.equal(await raceWithSleep(bAcquire, 300), "pending", "heartbeats must keep A's lease");

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
		await sleep(1500);
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

	it("passes a parent through with a timeout after queueTimeoutMs; a child waits unlimited", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		await waitForDaemon(d.socketPath);
		const holder = new LeaseClient({ socketPath: d.socketPath });
		await holder.acquire(QUEUE_KEY, "H", "parent"); // occupy the single slot

		const parent = new LeaseClient({ socketPath: d.socketPath, queueTimeoutMs: 300 });
		const child = new LeaseClient({ socketPath: d.socketPath, queueTimeoutMs: 300 });
		const p = parent.acquire(QUEUE_KEY, "P", "parent");
		const c = child.acquire(QUEUE_KEY, "C", "child");

		assert.equal(await p, "timeout", "a parent passes through with a timeout reason after queueTimeoutMs");
		assert.equal(await raceWithSleep(c, 400), "pending", "child default is unlimited: queueTimeoutMs never applies to a child");

		holder.release(QUEUE_KEY, "H");
		assert.equal(await raceWithSleep(c, 3000), "resolved", "the child is granted once the slot frees");

		parent.close();
		child.close();
		holder.close();
		d.stop();
	});
});

describe("daemon autostart", () => {
	const dir = mkdtempSync(join(tmpdir(), "llm-queue-autostart-"));
	const socketPath = join(dir, "queue.sock");
	const configPath = join(dir, "config.json");
	writeFileSync(configPath, JSON.stringify({ defaults: { maxStreams: 1 }, queues: {} }));
	const lockPath = `${socketPath}.pid`;

	after(() => spawnSync("pkill", ["-f", socketPath]));

	it("starts the daemon detached and records its pid in the lockfile", async () => {
		const lease = new LeaseClient({ socketPath, daemonPath: DAEMON, configPath });
		assert.equal((await lease.status()).op, "status");
		assert.ok(Number(readFileSync(lockPath, "utf8")) > 0);
		lease.close();
	});

	it("reclaims a stale lockfile left by a dead daemon", async () => {
		spawnSync("pkill", ["-f", socketPath]);
		await sleep(300);
		writeFileSync(lockPath, "999999");
		const lease = new LeaseClient({ socketPath, daemonPath: DAEMON, configPath });
		assert.equal((await lease.status()).op, "status");
		assert.notEqual(Number(readFileSync(lockPath, "utf8")), 999999);
		lease.close();
	});
});
