import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, describe, it } from "node:test";
import { LeaseClient, ensureDaemon } from "../src/lease-client.ts";
import { DAEMON, QUEUE_KEY, sleep, startDaemon, waitForDaemon } from "./harness.ts";

const raceWithSleep = (promise: Promise<void>, ms: number) =>
	Promise.race([promise.then(() => "resolved"), sleep(ms).then(() => "pending")]);

describe("lease client", () => {
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

	it("cancels a queued acquire when the abort signal fires", async () => {
		const d = startDaemon({ defaults: { maxStreams: 1 }, queues: { [QUEUE_KEY]: { maxStreams: 1 } } });
		await waitForDaemon(d.socketPath);
		const holder = new LeaseClient({ socketPath: d.socketPath });
		await holder.acquire(QUEUE_KEY, "H", "parent");
		const c = new LeaseClient({ socketPath: d.socketPath });
		const ac = new AbortController();
		const queued = c.acquire(QUEUE_KEY, "C", "parent", ac.signal);
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
