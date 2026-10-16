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
