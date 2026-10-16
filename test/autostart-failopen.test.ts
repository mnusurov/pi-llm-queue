/**
 * Fail-open and daemon autostart: the queue must never be a single point of
 * failure, must autostart from the client side, and must reclaim a lockfile
 * left behind by a dead daemon. Process-level + socket-seam tests, stub only.
 */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { after, afterEach, describe, test } from "node:test";
import { clientTracker, DAEMON_ENTRY, sleep, startTestDaemon, tempDir } from "./harness.ts";
import { ensureDaemon, pidAlive, readLockfile, reclaimStaleLock } from "../src/lib/autostart.ts";
import { LeaseClient } from "../src/lib/lease-client.ts";

const ENDPOINT = "http://127.0.0.1:8003/v1";

describe("fail-open", () => {
	const tracker = clientTracker();
	afterEach(() => tracker.closeAll());
	test("acquire passes through when no daemon is listening", async () => {
		const dir = tempDir();
		const orphan = new LeaseClient(path.join(dir, "absent.sock"), { connectTimeoutMs: 200 });
		const grant = await orphan.acquire({ queueKey: ENDPOINT, sessionKey: "s", role: "parent" });
		assert.equal(grant.granted, true, "no daemon: request proceeds unqueued");
		assert.equal(grant.failOpen, true);
		assert.equal(await orphan.status(), null);
		assert.equal(await orphan.ping(), null);
		orphan.close();
		fs.rmSync(dir, { recursive: true, force: true });
	});

	test("a daemon that dies mid-session leaves the client passing through", async () => {
		const daemon = await startTestDaemon({ config: { queues: { [ENDPOINT]: { maxStreams: 1 } } } });
		const a = tracker.make(daemon.socketPath);
		const first = await a.acquire({ queueKey: ENDPOINT, sessionKey: "s1", role: "parent" });
		assert.equal(first.granted, true);
		await daemon.stop();
		const second = await a.acquire({ queueKey: ENDPOINT, sessionKey: "s1", role: "parent" });
		assert.equal(second.granted, true, "requests keep flowing after the daemon dies");
		assert.equal(second.failOpen, true);
		a.close();
	});

	test("queueTimeoutMs degrades to a warning, not a hard failure", async () => {
		const daemon = await startTestDaemon({ config: { queues: { [ENDPOINT]: { maxStreams: 1 } } } });
		try {
			const holder = tracker.make(daemon.socketPath);
			const blocked = tracker.make(daemon.socketPath);
			const grant = await holder.acquire({ queueKey: ENDPOINT, sessionKey: "holder", role: "parent" });
			assert.equal(grant.granted, true);
			const result = await blocked.acquire({ queueKey: ENDPOINT, sessionKey: "blocked", role: "parent", timeoutMs: 400 });
			assert.equal(result.granted, true);
			assert.equal(result.timedOut, true, "queueTimeoutMs expiry passes through with a flag");
			holder.close();
			blocked.close();
		} finally {
			await daemon.stop();
		}
	});
});

describe("daemon autostart", () => {
	const tracker = clientTracker();
	afterEach(() => tracker.closeAll());
	const dirs: string[] = [];
	after(() => {
		// ensureDaemon spawns detached daemons: stop them before removing their dirs
		// so a test run never leaves orphan daemons behind.
		for (const dir of dirs) {
			const pid = readLockfile(path.join(dir, "queue.lock"));
			if (pid !== null && pidAlive(pid)) {
				try {
					process.kill(pid, "SIGTERM");
				} catch {
					/* already gone */
				}
			}
			fs.rmSync(dir, { recursive: true, force: true });
		}
	});

	test("ensureDaemon starts the daemon, writes a lockfile, and is idempotent", async () => {
		const dir = tempDir();
		dirs.push(dir);
		const socketPath = path.join(dir, "queue.sock");
		const lockPath = path.join(dir, "queue.lock");

		const first = await ensureDaemon({ socketPath, lockPath, daemonEntry: DAEMON_ENTRY });
		assert.equal(first.running, true, "daemon answers on the socket");
		assert.equal(first.started, true, "this call started it");
		const pid = readLockfile(lockPath);
		assert.ok(pid && pidAlive(pid), "lockfile holds the live daemon pid");

		const second = await ensureDaemon({ socketPath, lockPath, daemonEntry: DAEMON_ENTRY });
		assert.equal(second.running, true);
		assert.equal(second.started, false, "a live daemon is reused, not restarted");

		const probe = tracker.make(socketPath, { connectTimeoutMs: 300 });
		const pong = await probe.ping();
		probe.close();
		assert.ok(pong && pong.pid === pid, "socket is served by the lockfile pid");
	});

	test("a stale lockfile (dead daemon pid) is reclaimed", async () => {
		const dir = tempDir();
		dirs.push(dir);
		const socketPath = path.join(dir, "queue.sock");
		const lockPath = path.join(dir, "queue.lock");
		fs.writeFileSync(lockPath, "999999\n", "utf8");

		const result = await ensureDaemon({ socketPath, lockPath, daemonEntry: DAEMON_ENTRY, waitMs: 4000 });
		assert.equal(result.reclaimed, true, "dead pid lockfile is removed");
		assert.equal(result.running, true, "daemon starts after reclaim");
		assert.equal(readLockfile(lockPath) !== 999999, true, "lockfile rewritten with the new pid");
	});

	test("a live lockfile from a foreign process blocks a duplicate spawn", async () => {
		const dir = tempDir();
		dirs.push(dir);
		const socketPath = path.join(dir, "queue.sock");
		const lockPath = path.join(dir, "queue.lock");
		const squatter = spawn("sleep", ["20"], { stdio: "ignore" });
		squatter.unref();
		fs.writeFileSync(lockPath, `${squatter.pid}\n`, "utf8");
		try {
			const result = await ensureDaemon({ socketPath, lockPath, daemonEntry: DAEMON_ENTRY, waitMs: 800 });
			assert.equal(result.running, false, "no daemon is reachable");
			assert.equal(result.started, false, "a live lockfile is not overwritten by a second daemon");
			assert.equal(readLockfile(lockPath), squatter.pid, "foreign lockfile left intact");
		} finally {
			squatter.kill("SIGTERM");
		}
	});

	test("reclaimStaleLock removes only dead lockfiles", async () => {
		const dir = tempDir();
		dirs.push(dir);
		const dead = path.join(dir, "dead.lock");
		const live = path.join(dir, "live.lock");
		fs.writeFileSync(dead, "999998\n", "utf8");
		const squatter = spawn("sleep", ["20"], { stdio: "ignore" });
		squatter.unref();
		fs.writeFileSync(live, `${squatter.pid}\n`, "utf8");
		try {
			assert.equal(reclaimStaleLock(dead), true);
			assert.equal(fs.existsSync(dead), false);
			assert.equal(reclaimStaleLock(live), false);
			assert.equal(fs.existsSync(live), true);
		} finally {
			squatter.kill("SIGTERM");
			await sleep(10);
		}
	});
});
