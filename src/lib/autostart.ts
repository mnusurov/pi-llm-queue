/**
 * Daemon autostart: ping the socket, reclaim a stale lockfile, spawn the
 * daemon detached, and wait for it to answer. Fail-open by design: every
 * failure here returns without throwing and the caller proceeds unqueued.
 */
import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { defaultLockPath, defaultSocketPath } from "./config.ts";
import { LeaseClient } from "./lease-client.ts";

export function pidAlive(pid: number): boolean {
	if (!Number.isInteger(pid) || pid <= 0) return false;
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		// EPERM means the pid exists but belongs to another user.
		return (error as NodeJS.ErrnoException).code === "EPERM";
	}
}

export function readLockfile(lockPath: string = defaultLockPath()): number | null {
	try {
		const pid = Number(fs.readFileSync(lockPath, "utf8").trim());
		return Number.isInteger(pid) && pid > 0 ? pid : null;
	} catch {
		return null;
	}
}

/** Remove a lockfile whose pid is no longer running. Returns true when removed. */
export function reclaimStaleLock(lockPath: string = defaultLockPath()): boolean {
	const pid = readLockfile(lockPath);
	if (pid === null) return false;
	if (pidAlive(pid)) return false;
	try {
		fs.unlinkSync(lockPath);
		return true;
	} catch {
		return false;
	}
}

export function defaultDaemonEntry(): string {
	return path.resolve(new URL("../daemon/daemon.js", import.meta.url).pathname);
}

export interface EnsureDaemonOptions {
	socketPath?: string;
	lockPath?: string;
	daemonEntry?: string;
	env?: Record<string, string>;
	waitMs?: number;
}

export interface EnsureDaemonResult {
	running: boolean;
	started: boolean;
	/** True when a stale lockfile (dead daemon pid) was reclaimed. */
	reclaimed: boolean;
}

async function ping(socketPath: string): Promise<boolean> {
	const client = new LeaseClient(socketPath, { failOpen: false, connectTimeoutMs: 300 });
	try {
		return (await client.ping()) !== null;
	} catch {
		return false;
	} finally {
		client.close();
	}
}

export async function ensureDaemon(options: EnsureDaemonOptions = {}): Promise<EnsureDaemonResult> {
	const socketPath = options.socketPath ?? defaultSocketPath();
	const lockPath = options.lockPath ?? defaultLockPath();
	const daemonEntry = options.daemonEntry ?? defaultDaemonEntry();
	const waitMs = options.waitMs ?? 3000;

	if (await ping(socketPath)) return { running: true, started: false, reclaimed: false };

	let reclaimed = false;
	const lockPid = readLockfile(lockPath);
	if (lockPid !== null && !pidAlive(lockPid)) {
		reclaimed = reclaimStaleLock(lockPath);
	} else if (lockPid !== null) {
		// A daemon is starting up elsewhere; give it a moment to bind the socket.
		const deadline = Date.now() + Math.min(waitMs, 1500);
		while (Date.now() < deadline) {
			await new Promise((resolve) => setTimeout(resolve, 100));
			if (await ping(socketPath)) return { running: true, started: false, reclaimed: false };
		}
		return { running: false, started: false, reclaimed: false };
	}

	const child = spawn(process.execPath, [daemonEntry], {
		detached: true,
		stdio: "ignore",
		env: { ...process.env, PI_QUEUE_SOCKET: socketPath, PI_QUEUE_LOCK: lockPath, ...(options.env ?? {}) },
	});
	child.unref();

	const deadline = Date.now() + waitMs;
	while (Date.now() < deadline) {
		if (await ping(socketPath)) return { running: true, started: true, reclaimed };
		await new Promise((resolve) => setTimeout(resolve, 50));
	}
	return { running: false, started: true, reclaimed };
}
