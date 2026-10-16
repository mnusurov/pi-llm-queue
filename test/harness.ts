/**
 * Socket-seam test harness: starts the real daemon on a temp unix socket with
 * a temp config, and exposes NDJSON clients. Tests never touch a real model
 * endpoint — the daemon is the only thing under test.
 */
import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { LeaseClient } from "../src/lib/lease-client.ts";

export const DAEMON_ENTRY = path.resolve(import.meta.dirname, "../src/daemon/daemon.js");

export interface TestDaemon {
	dir: string;
	socketPath: string;
	lockPath: string;
	configPath: string;
	pid: number;
	stop: () => Promise<void>;
}

export function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

export function tempDir(prefix = "pi-llm-queue-test-"): string {
	return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

export async function startTestDaemon(options: { config?: Record<string, unknown>; dir?: string } = {}): Promise<TestDaemon> {
	const dir = options.dir ?? tempDir();
	const socketPath = path.join(dir, "queue.sock");
	const lockPath = path.join(dir, "queue.lock");
	const configPath = path.join(dir, "queue.json");
	fs.writeFileSync(configPath, JSON.stringify(options.config ?? {}), "utf8");
	const child: ChildProcess = spawn(process.execPath, [DAEMON_ENTRY], {
		cwd: dir,
		env: { ...process.env, PI_QUEUE_SOCKET: socketPath, PI_QUEUE_LOCK: lockPath, PI_QUEUE_CONFIG: configPath },
		stdio: "ignore",
	});
	child.on("error", () => undefined);
	const deadline = Date.now() + 5000;
	while (Date.now() < deadline) {
		const probe = new LeaseClient(socketPath, { connectTimeoutMs: 200, failOpen: false });
		const pong = await probe.ping();
		probe.close();
		if (pong) {
			return {
				dir,
				socketPath,
				lockPath,
				configPath,
				pid: pong.pid,
				stop: async () => {
					if (!child.killed) child.kill("SIGTERM");
					await new Promise((resolve) => child.once("exit", resolve));
					fs.rmSync(dir, { recursive: true, force: true });
				},
			};
		}
		await sleep(50);
	}
	child.kill("SIGTERM");
	throw new Error("test daemon did not start within 5s");
}

export function client(socketPath: string, options?: ConstructorParameters<typeof LeaseClient>[1]): LeaseClient {
	return new LeaseClient(socketPath, options);
}
