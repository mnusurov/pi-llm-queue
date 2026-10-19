// Socket-seam test harness: spawns the daemon and speaks its JSON line protocol.
import { spawn, type ChildProcess } from "node:child_process";
import { connect } from "node:net";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

export const DAEMON = decodeURIComponent(new URL("../src/daemon.js", import.meta.url).pathname);
export const QUEUE_KEY = "http://model.test/v1";

export interface Daemon {
	socketPath: string;
	configPath: string;
	proc: ChildProcess;
	stop(): void;
}

// Every harness daemon/socket is tracked so a failing assert cannot leave them alive:
// ref'd sockets and live daemons keep `node --test` from exiting. Call stopAllDaemons()
// and closeAllClients() from an `after` hook in each suite.
const spawned: ChildProcess[] = [];
const sockets: import("node:net").Socket[] = [];

export function stopAllDaemons(): void {
	for (const proc of spawned) proc.kill();
	spawned.length = 0;
}

export function closeAllClients(): void {
	for (const sock of sockets) sock.destroy();
	sockets.length = 0;
}

export function startDaemon(config: Record<string, any> = { defaults: { maxStreams: 1 }, queues: {} }, modelsJson?: unknown, modelsJsonPath?: string, sweepMs?: number): Daemon {
	const dir = mkdtempSync(join(tmpdir(), "llm-queue-"));
	const socketPath = join(dir, "queue.sock");
	const configPath = join(dir, "config.json");
	// The production default (0, #60) exits the daemon the moment its last counted client closes;
	// suites that reuse a daemon across connection gaps opt out here. Idle-exit/lifetime tests set
	// the field explicitly. 1e9 ms ≈ 11 days, and JSON cannot carry Infinity.
	const merged = { ...config, defaults: { daemonIdleExitMs: 1e9, ...(config.defaults ?? {}) } };
	writeFileSync(configPath, JSON.stringify(merged));
	const args = [DAEMON, "--socket", socketPath, "--config", configPath, ...(sweepMs ? ["--sweep-ms", String(sweepMs)] : [])];
	if (modelsJsonPath) {
		// A catalog path the daemon must survive not having (models.json is optional for pi).
		args.push("--models-json", modelsJsonPath);
	} else if (modelsJson !== undefined) {
		const written = join(dir, "models.json");
		writeFileSync(written, JSON.stringify(modelsJson));
		args.push("--models-json", written);
	}
	const proc = spawn(process.execPath, args, { stdio: "ignore" });
	proc.unref();
	spawned.push(proc);
	return { socketPath, configPath, proc, stop: () => proc.kill() };
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export interface Client {
	send(msg: unknown): void;
	/** Next message from the daemon, rejecting if none arrives within ms. */
	next(ms: number): Promise<any>;
	close(): void;
}

async function tryConnect(socketPath: string): Promise<import("node:net").Socket> {
	const sock = connect(socketPath);
	await new Promise((resolve, reject) => {
		sock.once("connect", resolve);
		sock.once("error", reject);
	});
	return sock;
}

export async function connectClient(socketPath: string): Promise<Client> {
	for (let attempt = 0; attempt < 40; attempt++) {
		try {
			const sock = await tryConnect(socketPath);
			sockets.push(sock);
			return makeClient(sock);
		} catch {
			await sleep(50);
		}
	}
	throw new Error(`daemon socket never became reachable: ${socketPath}`);
}

export async function waitForDaemon(socketPath: string): Promise<void> {
	const client = await connectClient(socketPath);
	client.close();
}

function makeClient(sock: import("node:net").Socket): Client {
	const messages: any[] = [];
	let waiter: ((msg: any) => void) | null = null;
	sock.on("data", (chunk) => {
		for (const line of chunk.toString().split("\n")) {
			if (!line.trim()) continue;
			const msg = JSON.parse(line);
			if (waiter) {
				const resolve = waiter;
				waiter = null;
				resolve(msg);
			} else {
				messages.push(msg);
			}
		}
	});
	return {
		send: (msg) => sock.write(`${JSON.stringify(msg)}\n`),
		next: (ms) =>
			new Promise((resolve, reject) => {
				const done = (msg: any) => {
					clearTimeout(timer);
					if (waiter === done) waiter = null;
					resolve(msg);
				};
				const timer = setTimeout(() => {
					if (waiter === done) waiter = null;
					reject(new Error(`no message from daemon within ${ms}ms`));
				}, ms);
				if (messages.length) done(messages.shift());
				else waiter = done;
			}),
		close: () => sock.destroy(),
	};
}
