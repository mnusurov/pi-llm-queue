// Per-session lease client over the daemon's unix socket. Fail-open: if the daemon
// is unreachable, requests pass through unqueued.
import { connect, type Socket } from "node:net";
import { spawn } from "node:child_process";
import { existsSync, unlinkSync, writeFileSync } from "node:fs";

export type Role = "parent" | "child";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export interface DaemonPaths {
	socketPath: string;
	daemonPath?: string;
	configPath?: string;
	modelsJsonPath?: string;
}

function socketAlive(socketPath: string): Promise<boolean> {
	return new Promise((resolve) => {
		const sock = connect(socketPath);
		const done = (alive: boolean) => {
			clearTimeout(timer);
			sock.destroy();
			resolve(alive);
		};
		const timer = setTimeout(() => done(false), 500);
		sock.once("connect", () => done(true));
		sock.once("error", () => done(false));
	});
}

/**
 * Start the daemon when nothing owns the socket. Detached spawn + lockfile PID;
 * a lockfile whose socket does not answer is stale and gets reclaimed.
 */
export async function ensureDaemon({ socketPath, daemonPath, configPath, modelsJsonPath }: DaemonPaths): Promise<void> {
	if (!daemonPath) return;
	if (await socketAlive(socketPath)) return;
	const lockPath = `${socketPath}.pid`;
	if (existsSync(lockPath)) unlinkSync(lockPath); // stale lockfile reclaim
	const child = spawn(
		process.execPath,
		[
			daemonPath,
			"--socket",
			socketPath,
			...(configPath ? ["--config", configPath] : []),
			...(modelsJsonPath ? ["--models-json", modelsJsonPath] : []),
		],
		{ detached: true, stdio: "ignore" },
	);
	child.on("error", () => {});
	writeFileSync(lockPath, String(child.pid));
	child.unref();
}

export class LeaseClient {
	private sock: Socket | null = null;
	private msgs: any[] = [];
	private waiter: ((msg: any) => void) | null = null;

	private socketPath: string;
	private paths: DaemonPaths;

	constructor(paths: DaemonPaths) {
		this.paths = paths;
		this.socketPath = paths.socketPath;
	}

	private async connectDaemon(): Promise<boolean> {
		if (this.sock) return true;
		await ensureDaemon(this.paths);
		for (let attempt = 0; attempt < (this.paths.daemonPath ? 20 : 1); attempt++) {
			try {
				const sock = connect(this.socketPath);
				await new Promise((resolve, reject) => {
					sock.once("connect", resolve);
					sock.once("error", reject);
				});
				sock.on("data", (chunk) => {
					for (const line of chunk.toString().split("\n")) {
						if (!line.trim()) continue;
						const msg = JSON.parse(line);
						if (this.waiter) {
							const done = this.waiter;
							this.waiter = null;
							done(msg);
						} else {
							this.msgs.push(msg);
						}
					}
				});
				// Daemon died while queued: release the caller to its request (fail-open).
				sock.on("close", () => {
					this.sock = null;
					if (this.waiter) {
						const done = this.waiter;
						this.waiter = null;
						done({ op: "fail-open" });
					}
				});
				this.sock = sock;
				sock.unref(); // never keep pi's process alive through the lease connection
				return true;
			} catch {
				await sleep(50);
			}
		}
		return false;
	}

	private send(msg: unknown): void {
		this.sock?.write(`${JSON.stringify(msg)}\n`);
	}

	private read(): Promise<any> {
		return new Promise((resolve) => {
			if (this.msgs.length) resolve(this.msgs.shift());
			else this.waiter = resolve;
		});
	}

	/** Resolves when the slot is granted, or immediately when the daemon is unreachable. */
	async acquire(queueKey: string, sessionKey: string, role: Role): Promise<void> {
		if (!(await this.connectDaemon())) return;
		this.sock?.ref(); // a queued request must keep pi alive
		this.send({ op: "acquire", queueKey, sessionKey, role });
		await this.read();
		this.sock?.unref();
	}

	release(queueKey: string, sessionKey: string): void {
		this.send({ op: "release", queueKey, sessionKey });
		this.sock?.unref();
	}

	async status(): Promise<any> {
		if (!(await this.connectDaemon())) return null;
		this.sock?.ref();
		this.send({ op: "status" });
		const msg = await this.read();
		this.sock?.unref();
		return msg;
	}

	close(): void {
		this.sock?.destroy();
		this.sock = null;
	}
}
