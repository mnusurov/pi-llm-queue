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
	/** Heartbeat period; must stay well under the daemon's leaseTtlMs. */
	heartbeatMs?: number;
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
	// A failed spawn (bad path, ENOENT) must surface, not vanish silently.
	child.on("error", (error) => console.error(`pi-llm-queue: daemon spawn failed: ${error.message}`));
	writeFileSync(lockPath, String(child.pid));
	child.unref();
}

interface Pending {
	match: (msg: any) => boolean;
	resolve: (msg: any) => void;
}

export class LeaseClient {
	private sock: Socket | null = null;
	private pending: Pending[] = [];
	private heartbeat: ReturnType<typeof setInterval> | null = null;

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
				let buffer = "";
				sock.on("data", (chunk) => {
					// Socket reads are not line-aligned: accumulate partial lines, never parse a fragment.
					buffer += chunk.toString();
					const lines = buffer.split("\n");
					buffer = lines.pop() ?? "";
					for (const line of lines) {
						if (!line.trim()) continue;
						let msg;
						try {
							msg = JSON.parse(line);
						} catch {
							continue;
						}
						// Deliver by match; a reply with no matching request (e.g. a release "ok")
						// is dropped, never buffered for a later acquire to consume.
						const at = this.pending.findIndex((p) => p.match(msg));
						if (at < 0) continue;
						const [p] = this.pending.splice(at, 1);
						p.resolve(msg);
					}
				});
				// Daemon died while queued: release every caller to its request (fail-open).
				sock.on("close", () => {
					this.sock = null;
					const wakers = this.pending.splice(0, this.pending.length);
					for (const p of wakers) p.resolve({ op: "fail-open" });
				});
				this.sock = sock;
				sock.unref(); // never keep pi's process alive through the lease connection
				// Lease heartbeat: a live session must never lose its slot to the TTL.
				this.heartbeat ??= setInterval(() => this.send({ op: "renew" }), this.paths.heartbeatMs ?? 10000);
				this.heartbeat.unref();
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

	private read(match: (msg: any) => boolean): Promise<any> {
		return new Promise((resolve) => this.pending.push({ match, resolve }));
	}

	/**
	 * Resolves when the slot is granted, when the daemon is unreachable, or when
	 * `signal` aborts (the queued request is cancelled at the daemon first).
	 * `onQueued` fires while the request waits, so the caller can show a live indicator.
	 */
	async acquire(
		queueKey: string,
		sessionKey: string,
		role: Role,
		group?: string,
		signal?: AbortSignal,
		onQueued?: (info: { position: number; holders: string[] }) => void,
	): Promise<void> {
		if (!(await this.connectDaemon())) return;
		this.sock?.ref(); // a queued request must keep pi alive
		this.send({ op: "acquire", queueKey, sessionKey, role, group });
		const granted = (msg: any) => msg.op === "granted" && msg.queueKey === queueKey && msg.sessionKey === sessionKey;
		const entry: Pending = { match: granted, resolve: () => {} };
		const onAbort = () => {
			const at = this.pending.indexOf(entry);
			if (at >= 0) this.pending.splice(at, 1);
			this.send({ op: "cancel", queueKey, sessionKey });
			entry.resolve({ op: "aborted" });
		};
		// A queued acquire stays unanswered by design, so the indicator reads its position
		// from status polls: a short grace (an idle slot grants instantly) then every 2 s.
		// shortcut: position is enqueue order, not priority-grant order; upgrade when the
		// daemon pushes queued acks.
		let poll: ReturnType<typeof setInterval> | null = null;
		let settled = false;
		const report = async () => {
			const s = await this.status();
			if (settled) return; // the grant landed mid-poll: never re-arm the indicator after it
			const q = s?.queues?.find((q: any) => q.queueKey === queueKey);
			const at = q?.waiting.findIndex((w: any) => w.sessionKey === sessionKey) ?? -1;
			if (at >= 0) onQueued?.({ position: at + 1, holders: q.holders });
		};
		const firstPoll = onQueued
			? setTimeout(() => {
					report();
					poll = setInterval(report, 2000);
					poll.unref();
				}, 300)
			: null;
		firstPoll?.unref();
		await new Promise<void>((resolve) => {
			entry.resolve = () => {
				signal?.removeEventListener("abort", onAbort);
				resolve();
			};
			this.pending.push(entry);
			if (signal) {
				if (signal.aborted) {
					onAbort();
					return;
				}
				signal.addEventListener("abort", onAbort, { once: true });
			}
		});
		if (firstPoll) clearTimeout(firstPoll);
		if (poll) clearInterval(poll);
		settled = true;
		this.sock?.unref();
	}

	release(queueKey: string, sessionKey: string): void {
		this.send({ op: "release", queueKey, sessionKey });
		this.sock?.unref();
	}

	/** Re-tier a live queued waiter (from /queue promote|demote); fire-and-forget. */
	setRole(sessionKey: string, role: Role): void {
		this.send({ op: "role", sessionKey, role });
	}

	/**
	 * Activity beat: the holder is using the slot, so its idle/monopoly quanta restart.
	 * Resolves false when the daemon has switched the slot away — the caller must re-queue.
	 */
	async beat(queueKey: string, sessionKey: string): Promise<boolean> {
		if (!(await this.connectDaemon())) return true; // fail-open: stream unqueued
		this.sock?.ref(); // the reply must keep the process/worker alive while we await it
		this.send({ op: "renew", queueKey, sessionKey });
		const msg = await this.read((m) => m.op === "ok" && m.queueKey === queueKey && m.sessionKey === sessionKey);
		this.sock?.unref();
		return msg.granted !== false;
	}

	async status(): Promise<any> {
		if (!(await this.connectDaemon())) return null;
		this.sock?.ref();
		this.send({ op: "status" });
		const msg = await this.read((m) => m.op === "status");
		this.sock?.unref();
		return msg;
	}

	close(): void {
		this.sock?.destroy();
		this.sock = null;
	}
}
