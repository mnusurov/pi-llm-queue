// Per-session lease client over the daemon's unix socket. Fail-open: if the daemon
// is unreachable, requests pass through unqueued.
import { connect, type Socket } from "node:net";
import { spawn } from "node:child_process";
import { readJsonLines } from "./socket-lines.js";
import type { AcquireVerdict, BeatVerdict } from "./lease-verdict.ts";

export type Role = "parent" | "child";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export interface DaemonPaths {
	socketPath: string;
	daemonPath?: string;
	configPath?: string;
	modelsJsonPath?: string;
	/** Heartbeat period; must stay well under the daemon's leaseTtlMs. */
	heartbeatMs?: number;
	/** Daemon unreachable → pass through unqueued (default true). false → a request hard-errors. */
	failOpen?: boolean;
	/** Parent queue-wait warning threshold; on expiry the parent is warned but stays queued. Child is always unlimited. */
	queueTimeoutMs?: number;
}

/**
 * The daemon is unreachable and `failOpen` is false. A typed error, not a verdict: the operator
 * chose to make the queue a single point of failure, so a request must fail rather than slip through.
 */
export class DaemonUnreachableError extends Error {
	constructor() {
		super("pi-llm-queue: daemon unreachable and failOpen is false");
		this.name = "DaemonUnreachableError";
	}
}

/** Internal socket-close signal: a dead connection is not a daemon message and has no wire shape. */
const CLOSED = Symbol("lease socket closed");

/** One entry of the status reply: a Holder or a Waiter, reported by tier and group label. */
export interface StatusSession {
	sessionKey: string;
	role: Role;
	/** The `group` label the client acquired with; absent when it sent none. */
	group?: string;
}

export interface StatusQueue {
	queueKey: string;
	/** null is JSON's form of Infinity: a passthrough queue has unlimited slots. */
	maxStreams: number | null;
	switches: number;
	holders: StatusSession[];
	waiting: StatusSession[];
}

export interface StatusReply {
	op: "status";
	queues: StatusQueue[];
}

/**
 * Start the daemon when nothing owns the socket. No liveness probe here: the daemon's own
 * start() resolves the race (a live socket makes the second daemon exit 0, a stale socket file
 * is reclaimed), so one detached spawn per session start is enough.
 */
export async function ensureDaemon({ socketPath, daemonPath, configPath, modelsJsonPath }: DaemonPaths): Promise<void> {
	if (!daemonPath) return;
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
	child.unref();
}

/** A socket reply, or the internal close signal — the two things a pending request resolves with. */
type DaemonReply = Record<string, any> | typeof CLOSED;

interface Pending {
	match: (msg: any) => boolean;
	resolve: (msg: DaemonReply) => void;
}

export class LeaseClient {
	private sock: Socket | null = null;
	private pending: Pending[] = [];
	private heartbeat: ReturnType<typeof setInterval> | null = null;
	// Socket ref/unref is one switch, not a counter: ref/unref per call lets any second op
	// (a beat, a status poll) free the process to exit while a queued acquire is still waiting.
	// Count outstanding requests instead: ref on 0 -> 1, unref when the last one finishes.
	private outstanding = 0;
	private refSocket(): void {
		if (this.outstanding++ === 0) this.sock?.ref();
	}
	private unrefSocket(): void {
		if (--this.outstanding <= 0) {
			this.outstanding = 0;
			this.sock?.unref();
		}
	}

	private socketPath: string;
	private paths: DaemonPaths;
	private failOpen: boolean;

	constructor(paths: DaemonPaths) {
		this.paths = paths;
		this.socketPath = paths.socketPath;
		this.failOpen = paths.failOpen ?? true;
	}

	// failOpen: false and the daemon is gone → the request hard-errors (the queue is a
	// single point of failure by the operator's explicit choice).
	private unreachable(): never {
		throw new DaemonUnreachableError();
	}

	/** The Fail-open policy for an unreachable daemon: pass through unqueued, or refuse. */
	private passthrough(): { kind: "passthrough" } {
		if (!this.failOpen) this.unreachable();
		return { kind: "passthrough" };
	}

	private async connectDaemon(): Promise<boolean> {
		if (this.sock) return true;
		await ensureDaemon(this.paths);
		for (let attempt = 0; attempt < (this.paths.daemonPath ? 60 : 1); attempt++) {
			try {
				const sock = connect(this.socketPath);
				await new Promise((resolve, reject) => {
					sock.once("connect", resolve);
					sock.once("error", reject);
				});
				readJsonLines(sock, (msg) => {
					// Deliver by match; a reply with no matching request (e.g. a release "ok")
					// is dropped, never buffered for a later acquire to consume.
					const at = this.pending.findIndex((p) => p.match(msg));
					if (at < 0) return;
					const [p] = this.pending.splice(at, 1);
					p.resolve(msg);
				});
				// Daemon died while queued: release every caller to its request (fail-open).
				sock.on("close", () => {
					this.sock = null;
					const wakers = this.pending.splice(0, this.pending.length);
					for (const p of wakers) p.resolve(CLOSED);
				});
				this.sock = sock;
				sock.unref(); // never keep pi's process alive through the lease connection
				// Lease heartbeat: a live session must never lose its slot to the TTL. Renew is
				// connection-wide, so it only refreshes the leases still on this socket; a session's
				// lease leaves the socket the moment it releases (shutdown/abort/settle), so a dead
				// session can never be kept alive past its TTL as a zombie (#53 defect 4).
				if (!this.heartbeat) this.armHeartbeat();
				return true;
			} catch {
				await sleep(50);
			}
		}
		return false;
	}

	private armHeartbeat(): void {
		this.heartbeat = setInterval(() => this.send({ op: "renew" }), this.paths.heartbeatMs ?? 10000);
		this.heartbeat.unref();
	}

	/**
	 * Re-arm the connection-wide heartbeat at a new period (#81). The extension passes the smallest
	 * per-queue keyed beat so a bare renew alone keeps every lease alive under that queue's TTL;
	 * called on config hot-reload, which is why a live timer must be restarted, not left in place.
	 */
	setHeartbeatMs(ms: number): void {
		if (!Number.isFinite(ms) || ms <= 0) return;
		this.paths.heartbeatMs = ms;
		if (this.heartbeat) {
			clearInterval(this.heartbeat);
			this.armHeartbeat();
		}
	}

	private send(msg: unknown): void {
		this.sock?.write(`${JSON.stringify(msg)}\n`);
	}

	private read(match: (msg: any) => boolean): Promise<DaemonReply> {
		return new Promise((resolve) => this.pending.push({ match, resolve }));
	}

	/**
	 * Resolves `held` when the slot is granted, `passthrough` when the daemon is unreachable, or
	 * `cancelled` when `signal` aborts (the queued request is cancelled at the daemon first).
	 * Throws `DaemonUnreachableError` instead of `passthrough` when `failOpen` is false.
	 * `onQueued` fires while the request waits, so the caller can show a live indicator.
	 */
	async acquire(
		queueKey: string,
		sessionKey: string,
		role: Role,
		group?: string,
		signal?: AbortSignal,
		onQueued?: (info: { position: number; holders: string[] }) => void,
		onTimeout?: () => void,
	): Promise<AcquireVerdict> {
		if (!(await this.connectDaemon())) return this.passthrough();
		this.refSocket();
		this.send({ op: "acquire", queueKey, sessionKey, role, group, activity: true });
		const granted = (msg: any) => msg.op === "granted" && msg.queueKey === queueKey && msg.sessionKey === sessionKey;
		const entry: Pending = { match: granted, resolve: () => {} };
		// A child must never pass through unqueued (it would defeat serialisation), so the
		// queue-wait cap applies to parents only; a child's default is unlimited.
		const timeoutMs = role === "child" ? Infinity : this.paths.queueTimeoutMs ?? Infinity;
		// A queued acquire stays unanswered by design, so the indicator reads its position
		// from status polls: a short grace (an idle slot grants instantly) then every 2 s.
		// shortcut: position is enqueue order, not priority-grant order; upgrade when the
		// daemon pushes queued acks.
		let poll: ReturnType<typeof setInterval> | null = null;
		let timer: ReturnType<typeof setTimeout> | null = null;
		let settled = false;
		const report = async () => {
			const s = await this.status();
			if (settled) return; // the grant landed mid-poll: never re-arm the indicator after it
			const q = s?.queues.find((entry) => entry.queueKey === queueKey);
			const at = q?.waiting.findIndex((w) => w.sessionKey === sessionKey) ?? -1;
			// The indicator names the slot holders by session id only: tier and group are for a
			// dashboard, not for the waiting UX, which stays as it was.
			if (q && at >= 0) onQueued?.({ position: at + 1, holders: q.holders.map((h) => h.sessionKey) });
		};
		const firstPoll = onQueued
			? setTimeout(() => {
					report();
					poll = setInterval(report, 2000);
					poll.unref();
				}, 300)
			: null;
		firstPoll?.unref();
		const verdict = await new Promise<AcquireVerdict>((resolve) => {
			let done = false;
			const finish = (r: AcquireVerdict) => {
				if (done) return;
				done = true;
				if (timer) clearTimeout(timer);
				signal?.removeEventListener("abort", onAbort);
				resolve(r);
			};
			const onAbort = () => {
				const at = this.pending.indexOf(entry);
				if (at >= 0) this.pending.splice(at, 1);
				this.send({ op: "cancel", queueKey, sessionKey });
				finish({ kind: "cancelled" });
			};
			// Grant delivery passes the granted msg; a daemon death mid-wait passes the close signal.
			entry.resolve = (msg) => finish(msg === CLOSED ? { kind: "passthrough" } : { kind: "held" });
			this.pending.push(entry);
			if (signal) {
				if (signal.aborted) {
					onAbort();
					return;
				}
				signal.addEventListener("abort", onAbort, { once: true });
			}
			if (isFinite(timeoutMs)) {
				timer = setTimeout(() => {
					// Warn-only: the parent is notified but stays queued — idle-yield self-heals a stuck queue.
					onTimeout?.();
				}, timeoutMs);
				timer.unref();
			}
		});
		if (firstPoll) clearTimeout(firstPoll);
		if (poll) clearInterval(poll);
		settled = true;
		this.unrefSocket();
		// A daemon that died mid-wait passed through: honour failOpen for that verdict too.
		return verdict.kind === "passthrough" ? this.passthrough() : verdict;
	}

	release(queueKey: string, sessionKey: string): void {
		this.send({ op: "release", queueKey, sessionKey });
	}

	/** Re-tier a live queued waiter (from /queue promote|demote); fire-and-forget. */
	setRole(sessionKey: string, role: Role): void {
		this.send({ op: "role", sessionKey, role });
	}

	/**
	 * Activity beat: the holder is using the slot, so its TTL refreshes.
	 * Resolves `lost` when the daemon no longer grants the slot (TTL expiry, a release) — the
	 * caller must re-queue — and `passthrough` when the daemon is unreachable, mid-beat included.
	 */
	async beat(queueKey: string, sessionKey: string): Promise<BeatVerdict> {
		if (!(await this.connectDaemon())) return this.passthrough();
		this.refSocket(); // the reply must keep the process/worker alive while we await it
		this.send({ op: "renew", queueKey, sessionKey });
		const msg = await this.read((m) => m.op === "ok" && m.queueKey === queueKey && m.sessionKey === sessionKey);
		this.unrefSocket();
		if (msg === CLOSED) return this.passthrough(); // daemon died mid-beat
		return msg.granted === false ? { kind: "lost" } : { kind: "held" };
	}

	/**
	 * Shutdown intent (#63): announce "this client is leaving" to the daemon, then close the
	 * socket. The daemon treats bye as a disconnect (release leases, decrement the client count),
	 * so the idle-exit arms at the word — a pi process that lingers after its last session cannot
	 * keep the daemon resident. Fire-and-forget: no reply, like role/cancel. Idempotent: pi
	 * converges cancellation, reload, session replacement and exit on the same shutdown path.
	 */
	bye(): void {
		if (this.heartbeat) {
			clearInterval(this.heartbeat);
			this.heartbeat = null;
		}
		const sock = this.sock;
		if (!sock) return;
		try {
			sock.write(`${JSON.stringify({ op: "bye" })}\n`);
		} catch {}
		sock.destroy(); // the close handler clears this.sock and releases any pending request
	}

	async status(): Promise<StatusReply | null> {
		if (!(await this.connectDaemon())) return null;
		this.refSocket();
		this.send({ op: "status" });
		const msg = await this.read((m) => m.op === "status");
		this.unrefSocket();
		// A close mid-request resolves every pending request with the close signal: never hand it
		// to /queue, which expects a StatusReply and would throw on `.queues.map`.
		return msg === CLOSED ? null : (msg as StatusReply);
	}

	close(): void {
		this.sock?.destroy();
		this.sock = null;
	}
}
