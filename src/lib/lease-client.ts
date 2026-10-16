/**
 * Socket client for the pi-llm-queue daemon.
 *
 * One client per pi session. Grants are per session lease, so a session's
 * consecutive requests keep the same slot (cache-warm append-only prefix).
 * Transport failure degrades fail-open: requests pass through unqueued.
 */
import net from "node:net";
import { defaultSocketPath } from "./config.ts";

export type Role = "parent" | "child";

export interface LeaseGrant {
	granted: boolean;
	leaseId?: string;
	/** Daemon unreachable / protocol failure: caller proceeds unqueued. */
	failOpen?: boolean;
	/** queueTimeoutMs expired: caller proceeds unqueued. */
	timedOut?: boolean;
	aborted?: boolean;
	position?: number;
}

export interface QueueStatus {
	key: string;
	maxStreams: number;
	holders: { sessionKey: string; role: Role; leaseId: string; acquiredAt: number; expiresAt: number }[];
	waiting: { parent: number; child: number };
	waiters: { sessionKey: string; role: Role; waitedMs: number }[];
	switches: number;
	grants: number;
}

interface Msg {
	type?: string;
	token?: string;
	ok?: boolean;
	state?: string;
	[key: string]: unknown;
}

export interface LeaseClientOptions {
	failOpen?: boolean;
	connectTimeoutMs?: number;
	onLeaseExpired?: (leaseId: string, reason: string) => void;
	onDisconnect?: () => void;
}

export class LeaseClient {
	readonly socketPath: string;
	private socket: net.Socket | null = null;
	private buffer = "";
	private seq = 0;
	private pending = new Map<string, (msg: Msg | null) => void>();
	private closed = false;
	private readonly failOpen: boolean;
	private readonly connectTimeoutMs: number;
	private readonly options: LeaseClientOptions;

	constructor(socketPath: string = defaultSocketPath(), options: LeaseClientOptions = {}) {
		this.socketPath = socketPath;
		this.options = options;
		this.failOpen = options.failOpen ?? true;
		this.connectTimeoutMs = options.connectTimeoutMs ?? 1000;
	}

	private dispatch(msg: Msg | null) {
		if (!msg) {
			for (const resolve of this.pending.values()) resolve(null);
			this.pending.clear();
			this.options.onDisconnect?.();
			return;
		}
		if (msg.token && this.pending.has(msg.token)) {
			const resolve = this.pending.get(msg.token)!;
			this.pending.delete(msg.token);
			resolve(msg);
			return;
		}
		if (msg.type === "lease_expired" && typeof msg.leaseId === "string") {
			this.options.onLeaseExpired?.(msg.leaseId, String(msg.reason ?? "ttl"));
		}
	}

	private ensureConnected(): Promise<boolean> {
		if (this.socket && !this.socket.destroyed) return Promise.resolve(true);
		return new Promise((resolve) => {
			let settled = false;
			const settle = (value: boolean) => {
				if (settled) return;
				settled = true;
				clearTimeout(timer);
				resolve(value);
			};
			const timer = setTimeout(() => {
				sock.destroy();
				settle(false);
			}, this.connectTimeoutMs);
			const sock = net.connect(this.socketPath);
			sock.once("connect", () => {
				this.socket = sock;
				this.buffer = "";
				sock.on("data", (chunk) => this.onData(chunk));
				sock.on("close", () => {
					if (this.socket === sock) {
						this.socket = null;
						this.dispatch(null);
					}
				});
				sock.on("error", () => sock.destroy());
				settle(true);
			});
			sock.once("error", () => {
				this.socket = null;
				settle(false);
			});
		});
	}

	private onData(chunk: Buffer) {
		this.buffer += chunk.toString("utf8");
		let newline: number;
		while ((newline = this.buffer.indexOf("\n")) >= 0) {
			const line = this.buffer.slice(0, newline).trim();
			this.buffer = this.buffer.slice(newline + 1);
			if (!line) continue;
			try {
				this.dispatch(JSON.parse(line) as Msg);
			} catch {
				/* ignore malformed line */
			}
		}
	}

	private write(msg: Msg): boolean {
		if (!this.socket || this.socket.destroyed) return false;
		this.socket.write(`${JSON.stringify(msg)}\n`);
		return true;
	}

	private request(msg: Msg, timeoutMs?: number): Promise<Msg | null> {
		return new Promise((resolve) => {
			const token = msg.token ?? `t${++this.seq}`;
			const payload = { ...msg, token };
			let settled = false;
			const settle = (value: Msg | null) => {
				if (settled) return;
				settled = true;
				if (timer) clearTimeout(timer);
				this.pending.delete(token);
				resolve(value);
			};
			this.pending.set(token, settle);
			const timer = timeoutMs ? setTimeout(() => settle(null), timeoutMs) : null;
			if (!this.write(payload)) settle(null);
		});
	}

	/**
	 * Wait for a slot on `queueKey` for `sessionKey`. Resolves when granted,
	 * fail-open when the daemon is unreachable, timed out, or aborted.
	 */
	async acquire(args: {
		queueKey: string;
		sessionKey: string;
		role: Role;
		group?: string;
		signal?: AbortSignal;
		timeoutMs?: number | null;
	}): Promise<LeaseGrant> {
		if (this.closed) return { granted: true, failOpen: true };
		const connected = await this.ensureConnected();
		if (!connected) return { granted: true, failOpen: this.failOpen };
		const token = `t${++this.seq}`;
		const ack = await this.request(
			{
				type: "acquire",
				token,
				queueKey: args.queueKey,
				sessionKey: args.sessionKey,
				role: args.role,
				...(args.group ? { group: args.group } : {}),
			},
			2000,
		);
		if (!ack || ack.ok === false) return { granted: true, failOpen: this.failOpen };
		if (ack.state === "granted") return { granted: true, leaseId: ack.leaseId as string | undefined, position: 0 };
		const position = typeof ack.position === "number" ? ack.position : 0;
		return new Promise((resolve) => {
			let settled = false;
			const cleanup = () => {
				if (settled) return;
				settled = true;
				this.pending.delete(token);
				if (timer) clearTimeout(timer);
				args.signal?.removeEventListener("abort", onAbort);
			};
			const settle = (grant: LeaseGrant) => {
				cleanup();
				resolve(grant);
			};
			this.pending.set(token, (msg) => {
				if (!msg) settle({ granted: true, failOpen: this.failOpen });
				else if (msg.state === "granted") settle({ granted: true, leaseId: msg.leaseId as string | undefined, position: 0 });
				else if (msg.state === "cancelled") settle({ granted: false, aborted: true });
				else settle({ granted: true, failOpen: this.failOpen });
			});
			const timer =
				args.timeoutMs && args.timeoutMs > 0
					? setTimeout(() => {
							this.write({ type: "cancel", token });
							settle({ granted: true, failOpen: this.failOpen, timedOut: true, position });
						}, args.timeoutMs)
					: null;
			const onAbort = () => {
				this.write({ type: "cancel", token });
				settle({ granted: false, aborted: true });
			};
			if (args.signal) {
				if (args.signal.aborted) onAbort();
				else args.signal.addEventListener("abort", onAbort, { once: true });
			}
		});
	}

	async renew(leaseId: string): Promise<boolean> {
		if (this.closed || !this.socket) return false;
		const resp = await this.request({ type: "renew", leaseId }, 2000);
		return !!resp && resp.ok === true && resp.state === "renewed";
	}

	async release(leaseId: string): Promise<void> {
		if (this.closed || !this.socket) return;
		await this.request({ type: "release", leaseId }, 2000);
	}

	async status(): Promise<QueueStatus[] | null> {
		if (this.closed) return null;
		if (!(await this.ensureConnected())) return null;
		const resp = await this.request({ type: "status" }, 2000);
		if (!resp || resp.ok === false || !Array.isArray(resp.queues)) return null;
		return resp.queues as QueueStatus[];
	}

	async ping(): Promise<{ pid: number; uptimeMs: number } | null> {
		if (this.closed) return null;
		if (!(await this.ensureConnected())) return null;
		const resp = await this.request({ type: "ping" }, 2000);
		if (!resp || resp.ok === false) return null;
		return { pid: Number(resp.pid), uptimeMs: Number(resp.uptimeMs) };
	}

	close(): void {
		this.closed = true;
		this.socket?.destroy();
		this.socket = null;
	}
}
