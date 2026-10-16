/**
 * Socket client for the pi-llm-queue daemon.
 *
 * One client per pi session. Grants are per session lease, so a session's
 * consecutive requests keep the same slot (cache-warm append-only prefix).
 * Transport failure degrades fail-open: requests pass through unqueued.
 *
 * A token can produce several messages (an `acquire` answers `queued` first,
 * then `granted` later — sometimes in the same socket chunk), so handlers stay
 * registered until the exchange reaches a terminal state.
 */
import net from "node:net";
import { StringDecoder } from "node:string_decoder";
import { defaultSocketPath } from "./config.ts";

const MAX_LINE_BYTES = 64 * 1024;
const REQUEST_TIMEOUT_MS = 2000;

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

export interface Msg {
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

type Handler = (msg: Msg | null) => void;

export class LeaseClient {
	readonly socketPath: string;
	private socket: net.Socket | null = null;
	private buffer = "";
	private decoder = new StringDecoder("utf8");
	private seq = 0;
	private handlers = new Map<string, Handler>();
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
			const handlers = [...this.handlers.values()];
			this.handlers.clear();
			for (const handler of handlers) handler(null);
			this.options.onDisconnect?.();
			return;
		}
		if (msg.token && this.handlers.has(msg.token)) {
			this.handlers.get(msg.token)!(msg);
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
			const sock = net.connect(this.socketPath);
			const timer = setTimeout(() => {
				sock.destroy();
				settle(false);
			}, this.connectTimeoutMs);
			sock.once("connect", () => {
				this.socket = sock;
				this.buffer = "";
				this.decoder = new StringDecoder("utf8");
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
		this.buffer += this.decoder.write(chunk);
		let newline: number;
		while ((newline = this.buffer.indexOf("\n")) >= 0) {
			const line = this.buffer.slice(0, newline).trim();
			this.buffer = this.buffer.slice(newline + 1);
			if (Buffer.byteLength(line, "utf8") > MAX_LINE_BYTES) {
				this.socket?.destroy();
				return;
			}
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

	/** One request, one answer. Resolves null on transport failure or timeout. */
	private request(msg: Msg, timeoutMs: number = REQUEST_TIMEOUT_MS): Promise<Msg | null> {
		return new Promise((resolve) => {
			const token = msg.token ?? `t${++this.seq}`;
			let settled = false;
			const finish = (value: Msg | null) => {
				if (settled) return;
				settled = true;
				clearTimeout(timer);
				this.handlers.delete(token);
				resolve(value);
			};
			this.handlers.set(token, (incoming) => finish(incoming));
			const timer = setTimeout(() => finish(null), timeoutMs);
			if (!this.write({ ...msg, token })) finish(null);
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
		if (this.closed) return { granted: this.failOpen, failOpen: true };
		if (!(await this.ensureConnected())) return { granted: this.failOpen, failOpen: true };

		const token = `t${++this.seq}`;
		let settleAck: (msg: Msg | null) => void = () => undefined;
		let settleGrant: (grant: LeaseGrant) => void = () => undefined;
		const ack = new Promise<Msg | null>((resolve) => (settleAck = resolve));
		const grant = new Promise<LeaseGrant>((resolve) => (settleGrant = resolve));

		const finish = () => {
			this.handlers.delete(token);
			if (timer) clearTimeout(timer);
			args.signal?.removeEventListener("abort", onAbort);
		};
		const handler: Handler = (msg) => {
			if (!msg) {
				settleAck(null);
				settleGrant({ granted: this.failOpen, failOpen: true });
				finish();
				return;
			}
			if (msg.state === "queued") {
				settleAck(msg);
				return; // keep listening for the grant
			}
			if (msg.state === "granted") {
				settleAck(msg);
				settleGrant({ granted: true, leaseId: msg.leaseId as string | undefined, position: 0 });
				finish();
				return;
			}
			if (msg.state === "cancelled") {
				settleGrant({ granted: false, aborted: true });
				finish();
				return;
			}
			settleAck(msg);
			settleGrant({ granted: this.failOpen, failOpen: true });
			finish();
		};
		this.handlers.set(token, handler);

		const grantTimeoutMs = args.timeoutMs && args.timeoutMs > 0 ? args.timeoutMs : null;
		const timer = grantTimeoutMs
			? setTimeout(() => {
					this.write({ type: "cancel", token });
					settleAck({ state: "timeout" });
					settleGrant({ granted: this.failOpen, failOpen: true, timedOut: true });
					finish();
				}, grantTimeoutMs)
			: null;
		const onAbort = () => {
			this.write({ type: "cancel", token });
			settleGrant({ granted: false, aborted: true });
			finish();
		};
		if (args.signal) {
			if (args.signal.aborted) onAbort();
			else args.signal.addEventListener("abort", onAbort, { once: true });
		}

		if (!this.write({ type: "acquire", token, queueKey: args.queueKey, sessionKey: args.sessionKey, role: args.role, ...(args.group ? { group: args.group } : {}) })) {
			settleAck(null);
			settleGrant({ granted: this.failOpen, failOpen: true });
			finish();
		}

		const first = await Promise.race([ack, new Promise<Msg | null>((resolve) => setTimeout(() => resolve(null), REQUEST_TIMEOUT_MS))]);
		if (first?.state === "granted") return { granted: true, leaseId: first.leaseId as string | undefined, position: 0 };
		if (first?.state !== "queued") {
			settleGrant({ granted: this.failOpen, failOpen: true });
			finish();
			return { granted: this.failOpen, failOpen: true };
		}
		const position = typeof first.position === "number" ? first.position : 0;
		const result = await grant;
		return { ...result, position: result.position ?? position };
	}

	async renew(leaseId: string): Promise<boolean> {
		if (this.closed || !this.socket) return false;
		const resp = await this.request({ type: "renew", leaseId });
		return !!resp && resp.ok === true && resp.state === "renewed";
	}

	async release(leaseId: string): Promise<void> {
		if (this.closed || !this.socket) return;
		await this.request({ type: "release", leaseId });
	}

	async status(): Promise<QueueStatus[] | null> {
		if (this.closed) return null;
		if (!(await this.ensureConnected())) return null;
		const resp = await this.request({ type: "status" });
		if (!resp || resp.ok === false || !Array.isArray(resp.queues)) return null;
		return resp.queues as QueueStatus[];
	}

	/** Ask the daemon to re-read its config file (hot-reload). */
	async reload(): Promise<boolean> {
		if (this.closed || !this.socket) return false;
		const resp = await this.request({ type: "reload" });
		return !!resp && resp.ok === true && resp.state === "reloaded";
	}

	async ping(): Promise<{ pid: number; uptimeMs: number } | null> {
		if (this.closed) return null;
		if (!(await this.ensureConnected())) return null;
		const resp = await this.request({ type: "ping" });
		if (!resp || resp.ok === false) return null;
		return { pid: Number(resp.pid), uptimeMs: Number(resp.uptimeMs) };
	}

	close(): void {
		this.closed = true;
		this.socket?.destroy();
		this.socket = null;
	}
}
