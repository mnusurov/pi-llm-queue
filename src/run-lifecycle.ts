// The Run lifecycle, owned in one place (#92): extension.ts keeps pi wiring, and one RunLifecycle
// per session holds the ordering rules. No pi, addressing, daemon, socket, or LeaseClient import:
// the ports are injected, so the rules are testable without a daemon (ADR 0007).

// Type-only: the shared verdict, not the LeaseClient (ADR 0007).
import type { AcquireVerdict, BeatVerdict } from "./lease-verdict.ts";

/** Opaque handle from Clock; the production adapter returns Node's Timeout object. */
export type Timer = unknown;

/** Time as a port: unit tests move it exactly; the Extension adapter uses Node's timers. */
export interface Clock {
	now(): number;
	setTimeout(fn: () => void, ms: number): Timer;
	clearTimeout(timer: Timer): void;
}

/** A per-session view of the lease client: the session key is already bound on the other side. */
export interface LeasePort {
	acquire(
		queueKey: string,
		signal?: AbortSignal,
		handlers?: {
			onQueued(info: { position: number; holders: string[] }): void;
			onWaitCap(): void;
		},
	): Promise<AcquireVerdict>;
	release(queueKey: string): void;
	beat(queueKey: string): Promise<BeatVerdict>;
}

/** The footer indicator and user-visible warnings; the Extension maps it onto pi's ui. */
export interface UiPort {
	status(text: string | undefined): void;
	notify(message: string, level: "info" | "warning" | "error"): void;
}

export interface RunLifecycleOptions {
	sessionKey: string;
	lease: LeasePort;
	clock: Clock;
	ui: UiPort;
	/** Keyed Beat period for one Queue, resolved from that Queue's own policy (#81). */
	beatMs(queueKey: string): number;
	/** Live Blocking Tool list: read on every tool start, so a hot-reload applies at once. */
	blockingTools(): readonly string[];
}

/** Wake chains (parent woken by a child) must not thrash the slot: spec says 2-5 s. */
const GRACE_MS = 3000;
// Shown when a parent's queue wait exceeds queueTimeoutMs; the request stays queued.
const TIMEOUT_WARNING = "pi-llm-queue: queue wait exceeded queueTimeoutMs — still waiting";

type RunState = "idle" | "queued" | "held" | "grace";
type LeaseOwner = "run" | "request";

interface LeaseState {
	owner: LeaseOwner;
	beat: Timer | null;
}

interface QueueWait {
	position: number;
	holders: string[];
}

/** What a waiting session shows in the footer: its queue position and the holders ahead of it. */
type QueuedInfo = QueueWait & { startedAt: number };

/**
 * One session's Run lifecycle. Verbs, not fields: startRun/settleRun own the Run, withSlot owns a
 * request's Grant handover, parkOnTool/leaveTool own the Beat gate, shutdown owns the teardown.
 */
export class RunLifecycle {
	readonly sessionKey: string;
	private readonly lease: LeasePort;
	private readonly clock: Clock;
	private ui: UiPort;
	private readonly beatMs: (queueKey: string) => number;
	private readonly blockingTools: () => readonly string[];

	private state: RunState = "idle";
	/** A session may hold more than one Queue at once: the Run's and a lone request's. */
	private readonly leases = new Map<string, LeaseState>();
	/** Orthogonal to `state`: a Run can be held or in Grace while a Blocking Tool is in flight. */
	private readonly parked = new Set<string>();
	private readonly pendingAcquire = new Map<string, Promise<AcquireVerdict>>();

	private releaseTimer: Timer | null = null;
	private queuedInfo: QueuedInfo | null = null;
	private indicatorTimer: Timer | null = null;

	constructor(options: RunLifecycleOptions) {
		this.sessionKey = options.sessionKey;
		this.lease = options.lease;
		this.clock = options.clock;
		this.ui = options.ui;
		this.beatMs = options.beatMs;
		this.blockingTools = options.blockingTools;
	}

	setUi(ui: UiPort): void {
		this.ui = ui;
	}

	/**
	 * A Run is starting at `agent_start`. Cancels a pending Grace release and, when the Run moves
	 * to another Queue, releases the old lease before claiming the new one (#81). A null queueKey
	 * is a Run on an unqueued provider: it still ends the old lease and Grace, but claims nothing.
	 */
	async startRun(queueKey: string | null, signal?: AbortSignal): Promise<void> {
		this.cancelPendingRelease();
		const running = this.runQueueKey();
		if (running !== null && running !== queueKey) this.release();
		if (!queueKey) return;

		this.state = "queued";
		// Attach the abort path before awaiting: Esc while queued must cancel the Run's lease.
		signal?.addEventListener("abort", () => this.release(), { once: true });

		const verdict = await this.acquire(queueKey, signal);
		this.clearQueued();
		// Only a held lease claims the Slot: cancelled and passthrough must not mark it held.
		if (verdict.kind !== "held") {
			this.state = "idle";
			return;
		}
		this.claim(queueKey, "run");
		this.state = "held";
		this.startBeat(queueKey);
	}

	/** The Run ended: a new Run inside the Grace window keeps the Slot without a Switch. */
	settleRun(): void {
		this.armGraceRelease();
	}

	/**
	 * Run one request under the slot. Probes the Run's lease when this Queue is already held,
	 * joins the Run's in-flight acquire when one exists (#55), and owns the Grant handover: a
	 * request-owned lease is released when the body ends unless a Run claimed it meanwhile.
	 * A lone request that resumes the session inside Grace re-arms it.
	 */
	async withSlot<T>(queueKey: string, signal: AbortSignal | undefined, body: () => Promise<T>): Promise<T> {
		// Run-owned only: another request's lease is not the Run's to retain or re-arm Grace for.
		const wasHeld = this.leases.get(queueKey)?.owner === "run";
		const inGrace = this.state === "grace";
		this.cancelPendingRelease();

		let selfQueued = false;
		try {
			// A held lease beats: the Run keeps the Slot across tool executions. A lost beat means
			// the daemon no longer grants it — re-queue before streaming; passthrough still streams.
			const beat = wasHeld ? await this.lease.beat(queueKey) : null;
			const retained = beat !== null && beat.kind !== "lost";
			if (!retained) {
				const verdict = await this.acquire(queueKey, signal);
				this.clearQueued();
				if (verdict.kind === "held") {
					const runOwns = wasHeld || this.leases.get(queueKey)?.owner === "run";
					if (runOwns) this.claim(queueKey, "run"); // the Run re-took, or won, the Slot
					else {
						this.claim(queueKey, "request");
						selfQueued = true;
					}
					this.startBeat(queueKey); // the run-scoped Beat covers this grant too
				}
			}
			return await body();
		} finally {
			if (selfQueued && this.leases.get(queueKey)?.owner === "request") {
				this.releaseLease(queueKey); // stop at release: the Beat must not outlive its lease
			} else if (inGrace && this.state !== "held" && this.state !== "queued") {
				// No Run is coming to re-arm: this lone request resumed the session, so restart Grace.
				this.armGraceRelease();
			}
		}
	}

	/** A Blocking Tool started: pause the run-scoped Beat so idle-yield can hand the Slot over. */
	parkOnTool(toolCallId: string, toolName: string): void {
		if (this.blockingTools().includes(toolName)) this.parked.add(toolCallId);
	}

	leaveTool(toolCallId: string): void {
		this.parked.delete(toolCallId);
	}

	/** Release everything this session holds and clear its footer indicator. */
	shutdown(): void {
		this.release();
		this.clearQueued();
	}

	holds(queueKey: string): boolean {
		return this.leases.has(queueKey);
	}

	/**
	 * Re-arm every live Beat at the current config's period (hot-reload, #81). Beat immediately
	 * first: a shortened idle window applies at the daemon at once, and waiting a full new period
	 * would let a sweep yield the holder before its first beat under the new policy.
	 */
	restartBeats(): void {
		for (const queueKey of [...this.leases.keys()]) {
			this.startBeat(queueKey);
			this.beatNow(queueKey);
		}
	}

	/** The Beat gate: beats only for a held lease, and silence while parked on an agent/user. */
	private shouldBeat(queueKey: string): boolean {
		return this.holds(queueKey) && this.parked.size === 0;
	}

	private beatNow(queueKey: string): void {
		if (!this.shouldBeat(queueKey)) return;
		this.lease.beat(queueKey).then(
			(verdict) => {
				if (verdict.kind === "lost") this.stopBeat(queueKey); // the Slot is gone: the next stream re-queues
			},
			() => {},
		);
	}

	private startBeat(queueKey: string): void {
		this.stopBeat(queueKey);
		if (!this.holds(queueKey)) return;
		const period = this.beatMs(queueKey);
		const arm = (): void => {
			const lease = this.leases.get(queueKey);
			if (!lease) return;
			lease.beat = this.clock.setTimeout(() => {
				const live = this.leases.get(queueKey);
				if (!live) return;
				live.beat = null;
				this.beatNow(queueKey);
				arm();
			}, period);
		};
		arm();
	}

	private stopBeat(queueKey: string): void {
		const lease = this.leases.get(queueKey);
		if (!lease?.beat) return;
		this.clock.clearTimeout(lease.beat);
		lease.beat = null;
	}

	private claim(queueKey: string, owner: LeaseOwner): void {
		const lease = this.leases.get(queueKey);
		if (lease) lease.owner = owner;
		else this.leases.set(queueKey, { owner, beat: null });
	}

	private releaseLease(queueKey: string): void {
		this.stopBeat(queueKey);
		this.leases.delete(queueKey);
		this.lease.release(queueKey);
	}

	private runQueueKey(): string | null {
		for (const [queueKey, lease] of this.leases) if (lease.owner === "run") return queueKey;
		return null;
	}

	/** Acquire this Queue once: a concurrent call for the same Queue shares the request (#55). */
	private acquire(queueKey: string, signal?: AbortSignal): Promise<AcquireVerdict> {
		const pending = this.pendingAcquire.get(queueKey);
		if (pending) return pending;

		const promise = this.lease.acquire(queueKey, signal, {
			onQueued: (info) => this.showQueued(info),
			onWaitCap: () => this.ui.notify(TIMEOUT_WARNING, "warning"),
		});
		this.pendingAcquire.set(queueKey, promise);
		promise
			.finally(() => {
				if (this.pendingAcquire.get(queueKey) === promise) this.pendingAcquire.delete(queueKey);
			})
			.catch(() => {}); // the awaiters see the rejection; this tap only clears state
		return promise;
	}

	private cancelPendingRelease(): void {
		if (!this.releaseTimer) return;
		this.clock.clearTimeout(this.releaseTimer);
		this.releaseTimer = null;
	}

	/** Release the slot unless a Run claims it within the Grace window. */
	private armGraceRelease(): void {
		this.cancelPendingRelease();
		this.state = "grace";
		this.releaseTimer = this.clock.setTimeout(() => {
			this.releaseTimer = null;
			this.release();
		}, GRACE_MS);
	}

	/** Every lease this session still holds, request-owned included (#81), plus the parked set. */
	private release(): void {
		this.cancelPendingRelease();
		for (const [queueKey, lease] of [...this.leases]) {
			this.stopBeat(queueKey);
			lease.beat = null;
			this.lease.release(queueKey);
		}
		this.leases.clear();
		this.parked.clear(); // a lost tool_execution_end must not silence the Beat of later Runs
		this.state = "idle";
	}

	private showQueued(info: QueueWait): void {
		this.queuedInfo = { ...info, startedAt: this.queuedInfo?.startedAt ?? this.clock.now() };
		if (!this.indicatorTimer) this.armIndicator();
		this.render();
	}

	private armIndicator(): void {
		this.indicatorTimer = this.clock.setTimeout(() => {
			this.indicatorTimer = null;
			if (!this.queuedInfo) return;
			this.render();
			this.armIndicator();
		}, 1000);
	}

	private clearQueued(): void {
		if (this.indicatorTimer) {
			this.clock.clearTimeout(this.indicatorTimer);
			this.indicatorTimer = null;
		}
		if (this.queuedInfo) {
			this.queuedInfo = null;
			this.render();
		}
	}

	private render(): void {
		this.ui.status(
			this.queuedInfo
				? `llm-queue #${this.queuedInfo.position} · ${Math.round((this.clock.now() - this.queuedInfo.startedAt) / 1000)}s · behind ${this.queuedInfo.holders.join(", ") || "?"}`
				: undefined,
		);
	}
}

/**
 * The pi process's sessions. `ownerFor` replaces the old held-key scan: a request without a
 * session id belongs to whichever session holds that Queue's Slot, or the newest session
 * otherwise. `forget` reports whether the removed session was the last one (caller says bye).
 */
export class SessionRegistry {
	private readonly sessions = new Map<string, RunLifecycle>();
	private readonly create: (sessionKey: string) => RunLifecycle;
	private newest = "";

	constructor(create: (sessionKey: string) => RunLifecycle) {
		this.create = create;
	}

	for(sessionKey: string): RunLifecycle {
		let lifecycle = this.sessions.get(sessionKey);
		if (!lifecycle) {
			lifecycle = this.create(sessionKey);
			this.sessions.set(sessionKey, lifecycle);
			this.newest = sessionKey;
		}
		return lifecycle;
	}

	ownerFor(queueKey: string): string {
		for (const lifecycle of this.sessions.values()) if (lifecycle.holds(queueKey)) return lifecycle.sessionKey;
		return this.newest;
	}

	forget(sessionKey: string): boolean {
		this.sessions.delete(sessionKey);
		return this.sessions.size === 0;
	}

	/** Hot-reload: every live Beat follows the edited policy (#81). */
	restartBeats(): void {
		for (const lifecycle of this.sessions.values()) lifecycle.restartBeats();
	}
}
