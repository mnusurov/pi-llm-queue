// The second adapter (ADR 0004, spec #65 phase C): a synchronous in-memory driver for the
// Scheduler. It is a test driver, not a production adapter — it registers fake connections as plain
// ids, calls Scheduler entries directly with an explicit `now` the test controls, and captures every
// decision an entry returns into a per-connection list the test reads. No socket, no sleep, no
// sweep timer, no `--sweep-ms`: the clock is a number and the sweep is a call.
//
// Policy is passed in as plain resolved values (the production adapter's job through config.js, the
// driver's job here): the constructor takes a Policy object or a per-queueKey resolver function.
import { Scheduler } from "../src/scheduler.js";
import { DEFAULTS as POLICY_DEFAULTS } from "../src/config.js";

export const QUEUE_KEY = "http://model.test/v1";
export const QUEUE_KEY2 = "http://model.test/v2";
// The pseudo-connection a sweep's connId-less decisions (a TTL evict, an idle release) route to, so
// capture() has one routing rule for every entry (the sweep has no triggering actor connection).
const SYSTEM = "__system__";

export interface Policy {
	maxStreams?: number;
	ttlMs?: number;
	agingAfterSlots?: number;
	agingAfterMs?: number;
	policy?: "priority-aging" | "fifo";
	idleYieldMs?: number;
}

export interface Decision {
	type: "grant" | "switch" | "queued" | "release" | "evict";
	connId?: string;
	queueKey: string;
	sessionKey: string;
	tier: string;
	requestedKey?: string;
	reason?: string;
}

// The daemon's resolved defaults, read from the config.js table (#77) so the constants live in one
// place; `leaseTtlMs` is the config field name, `ttlMs` the Scheduler's resolved one. A driver with
// no policy overrides behaves like a daemon serving defaults.
const DEFAULTS: Required<Policy> = {
	maxStreams: POLICY_DEFAULTS.maxStreams,
	ttlMs: POLICY_DEFAULTS.leaseTtlMs,
	agingAfterSlots: POLICY_DEFAULTS.agingAfterSlots,
	agingAfterMs: POLICY_DEFAULTS.agingAfterMs,
	policy: POLICY_DEFAULTS.policy,
	idleYieldMs: POLICY_DEFAULTS.idleYieldMs,
};

export class SchedulerHarness {
	readonly scheduler: Scheduler;
	// The controlled clock. Every entry is called at this instant; the test moves it explicitly.
	now: number;
	private connSeq = 0;
	// connId -> decisions routed to that connection (grants/queued carry the connId; release/evict/
	// switch carry none and are routed to the actor that triggered the entry, or to SYSTEM on a sweep).
	private byConn = new Map<string, Decision[]>();
	// The live policy. A config edit swaps it, and the live Queues re-resolve it (the hot-reload).
	private policy: Policy | ((queueKey: string) => Policy);

	constructor(policy: Policy | ((queueKey: string) => Policy) = {}) {
		this.policy = policy;
		this.now = 0;
		this.scheduler = new Scheduler((queueKey: string) => ({ ...DEFAULTS, ...this.resolve(queueKey) }));
	}

	// A config edit, in memory: the daemon's hot-reload rebuild, in miniature. Every live Queue is
	// rebuilt through the new policy and its holders and waiters are carried over, so a Holder keeps
	// the terms it was granted under and only the Queue's own terms change (#104). The group remap
	// and the over-capacity log are the daemon's alone — the driver has no groups (#103 moves the
	// rebuild into the Scheduler).
	editPolicy(policy: Policy | ((queueKey: string) => Policy)): void {
		this.policy = policy;
		const rebuilt = new Map<string, ReturnType<Scheduler["newQueue"]>>();
		for (const [key, queue] of this.scheduler.queues) {
			let moved = rebuilt.get(key);
			if (!moved) rebuilt.set(key, (moved = this.scheduler.newQueue(key)));
			moved.switches += queue.switches;
			for (const lease of queue.holders) {
				lease.queueKey = key;
				this.scheduler.redateLease(moved, lease, this.now);
				lease.lastActivityAt = this.now;
				moved.holders.push(lease);
			}
			for (const waiter of queue.waiting) moved.waiting.push(waiter);
		}
		this.scheduler.queues.clear();
		for (const [key, queue] of rebuilt) this.scheduler.queues.set(key, queue);
		// The rebuild grants whoever the new capacity frees, as the daemon does after its own rebuild.
		const decisions: Decision[] = [];
		for (const queue of this.scheduler.queues.values()) this.scheduler.grantNext(queue, this.now, decisions);
		this.capture(SYSTEM, decisions);
	}

	private resolve(queueKey: string): Policy {
		return typeof this.policy === "function" ? this.policy(queueKey) : this.policy;
	}

	// Register a fake connection; returns its plain id ("c1", "c2", ...).
	connect(): string {
		const connId = `c${++this.connSeq}`;
		this.byConn.set(connId, []);
		return connId;
	}

	// Move the controlled clock.
	setNow(now: number): void {
		this.now = now;
	}
	advance(ms: number): void {
		this.now += ms;
	}

	acquire(connId: string, sessionKey: string, opts: { role?: string; group?: string; activity?: boolean; queueKey?: string; requestedKey?: string } = {}) {
		const queueKey = opts.queueKey ?? QUEUE_KEY;
		const requestedKey = opts.requestedKey ?? queueKey;
		const result = this.scheduler.acquire(connId, sessionKey, opts.role, opts.group, opts.activity, queueKey, requestedKey, this.now);
		this.capture(connId, result.decisions);
		return result;
	}

	beat(connId: string, sessionKey: string, queueKey = QUEUE_KEY) {
		return this.scheduler.beat(connId, sessionKey, queueKey, this.now);
	}

	heartbeat(connId: string): Decision[] {
		const decisions = this.scheduler.heartbeat(connId, this.now);
		this.capture(connId, decisions);
		return decisions;
	}

	release(connId: string, sessionKey: string, queueKey = QUEUE_KEY): Decision[] {
		const decisions = this.scheduler.release(connId, sessionKey, queueKey, this.now);
		this.capture(connId, decisions);
		return decisions;
	}

	cancel(connId: string, sessionKey: string, queueKey = QUEUE_KEY): Decision[] {
		const decisions = this.scheduler.cancel(connId, sessionKey, queueKey, this.now);
		this.capture(connId, decisions);
		return decisions;
	}

	retier(connId: string, sessionKey: string, tier: string): void {
		this.scheduler.retier(connId, sessionKey, tier);
	}

	disconnect(connId: string): Decision[] {
		const decisions = this.scheduler.disconnect(connId, this.now);
		this.capture(connId, decisions);
		return decisions;
	}

	// The sweep, run at an explicit instant. Returns every decision it produced; grants/queued are
	// routed to their connId, evict/release (no connId) land in the SYSTEM bucket via capture.
	sweepAt(now = this.now): Decision[] {
		this.now = now;
		const decisions = this.scheduler.sweepAt(now);
		this.capture(SYSTEM, decisions);
		return decisions;
	}

	snapshot() {
		return this.scheduler.snapshot();
	}

	// Reads (non-destructive): the test inspects captured state at any point.
	decisionsFor(connId: string): Decision[] {
		return [...(this.byConn.get(connId) ?? [])];
	}
	grantsFor(connId: string): Decision[] {
		return this.decisionsFor(connId).filter((d) => d.type === "grant");
	}
	grantedSessions(connId: string): string[] {
		return this.grantsFor(connId).map((d) => d.sessionKey);
	}
	// The per-queue switch counter, read from the snapshot (the same source `status` reports).
	switches(queueKey = QUEUE_KEY): number {
		return this.snapshot().find((q) => q.queueKey === queueKey)?.switches ?? 0;
	}
	holders(queueKey = QUEUE_KEY): string[] {
		return (this.snapshot().find((q) => q.queueKey === queueKey)?.holders ?? []).map((h: { sessionKey: string }) => h.sessionKey);
	}
	evicts(): Decision[] {
		return this.decisionsFor(SYSTEM).filter((d) => d.type === "evict");
	}

	private capture(actor: string, decisions: Decision[]): void {
		for (const d of decisions) {
			if (d.connId) this.push(d.connId, d);
			else this.push(actor, d);
		}
	}

	private push(connId: string, d: Decision): void {
		let list = this.byConn.get(connId);
		if (!list) this.byConn.set(connId, (list = []));
		list.push(d);
	}
}
