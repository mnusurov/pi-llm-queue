// Run lifecycle unit seam (#92): one module owns Run state, lease ownership, Grace/Beat timers,
// and the parked Blocking Tool set. Fake LeasePort + fake Clock: no daemon, no sockets, no sleep.
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { RunLifecycle, SessionRegistry, type Clock, type LeasePort, type Timer } from "../src/run-lifecycle.ts";
import type { AcquireVerdict, BeatVerdict } from "../src/lease-verdict.ts";

const GRACE_MS = 3000;
const BEAT_MS = 100;

/** Deterministic clock: advance() fires due timers in order and flushes promise continuations. */
class FakeClock implements Clock {
	private at = 0;
	private seq = 0;
	private readonly timers = new Map<number, { at: number; fn: () => void }>();

	now(): number {
		return this.at;
	}

	setTimeout(fn: () => void, ms: number): Timer {
		const id = ++this.seq;
		this.timers.set(id, { at: this.at + ms, fn });
		return id;
	}

	clearTimeout(timer: Timer): void {
		this.timers.delete(timer as number);
	}

	/** Move time forward, firing every timer due on the way; flush the microtasks each one queues. */
	async advance(ms: number): Promise<void> {
		const target = this.at + ms;
		for (;;) {
			const due = [...this.timers.entries()].filter(([, timer]) => timer.at <= target).sort((a, b) => a[1].at - b[1].at)[0];
			if (!due) break;
			const [id, timer] = due;
			this.timers.delete(id);
			this.at = timer.at;
			timer.fn();
			await flush();
		}
		this.at = target;
		await flush();
	}
}

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

class FakeLease implements LeasePort {
	readonly acquires: string[] = [];
	readonly releases: string[] = [];
	readonly beats: string[] = [];
	readonly held = new Set<string>();
	/** Answers the next keyed beat lost, as a daemon that yielded the slot does. */
	lost = false;
	/** Answers every keyed beat passthrough, as an unreachable daemon does. */
	passthrough = false;
	private deferred: { queueKey: string; resolve: (verdict: AcquireVerdict) => void } | null = null;

	deferNextAcquire(): void {
		this.manual = true;
	}
	private manual = false;

	grantDeferred(): void {
		const pending = this.deferred;
		this.deferred = null;
		this.manual = false;
		if (!pending) throw new Error("no deferred acquire to grant");
		this.held.add(pending.queueKey);
		pending.resolve({ kind: "held" });
	}

	acquire(queueKey: string, _signal?: AbortSignal): Promise<AcquireVerdict> {
		this.acquires.push(queueKey);
		if (this.manual) return new Promise((resolve) => (this.deferred = { queueKey, resolve }));
		this.held.add(queueKey);
		return Promise.resolve({ kind: "held" });
	}

	release(queueKey: string): void {
		this.releases.push(queueKey);
		this.held.delete(queueKey);
	}

	beat(queueKey: string): Promise<BeatVerdict> {
		this.beats.push(queueKey);
		if (this.lost) {
			this.held.delete(queueKey);
			return Promise.resolve({ kind: "lost" });
		}
		return Promise.resolve({ kind: this.passthrough ? "passthrough" : "held" });
	}

	// The refresh period the port hands over (ADR 0009): the period the Lease's terms ask for.
	beatPeriodMs(): number {
		return BEAT_MS;
	}
}

const noopUi = { status() {}, notify() {} };

const make = (sessionKey = "S", lease = new FakeLease(), clock = new FakeClock()) => ({
	lease,
	clock,
	lifecycle: new RunLifecycle({
		sessionKey,
		lease,
		clock,
		ui: noopUi,
		blockingTools: () => ["subagent", "ask_user"],
	}),
});

describe("RunLifecycle: Grace", () => {
	it("keeps the lease through 2999 ms of Grace and releases it at 3000 ms", async () => {
		const { lease, clock, lifecycle } = make();
		await lifecycle.startRun("q");
		lifecycle.settleRun();

		await clock.advance(GRACE_MS - 1);
		assert.deepEqual(lease.releases, [], "the slot is still held one millisecond before the boundary");
		assert.equal(lifecycle.holds("q"), true);

		await clock.advance(1);
		assert.deepEqual(lease.releases, ["q"], "the elapsed Grace releases the lease exactly at 3000 ms");
		assert.equal(lifecycle.holds("q"), false);
	});

	it("a Run starting inside Grace cancels the pending release and keeps the lease", async () => {
		const { lease, clock, lifecycle } = make();
		await lifecycle.startRun("q");
		lifecycle.settleRun();
		await clock.advance(GRACE_MS - 1000);

		await lifecycle.startRun("q");
		await clock.advance(GRACE_MS * 2);

		assert.deepEqual(lease.releases, [], "the new Run cancelled the pending release");
		assert.equal(lifecycle.holds("q"), true);
	});

	it("a lone stream inside Grace re-arms Grace when it finishes", async () => {
		const { lease, clock, lifecycle } = make();
		await lifecycle.startRun("q");
		lifecycle.settleRun();
		await clock.advance(2000);

		await lifecycle.withSlot("q", undefined, async () => "stream");
		await clock.advance(GRACE_MS - 1);
		assert.deepEqual(lease.releases, [], "the lone stream restarted the Grace window");

		await clock.advance(1);
		assert.deepEqual(lease.releases, ["q"], "the re-armed Grace releases after its own 3000 ms");
	});

	it("a stream during an active Run does not arm Grace", async () => {
		const { lease, clock, lifecycle } = make();
		await lifecycle.startRun("q");

		await lifecycle.withSlot("q", undefined, async () => "stream");
		await clock.advance(GRACE_MS * 2);

		assert.deepEqual(lease.releases, [], "only agent_settled arms Grace");
		assert.equal(lifecycle.holds("q"), true);
	});
});

describe("RunLifecycle: acquire ownership", () => {
	it("a stream joins the Run's in-flight acquire instead of queuing twice (#55)", async () => {
		const { lease, lifecycle } = make();
		lease.deferNextAcquire();

		const run = lifecycle.startRun("q");
		const stream = lifecycle.withSlot("q", undefined, async () => "stream");
		await flush();
		assert.deepEqual(lease.acquires, ["q"], "one acquire serves both the Run and its first stream");

		lease.grantDeferred();
		await Promise.all([run, stream]);

		assert.equal(lifecycle.holds("q"), true, "the shared Grant leaves one run-owned lease");
	});

	it("a Run switching Queues releases the old lease and stops its Beat", async () => {
		const { lease, clock, lifecycle } = make();
		await lifecycle.startRun("q1");
		await lifecycle.startRun("q2");

		assert.deepEqual(lease.releases, ["q1"], "the old lease is released before the new one is claimed");
		assert.equal(lifecycle.holds("q1"), false);
		assert.equal(lifecycle.holds("q2"), true);

		const before = lease.beats.length;
		await clock.advance(BEAT_MS * 3);
		assert.ok(lease.beats.slice(before).every((key) => key === "q2"), "no Beat survives for the old Queue");
	});

	it("a request on a second Queue does not replace the Run's lease on the first", async () => {
		const { lease, lifecycle } = make();
		await lifecycle.startRun("q1");

		await lifecycle.withSlot("q2", undefined, async () => "stream");

		assert.equal(lifecycle.holds("q1"), true, "the Run keeps its first Queue");
		assert.equal(lifecycle.holds("q2"), false, "the request released its own Queue when the stream ended");
		assert.deepEqual(lease.releases, ["q2"]);
	});
});

describe("RunLifecycle: Beat gate", () => {
	it("parkOnTool pauses the Beat and leaveTool resumes it", async () => {
		const { lease, clock, lifecycle } = make();
		await lifecycle.startRun("q");

		lifecycle.parkOnTool("call-1", "subagent");
		const parked = lease.beats.length;
		await clock.advance(BEAT_MS * 3);
		assert.equal(lease.beats.length, parked, "a parked Run sends no keyed Beat");

		lifecycle.leaveTool("call-1");
		await clock.advance(BEAT_MS);
		assert.ok(lease.beats.length > parked, "leaving the Blocking Tool lets the Beat resume");
	});

	it("ignores a Compute Tool: only blockingTools parks the Run", async () => {
		const { lease, clock, lifecycle } = make();
		await lifecycle.startRun("q");

		lifecycle.parkOnTool("call-1", "bash");
		await clock.advance(BEAT_MS);
		assert.ok(lease.beats.length > 0, "a compute-busy Run keeps beating");
	});

	it("clears a lost tool_execution_end at release so the next Run still beats", async () => {
		const { lease, clock, lifecycle } = make();
		await lifecycle.startRun("q");
		lifecycle.parkOnTool("orphan", "ask_user"); // its end event never arrives
		lifecycle.settleRun();
		await clock.advance(GRACE_MS);

		await lifecycle.startRun("q");
		const before = lease.beats.length;
		await clock.advance(BEAT_MS);
		assert.ok(lease.beats.length > before, "the stale parked id did not silence the new Run");
	});

	it("a passthrough beat streams unqueued without re-queuing", async () => {
		const { lease, lifecycle } = make();
		await lifecycle.startRun("q");
		lease.passthrough = true;

		const acquires = lease.acquires.length;
		await lifecycle.withSlot("q", undefined, async () => "stream");
		assert.equal(lease.acquires.length, acquires, "passthrough keeps the Run's path: no re-queue");
		assert.equal(lifecycle.holds("q"), true, "the Run's lease is unchanged by a passthrough beat");
	});

	it("a beat answered lost stops the timer; the next stream re-queues", async () => {
		const { lease, clock, lifecycle } = make();
		await lifecycle.startRun("q");
		lease.lost = true;

		await clock.advance(BEAT_MS * 2);
		const afterLapse = lease.beats.length;
		await clock.advance(BEAT_MS * 3);
		assert.equal(lease.beats.length, afterLapse, "a lost Slot stops the Beat");

		// The lost Slot keeps answering the stream-start beat false, so the stream re-queues.
		const acquires = lease.acquires.length;
		await lifecycle.withSlot("q", undefined, async () => "stream");
		assert.equal(lease.acquires.length, acquires + 1, "the next stream re-queues before streaming");
	});
});

describe("SessionRegistry", () => {
	const makeRegistry = (leases: FakeLease[] = []) => {
		let index = 0;
		return new SessionRegistry((sessionKey) => {
			const lease = leases[index++] ?? new FakeLease();
			return new RunLifecycle({ sessionKey, lease, clock: new FakeClock(), ui: noopUi, blockingTools: () => [] });
		});
	};

	it("ownerFor attributes a sessionless request to the Holder, not the newest session", async () => {
		const registry = makeRegistry();
		const a = registry.for("A");
		registry.for("B");
		await a.startRun("q");

		assert.equal(registry.ownerFor("q"), "A", "the Holder answers for its Queue");
		assert.equal(registry.ownerFor("other"), "B", "with no Holder, the newest session answers");
	});

	it("forget returns wasLast only for the final session", () => {
		const registry = makeRegistry();
		const a = registry.for("A");
		const b = registry.for("B");

		assert.equal(registry.forget("A"), false, "one session remains");
		assert.equal(registry.forget("B"), true, "the last session reports wasLast");
		assert.notEqual(registry.for("A"), a, "a forgotten key gets a fresh lifecycle");
		assert.notEqual(registry.for("B"), b);
	});
});
