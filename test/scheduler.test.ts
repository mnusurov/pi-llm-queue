// The Scheduler driven in memory (spec #65 phase C): the Lease/TTL and idle-Yield families, migrated
// off the socket seam. The clock is a number the test moves, the sweep is a call, and every decision
// is read from the driver's capture — no sleep, no --sweep-ms, no `no message from daemon within`.
// The socket suite keeps one smoke test per family (daemon.test.ts).
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { QUEUE_KEY, QUEUE_KEY2, SchedulerHarness } from "./scheduler-harness.ts";

describe("lease / TTL (in memory)", () => {
	it("keeps a lease alive while its client heartbeats, and reclaims it when heartbeats stop", () => {
		const h = new SchedulerHarness({ ttlMs: 1000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent" });
		h.acquire(b, "B", { role: "parent" });

		// 12 bare heartbeats ~ every 100 ms: A's TTL keeps refreshing, B is never granted.
		for (let i = 0; i < 12; i++) {
			h.advance(100);
			h.heartbeat(a);
		}
		assert.deepEqual(h.grantsFor(b), [], "heartbeats keep A's slot: B stays queued");

		// Heartbeats stop: A's last refresh was at now=1200 (expires 2200); advance past it and sweep.
		h.advance(1200); // now=2400 > 2200
		h.sweepAt();
		assert.deepEqual(h.grantedSessions(b), ["B"], "the TTL reclaims A, B is granted");
	});

	it("answers a keyed beat with the authoritative grant state", () => {
		const h = new SchedulerHarness();
		const a = h.connect();

		h.acquire(a, "A", { role: "parent" });
		assert.equal(h.beat(a, "A").granted, true, "a beat on a held slot is granted");

		h.release(a, "A");
		assert.equal(h.beat(a, "A").granted, false, "a beat on a lost slot tells the client to re-queue");
	});

	it("releases every lease of a client that disconnects (no phantom holder)", () => {
		const h = new SchedulerHarness();
		const multi = h.connect();
		const w1 = h.connect();
		const w2 = h.connect();

		h.acquire(multi, "M1", { role: "parent" });
		h.acquire(multi, "M2", { role: "parent", queueKey: QUEUE_KEY2 });
		h.acquire(w1, "W1", { role: "parent" });
		h.acquire(w2, "W2", { role: "parent", queueKey: QUEUE_KEY2 });

		h.disconnect(multi); // one close frees BOTH slots
		assert.deepEqual(h.grantedSessions(w1), ["W1"]);
		assert.deepEqual(h.grantedSessions(w2), ["W2"]);
	});

	it("a holder that dies mid-stream is reclaimed after the TTL grace and grants the next waiter", () => {
		const h = new SchedulerHarness({ ttlMs: 1000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent" });
		h.beat(a, "A"); // in flight, never completed: refreshes expires to 1000
		h.acquire(b, "B", { role: "parent" });
		assert.deepEqual(h.grantsFor(b), [], "B waits behind the in-flight lease");

		h.setNow(1001); // the TTL is the in-flight grace: a silent client loses the slot
		h.sweepAt();
		assert.deepEqual(h.grantedSessions(b), ["B"]);
	});

	it("sweeps an expired lease with nobody waiting, so status never shows a ghost holder (exact TTL boundary)", () => {
		const h = new SchedulerHarness({ ttlMs: 1000 });
		const a = h.connect();

		h.acquire(a, "A", { role: "parent" }); // granted at now=0, expires at 1000

		h.sweepAt(999);
		assert.deepEqual(h.holders(QUEUE_KEY), ["A"], "999 < 1000: the lease is not yet lapsed");
		h.sweepAt(1000);
		assert.deepEqual(h.holders(QUEUE_KEY), ["A"], "the sweep evicts strictly after expiry (now > expires)");
		h.sweepAt(1001);
		assert.deepEqual(h.holders(QUEUE_KEY), [], "1001 > 1000: the unconditional TTL sweep clears the ghost holder");
	});
});

describe("idle yield (in memory)", () => {
	it("bare heartbeats keep the TTL alive but not the slot: idle-yield fires (#57 guard)", () => {
		const h = new SchedulerHarness({ idleYieldMs: 300, ttlMs: 30000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent", activity: true });
		h.acquire(b, "B", { role: "parent", activity: true });

		// 9 bare heartbeats every 100 ms: TTL stays alive, lastActivityAt goes stale (bare renew does
		// not refresh it), so the idle gate opens.
		for (let i = 0; i < 9; i++) {
			h.advance(100);
			h.heartbeat(a);
		}
		h.sweepAt(); // now=900, idle 900 > 300, waiters present, activity flagged
		assert.deepEqual(h.grantedSessions(b), ["B"], "a TTL-alive activity-idle holder yields to the waiter");
		assert.equal(h.beat(a, "A").granted, false, "the yielded holder learns via its next beat");
	});

	it("keyed beats during a long stream prevent idle-yield", () => {
		const h = new SchedulerHarness({ idleYieldMs: 500, ttlMs: 30000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent", activity: true });
		h.acquire(b, "B", { role: "parent", activity: true });

		// A long stream: keyed beats every 200 ms for 1.6 s (3× idleYieldMs) refresh lastActivityAt.
		for (let i = 0; i < 8; i++) {
			h.advance(200);
			h.beat(a, "A");
		}
		h.sweepAt();
		assert.deepEqual(h.grantsFor(b), [], "keyed beats keep the holder: no yield");

		h.release(a, "A");
		assert.deepEqual(h.grantedSessions(b), ["B"], "the waiter is granted on voluntary release");
	});

	it("never yields a lone idle holder (no waiters)", () => {
		const h = new SchedulerHarness({ idleYieldMs: 300, ttlMs: 30000 });
		const a = h.connect();

		h.acquire(a, "A", { role: "parent", activity: true });
		h.advance(600); // 2× idleYieldMs of silence, no waiters
		h.sweepAt();
		assert.equal(h.beat(a, "A").granted, true, "a lone idle holder keeps its slot");
	});

	it("never yields an unflagged (old-client) lease", () => {
		const h = new SchedulerHarness({ idleYieldMs: 300, ttlMs: 30000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent" }); // old client: no activity flag
		h.acquire(b, "B", { role: "parent", activity: true });
		h.advance(600);
		h.sweepAt();
		assert.deepEqual(h.grantsFor(b), [], "an unflagged holder is never yielded");

		h.release(a, "A");
		assert.deepEqual(h.grantedSessions(b), ["B"]);
	});

	it("idleYieldMs 0 disables idle-yield", () => {
		const h = new SchedulerHarness({ idleYieldMs: 0, ttlMs: 30000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent", activity: true });
		h.acquire(b, "B", { role: "parent", activity: true });
		h.advance(600);
		h.sweepAt();
		assert.deepEqual(h.grantsFor(b), [], "idleYieldMs 0 disables the yield");

		h.release(a, "A");
		assert.deepEqual(h.grantedSessions(b), ["B"]);
	});

	it("a yielded holder re-acquires and queues behind the new holder", () => {
		const h = new SchedulerHarness({ idleYieldMs: 300, ttlMs: 30000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent", activity: true });
		h.acquire(b, "B", { role: "parent", activity: true });
		h.advance(600);
		h.sweepAt(); // A yields, B is granted
		assert.deepEqual(h.grantedSessions(b), ["B"]);

		// A re-acquires: B holds the slot, so A parks as a waiter (no grant).
		const reacquire = h.acquire(a, "A", { role: "parent", activity: true });
		assert.equal(reacquire.granted, false, "the yielded holder re-queues behind the new holder");

		h.release(b, "B");
		assert.deepEqual(h.grantedSessions(a), ["A", "A"], "the re-queued holder is granted on release");
	});

	it("idle-yield counts as a switch in status", () => {
		const h = new SchedulerHarness({ idleYieldMs: 300, ttlMs: 30000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent", activity: true });
		h.acquire(b, "B", { role: "parent", activity: true });
		h.advance(600);
		h.sweepAt(); // A yields, B is granted from waiting: 1 switch
		assert.equal(h.switches(QUEUE_KEY), 1, "the idle-yield handover counts as a switch");
	});
});
