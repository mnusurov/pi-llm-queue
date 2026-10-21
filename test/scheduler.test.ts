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
		assert.equal(h.beat(a, "A").refreshed, true, "a beat on a live lease refreshes its TTL (ticket #68)");
		assert.deepEqual(h.beat(a, "A").decisions, [], "a beat is a query: it emits no decision");

		h.release(a, "A");
		assert.equal(h.beat(a, "A").granted, false, "a beat on a lost slot tells the client to re-queue");
		assert.equal(h.beat(a, "A").refreshed, false, "a beat on a phantom holder refreshes nothing");
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

// A config edit and the Lease it already holds (#104). A Lease records the TTL it was granted
// under, and every refresh of its expiry uses the longer of that TTL and the Queue's current TTL:
// a policy edit never shortens a Lease that is already held (ADR 0002), so the Holder keeps its
// Slot until it releases it. The edit itself is the harness's `editPolicy` — the hot-reload, in
// memory: the Queue's terms change, the Leases it holds keep theirs.
describe("policy edit / lease TTL (in memory)", () => {
	it("a shortened TTL never takes the Slot from a live Holder (the client's own reload never lands)", () => {
		const h = new SchedulerHarness({ ttlMs: 30000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent" });
		h.acquire(b, "B", { role: "parent" }); // B waits behind A
		h.advance(100);
		h.beat(a, "A"); // the client beats at the old period: TTL / 3 = 10 s

		// The daemon's config edit lands: the Queue's TTL is a third of what it was. The Extension's
		// reload may land late or never, so A keeps beating at the old period.
		h.editPolicy({ ttlMs: 1000 });

		h.advance(9900); // now = 10000: the first beat after the edit
		h.beat(a, "A");
		h.advance(1001); // now = 11001: past the expiry the shortened TTL would have written
		h.sweepAt();
		assert.deepEqual(h.holders(QUEUE_KEY), ["A"], "the Holder keeps its Slot: a policy edit never shortens a held Lease");
		assert.deepEqual(h.grantedSessions(b), [], "the Waiter is not granted: the Slot never changed hands");
	});

	it("a Lease granted after the edit is held for the new TTL", () => {
		const h = new SchedulerHarness({ ttlMs: 30000 });
		const a = h.connect();

		h.editPolicy({ ttlMs: 1000 });
		h.acquire(a, "A", { role: "parent" }); // granted under the new terms: expires at 1000

		h.sweepAt(999);
		assert.deepEqual(h.holders(QUEUE_KEY), ["A"], "the new Lease lives under the new TTL");
		h.sweepAt(1001);
		assert.deepEqual(h.holders(QUEUE_KEY), [], "the new Lease lapses at the new TTL");
	});

	it("a lengthened TTL applies at once: the Holder is evicted no earlier than the new TTL allows", () => {
		const h = new SchedulerHarness({ ttlMs: 1000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent" });
		h.acquire(b, "B", { role: "parent" });
		h.editPolicy({ ttlMs: 60000 }); // the Queue's TTL is lengthened
		h.beat(a, "A"); // the Holder's next beat refreshes under the new terms

		h.sweepAt(1001);
		assert.deepEqual(h.holders(QUEUE_KEY), ["A"], "the old TTL no longer reclaims the Holder");
		h.sweepAt(60001);
		assert.deepEqual(h.grantedSessions(b), ["B"], "the Holder lapses under the new TTL");
	});

	it("a lengthened TTL applies at once even when the Holder's expiry is near (the rebuild re-dates it)", () => {
		const h = new SchedulerHarness({ ttlMs: 1000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent" }); // granted at 0, expires at 1000
		h.acquire(b, "B", { role: "parent" });
		h.advance(900); // now = 900: the Holder's expiry is 100 ms away
		h.editPolicy({ ttlMs: 60000 }); // the Queue's TTL is lengthened

		h.sweepAt(1001); // the expiry the Holder was granted under has passed
		assert.deepEqual(h.holders(QUEUE_KEY), ["A"], "the lengthened TTL applies at once: the Holder is not reclaimed at the old TTL");
		h.sweepAt(60901);
		assert.deepEqual(h.grantedSessions(b), ["B"], "the Holder lapses under the new TTL");
	});

	it("a lengthening a later shortening cannot take back", () => {
		const h = new SchedulerHarness({ ttlMs: 1000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent" });
		h.acquire(b, "B", { role: "parent" });
		h.editPolicy({ ttlMs: 60000 }); // the Queue's TTL is lengthened
		h.beat(a, "A");
		h.advance(100);
		h.editPolicy({ ttlMs: 1000 }); // and shortened again: the client still beats at the long period

		h.advance(2000);
		h.beat(a, "A"); // the client's next beat, far apart under the shortened TTL
		h.advance(1001);
		h.sweepAt();
		assert.deepEqual(h.holders(QUEUE_KEY), ["A"], "the Lease keeps the longest TTL it was held under");
		assert.deepEqual(h.grantedSessions(b), [], "the Waiter is not granted: the Slot never changed hands");
	});

	it("the rebuild carries the Holder's terms and restarts its activity clock", () => {
		const h = new SchedulerHarness({ idleYieldMs: 300, ttlMs: 30000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent", activity: true });
		h.acquire(b, "B", { role: "parent", activity: true });
		h.advance(600); // 2× idleYieldMs of silence: the Holder is idle
		h.editPolicy({ idleYieldMs: 300, ttlMs: 30000 }); // the same TTL: only the carry happens

		h.sweepAt();
		assert.deepEqual(h.grantsFor(b), [], "the rebuild restarts the carried Holder's activity clock: no yield");
		h.advance(301);
		h.sweepAt();
		assert.deepEqual(h.grantedSessions(b), ["B"], "the Holder is idle past the window: it yields");
	});

	it("a Queue whose TTL did not change advances the expiry with every beat", () => {
		const h = new SchedulerHarness({ ttlMs: 1000 });
		const a = h.connect();

		h.acquire(a, "A", { role: "parent" }); // granted at 0, expires at 1000
		h.editPolicy({ ttlMs: 1000 }); // the same TTL: nothing changes
		h.advance(500);
		h.beat(a, "A"); // the expiry advances to 1500

		h.sweepAt(1001);
		assert.deepEqual(h.holders(QUEUE_KEY), ["A"], "the beat advanced the expiry past the old one");
		h.sweepAt(1501);
		assert.deepEqual(h.holders(QUEUE_KEY), [], "the Lease lapses one TTL after the last beat");
	});

	it("a bare heartbeat refreshes the Lease under the same rule", () => {
		const h = new SchedulerHarness({ ttlMs: 30000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent" });
		h.acquire(b, "B", { role: "parent" });
		h.editPolicy({ ttlMs: 1000 });

		h.advance(10000);
		h.heartbeat(a); // the connection-wide refresh
		h.advance(1001); // now = 11001: past the expiry the shortened TTL would have written
		h.sweepAt();
		assert.deepEqual(h.holders(QUEUE_KEY), ["A"], "a bare heartbeat keeps the Holder's Slot too");
	});

	it("a re-acquire refreshes the Lease under the same rule", () => {
		const h = new SchedulerHarness({ ttlMs: 30000 });
		const a = h.connect();

		h.acquire(a, "A", { role: "parent" });
		h.editPolicy({ ttlMs: 1000 });
		h.advance(10000);
		const again = h.acquire(a, "A", { role: "parent" }); // the Grace re-acquire: a TTL refresh
		assert.equal(again.refreshed, true, "the held Lease is refreshed, not granted again");

		h.advance(1001); // now = 11001
		h.sweepAt();
		assert.deepEqual(h.holders(QUEUE_KEY), ["A"], "the re-acquire keeps the Holder's Slot under the shortened TTL");
	});
});

// The Lease's terms (#105): the TTL and the idle-yield window it is held under. The Grant carries
// them, and the client derives its refresh period from them — so a config edit one process sees and
// the other does not can never shorten a Lease's life. The TTL term may grow, like #104's rule; the
// idle-yield window does not change while the Lease is held, because a window the daemon lengthened
// under a live Holder would leave that Holder's client beating slower than the window allows, and
// the Slot would go to a Waiter while the Holder still streams.
describe("lease terms (in memory)", () => {
	it("the Grant carries the terms the Lease is held under", () => {
		const h = new SchedulerHarness({ idleYieldMs: 4000, ttlMs: 12000 });
		const a = h.connect();

		h.acquire(a, "A", { role: "parent" });
		const [grant] = h.grantsFor(a);
		assert.deepEqual(grant.terms, { leaseTtlMs: 12000, idleYieldMs: 4000 }, "the Grant carries the terms the Lease is held under");
	});

	it("a shortened idle-yield window applies at the next Grant, not to a held Lease", () => {
		const h = new SchedulerHarness({ idleYieldMs: 300, ttlMs: 30000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent", activity: true });
		h.acquire(b, "B", { role: "parent", activity: true });
		h.advance(100);
		h.editPolicy({ idleYieldMs: 100, ttlMs: 30000 }); // the window is shortened under the live Holder

		h.advance(250); // 350 ms of silence: past the shortened window (100 ms), inside the granted one (300 ms)
		h.sweepAt();
		assert.deepEqual(h.holders(QUEUE_KEY), ["A"], "the Holder keeps its Slot: the window it was granted under still holds");

		h.advance(100); // 450 ms of silence: past the granted window
		h.sweepAt();
		assert.deepEqual(h.grantedSessions(b), ["B"], "the Holder is idle past its granted window: it yields");
	});

	it("a Lease granted after the edit is held under the new window", () => {
		const h = new SchedulerHarness({ idleYieldMs: 300, ttlMs: 30000 });
		const a = h.connect();

		h.editPolicy({ idleYieldMs: 100 });
		h.acquire(a, "A", { role: "parent", activity: true });
		const [grant] = h.grantsFor(a);
		assert.equal(grant.terms?.idleYieldMs, 100, "the new Lease is granted under the new window");
	});

	it("a lengthened idle-yield window does not reach a held Lease", () => {
		// The window is the one the Lease was granted under: a longer window would leave the
		// Holder's client beating slower than the window allows, and the Sweep would yield a Holder
		// that is still streaming (#105).
		const h = new SchedulerHarness({ idleYieldMs: 300, ttlMs: 30000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent", activity: true });
		h.acquire(b, "B", { role: "parent", activity: true });
		h.advance(100);
		h.editPolicy({ idleYieldMs: 1000, ttlMs: 30000 }); // the window is lengthened under the live Holder

		h.advance(350); // 450 ms of silence: past the window A was granted under (300 ms)
		h.sweepAt();
		assert.deepEqual(h.grantedSessions(b), ["B"], "A is idle past its own window: it yields — the lengthened window does not reach it");
	});

	it("idleYieldMs 0 disables the yield at the next Grant, not under a held Lease", () => {
		const h = new SchedulerHarness({ idleYieldMs: 300, ttlMs: 30000 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent", activity: true });
		h.acquire(b, "B", { role: "parent", activity: true });
		h.advance(100);
		h.editPolicy({ idleYieldMs: 0, ttlMs: 30000 }); // the yield is disabled

		h.advance(350); // 450 ms of silence: past the window A was granted under (300 ms)
		h.sweepAt();
		assert.deepEqual(h.grantedSessions(b), ["B"], "A keeps the window it was granted under: the disable reaches the next Grant");

		const c = h.connect();
		h.acquire(c, "C", { role: "parent", activity: true }); // queued behind B, which took the freed Slot
		h.release(b, "B");
		const [grant] = h.grantsFor(c);
		assert.equal(grant.terms?.idleYieldMs, 0, "the next Lease is granted with the yield disabled");
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

		const reacquire = h.acquire(a, "A", { role: "parent", activity: true });
		assert.equal(reacquire.granted, false, "the yielded holder re-queues behind the new holder");

		// Assert the re-queue Grant specifically: the release's decision list carries exactly the
		// handover grant to A, not A's earlier (pre-yield) grant from the cumulative capture.
		const handover = h.release(b, "B");
		assert.deepEqual(
			handover.filter((d) => d.type === "grant").map((d) => d.sessionKey),
			["A"],
			"the re-queued holder is granted on release",
		);
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

// The serialisation family, migrated off the socket seam (spec #65 phase C). The queue order is a
// Scheduler property: arrival order, disconnect-drop, cancel-drop. The socket suite keeps one smoke
// test for the wire path (`serialises two sessions...`); these assert the decisions directly.
describe("serialisation (in memory)", () => {
	it("grants queued sessions in arrival order (FIFO)", () => {
		const h = new SchedulerHarness();
		const first = h.connect();
		const second = h.connect();
		const third = h.connect();

		h.acquire(first, "A", { role: "parent" }); // direct grant into the free slot
		h.acquire(second, "B", { role: "parent" });
		h.acquire(third, "C", { role: "parent" });
		assert.deepEqual(h.grantsFor(second), [], "the slot is taken: both park as Waiters");

		// Each release hands the slot to the earliest waiter; the order is the arrival order.
		h.release(first, "A");
		assert.deepEqual(h.grantedSessions(second), ["B"]);
		h.release(second, "B");
		assert.deepEqual(h.grantedSessions(third), ["C"]);
	});

	it("drops a queued session that disconnects, and grants the patient one", () => {
		const h = new SchedulerHarness();
		const holder = h.connect();
		const impatient = h.connect();
		const patient = h.connect();

		h.acquire(holder, "H", { role: "parent" });
		h.acquire(patient, "P", { role: "parent" });
		h.acquire(impatient, "I", { role: "parent" });
		assert.deepEqual(h.grantsFor(impatient), [], "the impatient client is queued ahead of P");

		h.disconnect(impatient); // a close drops the queued request, no reply, no grant
		h.release(holder, "H");
		assert.deepEqual(h.grantedSessions(patient), ["P"], "the dropped waiter is skipped, P takes the slot");
		assert.deepEqual(h.grantsFor(impatient), [], "the disconnected client is never granted");
	});

	it("releases a held slot when its holder disconnects", () => {
		const h = new SchedulerHarness();
		const holder = h.connect();
		const waiter = h.connect();

		h.acquire(holder, "HH", { role: "parent" });
		h.acquire(waiter, "W", { role: "parent" });
		assert.deepEqual(h.grantsFor(waiter), [], "W waits behind the holder");

		h.disconnect(holder); // a close while holding frees the slot and hands it on
		assert.deepEqual(h.grantedSessions(waiter), ["W"]);
		assert.deepEqual(h.holders(QUEUE_KEY), ["W"], "no phantom holder remains");
	});

	it("drops a queued request on cancel and grants the next waiter", () => {
		const h = new SchedulerHarness();
		const holder = h.connect();
		const canceller = h.connect();
		const next = h.connect();

		h.acquire(holder, "H", { role: "parent" });
		h.acquire(canceller, "X", { role: "parent" });
		h.acquire(next, "N", { role: "parent" });
		assert.deepEqual(h.grantsFor(next), [], "N queues behind X");

		h.cancel(canceller, "X"); // an abort drops X from the waiting list
		h.release(holder, "H");
		assert.deepEqual(h.grantedSessions(next), ["N"], "N (queued after X) takes the slot, not X");
		assert.deepEqual(h.grantsFor(canceller), [], "the cancelled request is never granted");
	});
});

// Tier ordering (parent > child), migrated off the socket seam. The socket suite keeps the
// parent-beats-child smoke; FIFO-within-tier and the role re-tier op are Scheduler properties.
describe("tier ordering (in memory)", () => {
	it("grants queued parents in arrival order (FIFO within tier)", () => {
		const h = new SchedulerHarness();
		const holder = h.connect();
		const a = h.connect();
		const b = h.connect();
		const c = h.connect();

		h.acquire(holder, "H", { role: "parent" });
		h.acquire(a, "A", { role: "parent" });
		h.acquire(b, "B", { role: "parent" });
		h.acquire(c, "C", { role: "parent" });
		assert.deepEqual(h.grantsFor(a), [], "all three parents wait behind H");

		// One slot: each release grants exactly the next parent in arrival order.
		h.release(holder, "H");
		assert.deepEqual(h.grantedSessions(a), ["A"]);
		h.release(a, "A");
		assert.deepEqual(h.grantedSessions(b), ["B"]);
		h.release(b, "B");
		assert.deepEqual(h.grantedSessions(c), ["C"]);
	});

	it("re-tiers a queued waiter on the role op (promote a child ahead of a parent)", () => {
		const h = new SchedulerHarness();
		const holder = h.connect();
		const child = h.connect();
		const parent = h.connect();

		h.acquire(holder, "H", { role: "parent" });
		h.acquire(child, "C", { role: "child" });
		h.acquire(parent, "P", { role: "parent" });
		assert.deepEqual(h.grantsFor(child), [], "as a child, C ranks behind P");

		h.retier(child, "C", "parent"); // /queue promote: the waiter is re-tiered in place
		h.release(holder, "H");
		assert.deepEqual(h.grantedSessions(child), ["C"], "promoted, C leads the parent tier (it enqueued first)");
		h.release(child, "C");
		assert.deepEqual(h.grantedSessions(parent), ["P"]);
	});
});

// Aging (the anti-starvation pair), migrated off the socket seam. These are the streak regressions:
// the bump lives in one Grant path, so the direct-grant fast path and the queued path cannot drift.
// Instants are exact: agingAfterMs is never crossed (agingAfterMs 100000, clock stays at 0), so the
// boost can only come from the streak.
describe("aging (in memory)", () => {
	it("boosts the longest-waiting child after agingAfterSlots consecutive parent grants", () => {
		const h = new SchedulerHarness({ agingAfterMs: 100000, agingAfterSlots: 2 });
		const holder = h.connect();
		const child = h.connect();
		const p1 = h.connect();
		const p2 = h.connect();
		const p3 = h.connect();

		h.acquire(holder, "H", { role: "parent" }); // direct grant: streak 1
		h.acquire(child, "C", { role: "child" });
		h.acquire(p1, "P1", { role: "parent" });
		h.acquire(p2, "P2", { role: "parent" });
		h.acquire(p3, "P3", { role: "parent" });
		assert.deepEqual(h.grantsFor(child), [], "agingAfterMs is not crossed: the child waits");

		h.release(holder, "H");
		// streak 1 < agingAfterSlots: the earliest parent wins, and its grant makes the streak 2.
		assert.deepEqual(h.grantedSessions(p1), ["P1"]);
		h.release(p1, "P1");
		// streak 2 == agingAfterSlots: the child is boosted ahead of the remaining parents.
		assert.deepEqual(h.grantedSessions(child), ["C"], "aged by the parent-grant streak, not by the clock");
		h.release(child, "C");
		// a child grant resets the streak: P2 (earliest parent) is next, then P3.
		assert.deepEqual(h.grantedSessions(p2), ["P2"]);
		h.release(p2, "P2");
		assert.deepEqual(h.grantedSessions(p3), ["P3"]);
	});

	it("counts direct (free-slot) parent grants toward the aging streak", () => {
		const h = new SchedulerHarness({ agingAfterMs: 100000, agingAfterSlots: 2 });
		const p1 = h.connect();
		const p2 = h.connect();
		const child = h.connect();
		const p3 = h.connect();

		h.acquire(p1, "P1", { role: "parent" }); // streak 1: a direct grant, never queued
		h.release(p1, "P1");
		h.acquire(p2, "P2", { role: "parent" }); // streak 2: the fast path counts too
		h.acquire(child, "C", { role: "child" });
		h.acquire(p3, "P3", { role: "parent" });
		assert.deepEqual(h.grantsFor(child), [], "C is queued behind P2");

		h.release(p2, "P2");
		// The streak is already 2 from the fast-path grants: C jumps ahead of P3, the only parent.
		assert.deepEqual(h.grantedSessions(child), ["C"], "fast-path grants aged the child past P3");
	});
});

// No preemption (#49 / ADR 0002), migrated off the socket seam. The socket suite keeps the silent-
// holder smoke; the long-run version — many requests, many sweeps, no Switch — is a Scheduler
// property, and the driver runs it at exact instants with no sweep timer.
describe("no preemption (in memory)", () => {
	it("many requests over a long run never switch the slot away", () => {
		const h = new SchedulerHarness(); // the daemon's resolved defaults, quanta included as absent
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent" });
		h.acquire(b, "B", { role: "parent" });
		assert.deepEqual(h.grantsFor(b), [], "B queues behind a live holder");

		// 12 keyed beats and 12 sweeps spread over 1.2 s: the old request-count and idle quanta evicted
		// A here. A live holder is never preempted — only a TTL lapse, a release, an abort, a yield.
		for (let i = 0; i < 12; i++) {
			h.advance(100);
			h.beat(a, "A");
			h.sweepAt();
		}
		assert.deepEqual(h.grantsFor(b), [], "a busy live holder keeps the slot across 12 beats and sweeps");
		assert.equal(h.switches(QUEUE_KEY), 0, "no Switch in the whole run");
		assert.deepEqual(h.holders(QUEUE_KEY), ["A"]);

		h.release(a, "A");
		assert.deepEqual(h.grantedSessions(b), ["B"], "the waiter is granted on voluntary release");
	});
});

// The `requestedKey` echo (spec #65 finding 1): a session queued on two group members must get each
// grant keyed by the key that request asked with, never the other's. The Scheduler carries the
// requestedKey on the Waiter and returns it on the grant decision; the adapter echoes it. This is a
// Scheduler property (the per-request echo rides on the decision), so it is asserted in memory.
describe("requestedKey echo (in memory)", () => {
	const GROUP = "gpu";
	const MEMBER_A = "http://gpu.test/v1";
	const MEMBER_B = "http://zen.test/v1";

	it("carries the key each queued request asked with on its own grant decision", () => {
		const h = new SchedulerHarness();
		const holder = h.connect();
		const dual = h.connect();

		h.acquire(holder, "H", { role: "parent", queueKey: GROUP });
		// One session, two group-member keys, both queued on the group's single slot.
		h.acquire(dual, "S", { role: "parent", queueKey: GROUP, requestedKey: MEMBER_A });
		h.acquire(dual, "S", { role: "parent", queueKey: GROUP, requestedKey: MEMBER_B });
		assert.deepEqual(h.grantsFor(dual), [], "both member requests park as Waiters behind the holder");

		h.release(holder, "H", GROUP); // FIFO: the first waiter (asked with MEMBER_A) is granted
		assert.deepEqual(
			h.grantsFor(dual).map((g) => g.requestedKey),
			[MEMBER_A],
			"the first grant echoes the key that request asked with, not the other member's",
		);

		h.release(dual, "S", GROUP); // frees the slot, grants the second waiter (asked with MEMBER_B)
		assert.deepEqual(
			h.grantsFor(dual).map((g) => g.requestedKey),
			[MEMBER_A, MEMBER_B],
			"the second grant echoes its own key, proving the echo is per-request not per-session",
		);
	});
});

// The status view (tier, group label, per-tier depth, switch counter), migrated off the socket seam.
// The Scheduler's snapshot is the source `status` reports; the socket suite keeps the handover-switch
// smoke. The adapter's serialisation of this shape is its own contract, tested on the wire.
describe("status view (in memory)", () => {
	const queueOf = (h: SchedulerHarness, queueKey = QUEUE_KEY) => h.snapshot().find((q) => q.queueKey === queueKey);
	// The snapshot returns raw Holder/Waiter records (tier under the Scheduler's word); the adapter's
	// sessionView renames it to the wire `role`. The test projects the same three fields, renaming
	// tier→role as the adapter does, to assert the tier/group property over the raw source.
	const view = (list: { sessionKey: string; tier: string; group: string }[] | undefined) =>
		(list ?? []).map((e) => ({ sessionKey: e.sessionKey, role: e.tier, group: e.group }));

	it("reports tier and group label for every Holder and Waiter", () => {
		const h = new SchedulerHarness();
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "P", { role: "parent", group: "interactive" });
		h.acquire(b, "C", { role: "child", group: "scout" });
		assert.deepEqual(h.grantsFor(b), [], "the child waits behind the parent");

		// A Holder is not a bare session id: it carries role and group exactly like a Waiter.
		assert.deepEqual(
			[view(queueOf(h)?.holders), view(queueOf(h)?.waiting)],
			[
				[{ sessionKey: "P", role: "parent", group: "interactive" }],
				[{ sessionKey: "C", role: "child", group: "scout" }],
			],
		);

		h.release(a, "P");
		assert.deepEqual(
			view(queueOf(h)?.holders),
			[{ sessionKey: "C", role: "child", group: "scout" }],
			"a Holder granted from the waiting list keeps the tier and label it queued as",
		);
	});

	it("reports queue depth per tier in status", () => {
		const h = new SchedulerHarness();
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent" });
		h.acquire(b, "P", { role: "parent" });
		h.acquire(b, "C", { role: "child" }); // one connection may park several Waiters

		assert.deepEqual(
			queueOf(h)?.waiting.map((w: { sessionKey: string; tier: string }) => [w.sessionKey, w.tier]),
			[
				["P", "parent"],
				["C", "child"],
			],
			"every waiter carries its tier, so /queue can report per-tier depth",
		);
	});
});
