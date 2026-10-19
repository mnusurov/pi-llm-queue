// The Scheduler: the daemon's deciding part (ADR 0004, GLOSSARY "Scheduler"). It owns the queues
// Map, every Queue's state, and the Lease records (each carrying the connId it was granted to), and
// it answers who holds, who waits, and when a Switch happens. It performs no socket write, no log
// append and no clock read: `now` is a parameter on every entry, and every side effect is returned
// to the adapter as a decision in the order it must be emitted.
//
// Plain JS on purpose (the src/config.js reason): the daemon imports it and Node's built-in type
// stripping is refused for files under node_modules, so a .ts import would kill an installed daemon.
// It imports no config module: the adapter resolves policy through src/config.js and passes plain
// values in via the resolvePolicy callback.

// Decisions the adapter emits, in order:
//   grant   — a session takes the Slot: the adapter logs "grant" and sends the `granted` reply.
//   switch  — the grant that hands the Slot to a NEW session (came from `waiting`): log + counter.
//   queued  — an acquire parked as a Waiter: log "queued".
//   release — a Slot returns (voluntary, or an idle Yield with reason "idle"): log "release".
//   evict   — a TTL reclaim: log "evict" with reason "ttl".
// A direct grant into a free Slot is a Grant, not a Switch (protocol-facts), so it emits no switch.

export class Scheduler {
	// resolvePolicy(queueKey) -> { maxStreams, ttlMs, agingAfterSlots, agingAfterMs, policy, idleYieldMs }
	// (plain resolved values; the adapter owns the config.js resolution and the defaults fallback).
	constructor(resolvePolicy) {
		this.resolvePolicy = resolvePolicy;
		this.queues = new Map();
		// The connection scope of a Lease (protocol fact: a bare renew heartbeats every lease of the
		// connection). Each Lease lives in queue.holders AND here; the granted answer is an identity
		// comparison across both, so both live here (ADR 0004, "Considered options").
		this.connLeases = new Map(); // connId -> Lease[]
		this.connQueued = new Map(); // connId -> Waiter[]
	}

	// Queue policy, re-applied on hot-reload. `enabled: false` is passthrough: the adapter resolves
	// maxStreams to Infinity, so every session is granted immediately (status reports null).
	applyPolicyTo(queue, policy) {
		queue.maxStreams = policy.maxStreams;
		// A lease dies only without heartbeats (TTL): a live holder is never preempted (#49 / ADR
		// 0002), so the slot moves on voluntary release, abort, disconnect, or a TTL lapse alone.
		queue.ttlMs = policy.ttlMs;
		// Aging: anti-starvation for background jobs — a child is boosted ahead of parents after this
		// many consecutive parent grants, or after waiting this long.
		queue.agingAfterSlots = policy.agingAfterSlots;
		queue.agingAfterMs = policy.agingAfterMs;
		// "fifo" opts a queue out of parent/child tiering and aging (external multi-slot queues).
		queue.policy = policy.policy;
		// Idle-yield (ADR 0003): an activity-flagged holder silent past idleYieldMs, with Waiters
		// present, yields the Slot. 0 disables the feature.
		queue.idleYieldMs = policy.idleYieldMs;
		return queue;
	}

	newQueue(queueKey) {
		return this.applyPolicyTo(
			{ queueKey, parentGrantStreak: 0, switches: 0, holders: [], waiting: [] },
			this.resolvePolicy(queueKey),
		);
	}

	// Re-resolve policy for every live queue (hot-reload). Capacity changes take effect on the next
	// grant; the adapter calls grantNext after this to hand out any newly-free Slot.
	applyPolicy() {
		for (const queue of this.queues.values()) this.applyPolicyTo(queue, this.resolvePolicy(queue.queueKey));
	}

	queueFor(queueKey) {
		if (!this.queues.has(queueKey)) this.queues.set(queueKey, this.newQueue(queueKey));
		return this.queues.get(queueKey);
	}

	leaseOf(queue, sessionKey) {
		return queue.holders.find((l) => l.sessionKey === sessionKey);
	}

	pushLease(lease) {
		let list = this.connLeases.get(lease.connId);
		if (!list) this.connLeases.set(lease.connId, (list = []));
		list.push(lease);
	}

	pushQueued(connId, waiter) {
		let list = this.connQueued.get(connId);
		if (!list) this.connQueued.set(connId, (list = []));
		list.push(waiter);
	}

	// Pick the next waiter: an aged child (longest-waiting) jumps the queue; otherwise the earliest
	// parent, then the earliest child — FIFO within each tier (parent > child).
	pickNext(queue, now) {
		if (queue.policy === "fifo") return queue.waiting[0];
		let aged = null;
		for (const w of queue.waiting) {
			if (w.role !== "child") continue;
			if (now - w.enqueuedAt >= queue.agingAfterMs || queue.parentGrantStreak >= queue.agingAfterSlots) {
				if (!aged || w.enqueuedAt < aged.enqueuedAt) aged = w;
			}
		}
		if (aged) return aged;
		return queue.waiting.find((w) => w.role === "parent") ?? queue.waiting[0];
	}

	// The single Grant path: the Aging streak is bumped here once, so the direct-grant fast path and
	// the queued path cannot drift (ADR 0004 consequence). `fromWaiting` is the Switch predicate: a
	// Switch fires iff the granted session came from `waiting` (a direct grant is not a Switch).
	grantLease(queue, requester, now, fromWaiting, decisions) {
		const lease = {
			queueKey: queue.queueKey,
			sessionKey: requester.sessionKey,
			connId: requester.connId,
			role: requester.role,
			group: requester.group,
			expires: now + queue.ttlMs,
			activity: !!requester.activity,
			lastActivityAt: now,
		};
		queue.holders.push(lease);
		this.pushLease(lease);
		// A direct grant counts toward the parent-grant streak too: a parent that never queues must
		// not starve a queued child past agingAfterSlots.
		queue.parentGrantStreak = lease.role === "parent" ? queue.parentGrantStreak + 1 : 0;
		// The grant carries the requester's `requestedKey` (the key it asked with, opaque to the
		// Scheduler) so the adapter echoes it per-request: a session queued on two group members gets
		// each grant keyed by the key that request used, never the other's (protocol:228-232).
		decisions.push({ type: "grant", connId: lease.connId, queueKey: queue.queueKey, sessionKey: lease.sessionKey, role: lease.role, requestedKey: requester.requestedKey });
		if (fromWaiting) {
			// Handing the Slot to a new session is the Switch that re-prefills the KV cache.
			queue.switches++;
			decisions.push({ type: "switch", queueKey: queue.queueKey, sessionKey: lease.sessionKey, role: lease.role });
		}
		return lease;
	}

	grantNext(queue, now, decisions = []) {
		while (queue.waiting.length && queue.holders.length < queue.maxStreams) {
			const waiter = this.pickNext(queue, now);
			remove(queue.waiting, waiter);
			this.grantLease(queue, waiter, now, true, decisions);
			remove(this.connQueued.get(waiter.connId) ?? [], waiter);
		}
		return decisions;
	}

	// acquire: re-acquiring a held Slot is a TTL refresh (Grace window: a Parent woken by its Child
	// keeps the Slot, so the handback costs no Switch and logs nothing). A free Slot is a direct Grant.
	// Otherwise the request parks as a Waiter. Returns { granted, decisions }.
	acquire(connId, sessionKey, tier, group, activity, queueKey, requestedKey, now) {
		const queue = this.queueFor(queueKey);
		const role = tier ?? "parent";
		const held = this.leaseOf(queue, sessionKey);
		if (held) {
			held.expires = now + queue.ttlMs; // re-acquiring a held slot refreshes its TTL
			return { granted: true, refreshed: true, decisions: [] };
		}
		const decisions = [];
		if (queue.holders.length < queue.maxStreams) {
			this.grantLease(queue, { sessionKey, connId, activity, role, group, requestedKey }, now, false, decisions);
			return { granted: true, refreshed: false, decisions };
		}
		const waiter = { sessionKey, role, group, connId, queueKey, requestedKey, enqueuedAt: now, activity: !!activity };
		queue.waiting.push(waiter);
		this.pushQueued(connId, waiter);
		decisions.push({ type: "queued", connId, queueKey: queue.queueKey, sessionKey, role });
		return { granted: false, refreshed: false, decisions };
	}

	// beat (keyed renew): the authoritative `granted` answer. A live lease in queue.holders that is
	// also this connection's lease for this session refreshes TTL and lastActivityAt (a keyed beat
	// prevents idle-yield). granted:false tells the client the Slot is gone and it must re-queue.
	beat(connId, sessionKey, queueKey, now) {
		const queue = this.queueFor(queueKey);
		const lease = (this.connLeases.get(connId) ?? []).find((l) => l.sessionKey === sessionKey && l.queueKey === queue.queueKey);
		const granted = !!lease && this.leaseOf(queue, sessionKey) === lease;
		if (granted) {
			lease.expires = now + queue.ttlMs;
			lease.lastActivityAt = now;
		}
		return { granted };
	}

	// heartbeat (bare renew): keeps every lease of this connection alive. Refreshes TTL only, NOT
	// lastActivityAt, so a bare heartbeat does not prevent an idle-yield (ADR 0003).
	heartbeat(connId, now) {
		for (const lease of this.connLeases.get(connId) ?? []) lease.expires = now + this.queueFor(lease.queueKey).ttlMs;
	}

	// release: a voluntary return. Removes the Lease and hands the Slot on (grantNext).
	release(connId, sessionKey, queueKey, now) {
		const decisions = [];
		const queue = this.queues.get(queueKey);
		if (queue) this.releaseLease(queue, sessionKey, undefined, now, decisions);
		return decisions;
	}

	// cancel: drop a queued request the client aborted; if it was already granted, release the Slot.
	cancel(connId, sessionKey, queueKey, now) {
		const decisions = [];
		const queue = this.queues.get(queueKey);
		if (queue) {
			const w = queue.waiting.find((x) => x.sessionKey === sessionKey && x.connId === connId);
			if (w) {
				remove(queue.waiting, w);
				remove(this.connQueued.get(connId) ?? [], w);
			}
			if (this.leaseOf(queue, sessionKey)) this.releaseLease(queue, sessionKey, undefined, now, decisions);
		}
		return decisions;
	}

	// retier (/queue promote|demote): re-tier a live queued waiter in place (no reply, like cancel).
	retier(connId, sessionKey, tier) {
		const role = tier === "child" ? "child" : "parent";
		for (const queue of this.queues.values()) {
			const w = queue.waiting.find((x) => x.sessionKey === sessionKey && x.connId === connId);
			if (w) {
				w.role = role;
				break;
			}
		}
	}

	// disconnect: a close (or a `bye`) is an abort — release every lease it held, drop every queued
	// request. Replaces the daemon's cleanup/bye. The adapter decrements the Counted-client count AFTER
	// this returns. Iterate copies: releaseLease splices the conn lease list, and a client can hold
	// several leases.
	disconnect(connId, now) {
		const decisions = [];
		for (const lease of [...(this.connLeases.get(connId) ?? [])]) this.releaseLease(this.queueFor(lease.queueKey), lease.sessionKey, undefined, now, decisions);
		for (const w of [...(this.connQueued.get(connId) ?? [])]) {
			remove(this.queueFor(w.queueKey).waiting, w);
			remove(this.connQueued.get(connId) ?? [], w);
		}
		this.connLeases.delete(connId);
		this.connQueued.delete(connId);
		return decisions;
	}

	// The single release primitive. The log names the cause: a TTL reclaim is an evict — the only
	// eviction reason after ADR 0002 — a yield is a release with reason "idle", a voluntary return a
	// bare release. Then hand the Slot on.
	releaseLease(queue, sessionKey, reason, now, decisions) {
		const lease = this.leaseOf(queue, sessionKey);
		if (lease) {
			remove(queue.holders, lease);
			remove(this.connLeases.get(lease.connId) ?? [], lease);
			if (reason === "ttl") decisions.push({ type: "evict", queueKey: queue.queueKey, sessionKey, role: lease.role, reason: "ttl" });
			else decisions.push({ type: "release", queueKey: queue.queueKey, sessionKey, role: lease.role, reason });
		}
		this.grantNext(queue, now, decisions);
	}

	// sweepAt: TTL evict (unconditional, so `status` never reports a ghost holder) then idle-yield
	// (gated on idleYieldMs > 0, Waiters present, and the activity flag — ADR 0003).
	sweepAt(now) {
		const decisions = [];
		for (const queue of this.queues.values()) {
			for (const lease of [...queue.holders]) if (now > lease.expires) this.releaseLease(queue, lease.sessionKey, "ttl", now, decisions);
			// Idle-yield: an activity-flagged holder silent past idleYieldMs with waiters present yields
			// the slot. A lone holder is never disturbed; unflagged (old-client) leases are never yielded.
			if (queue.idleYieldMs > 0 && queue.waiting.length > 0) {
				for (const lease of [...queue.holders]) {
					if (lease.activity && now - lease.lastActivityAt > queue.idleYieldMs) this.releaseLease(queue, lease.sessionKey, "idle", now, decisions);
				}
			}
		}
		return decisions;
	}

	// snapshot: the raw queue state for the adapter's `status` view. The adapter maps it to the wire
	// shape (sessionView, maxStreams Infinity -> JSON null) so the Scheduler never serialises.
	snapshot() {
		return [...this.queues.values()].map((q) => ({
			queueKey: q.queueKey,
			maxStreams: q.maxStreams,
			switches: q.switches,
			holders: q.holders.map((l) => ({ sessionKey: l.sessionKey, role: l.role, group: l.group })),
			waiting: q.waiting.map((w) => ({ sessionKey: w.sessionKey, role: w.role, group: w.group })),
		}));
	}
}

function remove(list, item) {
	const at = list.indexOf(item);
	if (at >= 0) list.splice(at, 1);
}
