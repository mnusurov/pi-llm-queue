// The Scheduler: the daemon's deciding part (ADR 0004, GLOSSARY "Scheduler"). It owns the queues
// Map, every Queue's state, and the Lease records (each carrying the connId it was granted to), and
// it answers who holds, who waits, and when a Switch happens. It performs no socket write, no log
// append and no clock read: `now` is a parameter on every entry, and every side effect is returned
// to the adapter as a decision in the order it must be emitted.
//
// Plain JS on purpose (the src/config.js reason): the daemon imports it and Node's built-in type
// stripping is refused for files under node_modules, so a .ts import would kill an installed daemon.
// It imports no config module: the adapter resolves policy through src/config.js and passes plain
// values in via the resolvePolicy callback. The Lease's terms are the terms module's (ADR 0009):
// this one holds them and asks it for the growth rule, but never writes their shape.

// Decisions the adapter emits, in order (the shape is the decisions module's, ADR 0010):
//   grant   — a session takes the Slot: the adapter logs "grant" and sends the `granted` reply.
//   switch  — the grant that hands the Slot to a NEW session (came from `waiting`): log + counter.
//   queued  — an acquire parked as a Waiter: log "queued".
//   release — a Slot returns (voluntary, or an idle Yield with reason "idle"): log "release".
//   evict   — a TTL reclaim: log "evict" with reason "ttl".
// A direct grant into a free Slot is a Grant, not a Switch (protocol-facts), so it emits no switch.

import { growTerms, termsFor } from "./lease-terms.js";

/**
 * The Decision the Scheduler answers with, and the over-capacity report its Queue rebuild returns:
 * the shapes are owned by the decisions module (ADR 0010), this one writes them.
 *
 * @typedef {import("./decisions.js").Decision} Decision
 * @typedef {import("./decisions.js").OverCapacityReport} OverCapacityReport
 * @typedef {import("./lease-terms.js").LeaseTerms} LeaseTerms
 */

/**
 * A Queue's resolved policy: the values the config cascade produced for it.
 *
 * @typedef {object} QueuePolicy
 * @property {number} maxStreams The Queue's capacity (Infinity when it is passthrough).
 * @property {number} ttlMs The TTL term a Lease is granted under.
 * @property {number} agingAfterSlots The parent-grant streak after which a child jumps the queue.
 * @property {number} agingAfterMs The waiting time after which a child jumps the queue.
 * @property {string} policy The Queue's tiering policy ("priority-aging" or "fifo").
 * @property {number} idleYieldMs The idle-yield window a Lease is held under.
 */

/**
 * The request a grant answers: a session asking for a Queue, with the identity the Scheduler
 * records on the Lease it grants.
 *
 * @typedef {object} Requester
 * @property {string} sessionKey The session that asks.
 * @property {number | string} connId The connection the session asks from.
 * @property {string} tier The session's priority class.
 * @property {string} [group] The session's group label.
 * @property {boolean} [activity] Whether the session reports activity.
 * @property {string} [requestedKey] The key the request asked with.
 */

/**
 * A parked request: a Requester that waits, with the instant it was enqueued at.
 *
 * @typedef {Requester & { enqueuedAt: number, queueKey: string }} Waiter
 */

/**
 * A Lease: a Queue's live Holder, with the terms it is held under.
 *
 * @typedef {object} Lease
 * @property {string} queueKey The Queue the Lease belongs to.
 * @property {string} sessionKey The session that holds it.
 * @property {number | string} connId The connection it was granted to.
 * @property {string} tier The session's priority class.
 * @property {string} [group] The session's group label.
 * @property {LeaseTerms} terms The terms the Lease is held under.
 * @property {number} expires The instant the Lease's TTL lapses at.
 * @property {boolean} activity Whether the session reports activity.
 * @property {number} lastActivityAt The last instant the session was seen active.
 */

/**
 * A Queue: its capacity, its rules, its Holders and its Waiters.
 *
 * @typedef {object} Queue
 * @property {string} queueKey The Queue's key.
 * @property {number} parentGrantStreak The consecutive parent grants the aging rule counts.
 * @property {number} switches How many times the Queue handed its Slot to a new session.
 * @property {Lease[]} holders The Queue's live Leases.
 * @property {Waiter[]} waiting The Queue's parked requests.
 * @property {number} maxStreams The Queue's capacity.
 * @property {number} agingAfterSlots The parent-grant streak after which a child jumps the queue.
 * @property {number} agingAfterMs The waiting time after which a child jumps the queue.
 * @property {string} policy The Queue's tiering policy.
 * @property {LeaseTerms} terms The terms new Leases are granted under.
 */

/**
 * A Queue before its policy is applied: the Queue's own state, with the policy fields still to fill.
 *
 * @typedef {object} QueueDraft
 * @property {string} queueKey The Queue's key.
 * @property {number} parentGrantStreak The consecutive parent grants the aging rule counts.
 * @property {number} switches How many times the Queue handed its Slot to a new session.
 * @property {Lease[]} holders The Queue's live Leases.
 * @property {Waiter[]} waiting The Queue's parked requests.
 * @property {number} [maxStreams] The Queue's capacity.
 * @property {number} [agingAfterSlots] The parent-grant streak after which a child jumps the queue.
 * @property {number} [agingAfterMs] The waiting time after which a child jumps the queue.
 * @property {string} [policy] The Queue's tiering policy.
 * @property {LeaseTerms} [terms] The terms new Leases are granted under.
 */

/**
 * One Queue's state as the adapter's `status` view reads it.
 *
 * @typedef {object} QueueSnapshot
 * @property {string} queueKey The Queue's key.
 * @property {number} maxStreams The Queue's capacity.
 * @property {number} switches The Queue's switch count.
 * @property {Lease[]} holders The Queue's live Leases.
 * @property {Waiter[]} waiting The Queue's parked requests.
 */

/**
 * The daemon's deciding part (ADR 0004): it owns the Queues, the Lease records and the waiting
 * list, and it answers who holds, who waits and when a Switch happens. It performs no side
 * effect — every entry that decides takes `now` and returns the decisions the adapter must emit,
 * in order.
 *
 * A Queue's Policy arrives through the resolver the adapter supplies; a queue key the resolver
 * cannot resolve is the adapter's error, not a state the Scheduler recovers from.
 *
 * The Queue key a Queue lives under after a config reload arrives through the second resolver
 * (ADR 0005): the Scheduler owns the Queue rebuild, so it asks for the target key itself.
 *
 * The entries are the whole public surface: the Queue state, the grants, the pick, the release and
 * the connection scope are private (#115), so the class's own rules cannot be driven from outside —
 * a caller asks, the Scheduler decides.
 */
export class Scheduler {
	// The live Queues, by Queue key. Private: the Adapter never enumerates them — the Scheduler
	// iterates its own (#103).
	/** @type {Map<string, Queue>} */
	#queues;
	// resolvePolicy(queueKey) -> { maxStreams, ttlMs, agingAfterSlots, agingAfterMs, policy, idleYieldMs }
	// (the TTL and the idle-yield window are the two values the terms are built from)
	// (plain resolved values; the adapter gets them from config.js resolveQueuePolicy, the one
	// cascade that owns the defaults fallback).
	/** @type {(queueKey: string) => QueuePolicy} */
	#resolvePolicy;
	// resolveQueueKey(queueKey) -> the Queue key that Queue lives under after a reload (the
	// Addressing rule; the adapter supplies the call, the Scheduler applies it).
	/** @type {(queueKey: string) => string} */
	#resolveQueueKey;
	// The connection scope of a Lease (protocol fact: a bare renew heartbeats every lease of the
	// connection). Each Lease lives in queue.holders AND here; the granted answer is an identity
	// comparison across both, so both live here (ADR 0004, "Considered options").
	/** @type {Map<number | string, Lease[]>} */
	#connLeases; // connId -> Lease[]
	/** @type {Map<number | string, Waiter[]>} */
	#connQueued; // connId -> Waiter[]
	/**
	 * @param {(queueKey: string) => QueuePolicy} resolvePolicy The config cascade, resolved by the adapter.
	 * @param {(queueKey: string) => string} resolveQueueKey The addressing rule: where a Queue lives after a reload.
	 */
	constructor(resolvePolicy, resolveQueueKey) {
		// The Queue rebuild's rule is required: a Scheduler without it cannot answer where a Queue
		// lives after a reload, and a default would silently drop the remap. Fail at construction, not
		// at the first reload.
		if (typeof resolveQueueKey !== "function") throw new Error("Scheduler: resolveQueueKey(queueKey) is required");
		this.#resolvePolicy = resolvePolicy;
		this.#resolveQueueKey = resolveQueueKey;
		this.#queues = new Map();
		this.#connLeases = new Map();
		this.#connQueued = new Map();
	}

	// Queue policy, re-applied on hot-reload. `enabled: false` is passthrough: the adapter resolves
	// maxStreams to Infinity, so every session is granted immediately (status reports null).
	// The Queue's own state is the draft's; the policy fields are filled in here.
	/**
	 * @param {QueueDraft} queue The Queue's own state; the policy fields are filled in here.
	 * @param {QueuePolicy} policy The queue's resolved policy.
	 * @returns {Queue} The queue, with the policy applied.
	 */
	#applyPolicyTo(queue, policy) {
		// A lease dies only without heartbeats (TTL): a live holder is never preempted (#49 / ADR
		// 0002), so the slot moves on voluntary release, abort, disconnect, or a TTL lapse alone.
		// Aging: anti-starvation for background jobs — a child is boosted ahead of parents after this
		// many consecutive parent grants, or after waiting this long.
		queue.maxStreams = policy.maxStreams;
		queue.agingAfterSlots = policy.agingAfterSlots;
		queue.agingAfterMs = policy.agingAfterMs;
		// "fifo" opts a queue out of parent/child tiering and aging (external multi-slot queues).
		queue.policy = policy.policy;
		// The terms new Leases are granted under (ADR 0009): the TTL term and the idle-yield
		// window. The TTL may be shorter than a held Lease's — the Lease keeps the TTL it was
		// granted under (#104) — and the window a held Lease keeps is the one it was granted
		// under (#105), so only a new Grant sees a changed one.
		queue.terms = termsFor(policy.ttlMs, policy.idleYieldMs);
		// The policy fields are filled in above, so the draft is a Queue now.
		return /** @type {Queue} */ (queue);
	}

	/**
	 * @param {string} queueKey The Queue's key.
	 * @returns {Queue} The new Queue.
	 */
	#newQueue(queueKey) {
		return this.#applyPolicyTo({ queueKey, parentGrantStreak: 0, switches: 0, holders: [], waiting: [] }, this.#resolvePolicy(queueKey));
	}

	/**
	 * @param {string} queueKey The Queue's key.
	 * @returns {Queue} The Queue, created if it is not there yet.
	 */
	#queueFor(queueKey) {
		const existing = this.#queues.get(queueKey);
		if (existing) return existing;
		const queue = this.#newQueue(queueKey);
		this.#queues.set(queueKey, queue);
		return queue;
	}

	/**
	 * @param {Queue} queue The queue.
	 * @param {string} sessionKey The session.
	 * @returns {Lease | undefined} The session's Lease, if it holds one.
	 */
	#leaseOf(queue, sessionKey) {
		return queue.holders.find((l) => l.sessionKey === sessionKey);
	}

	/**
	 * @param {Lease} lease The lease to record.
	 */
	#pushLease(lease) {
		let list = this.#connLeases.get(lease.connId);
		if (!list) this.#connLeases.set(lease.connId, (list = []));
		list.push(lease);
	}

	/**
	 * @param {number | string} connId The connection the waiter came from.
	 * @param {Waiter} waiter The waiter to record.
	 */
	#pushQueued(connId, waiter) {
		let list = this.#connQueued.get(connId);
		if (!list) this.#connQueued.set(connId, (list = []));
		list.push(waiter);
	}

	// Pick the next waiter: an aged child (longest-waiting) jumps the queue; otherwise the earliest
	// parent, then the earliest child — FIFO within each tier (parent > child).
	/**
	 * @param {Queue} queue The queue.
	 * @param {number} now The wall-clock instant, in ms since the epoch.
	 * @returns {Waiter | undefined} The waiter to grant next, if any.
	 */
	#pickNext(queue, now) {
		if (queue.policy === "fifo") return queue.waiting[0];
		let aged = null;
		for (const w of queue.waiting) {
			if (w.tier !== "child") continue;
			if (now - w.enqueuedAt >= queue.agingAfterMs || queue.parentGrantStreak >= queue.agingAfterSlots) {
				if (!aged || w.enqueuedAt < aged.enqueuedAt) aged = w;
			}
		}
		if (aged) return aged;
		return queue.waiting.find((w) => w.tier === "parent") ?? queue.waiting[0];
	}

	// The single Grant path: the Aging streak is bumped here once, so the direct-grant fast path and
	// the queued path cannot drift (ADR 0004 consequence). `fromWaiting` is the Switch predicate: a
	// Switch fires iff the granted session came from `waiting` (a direct grant is not a Switch).
	/**
	 * @param {Queue} queue The queue.
	 * @param {Requester} requester The request the grant answers.
	 * @param {number} now The wall-clock instant, in ms since the epoch.
	 * @param {boolean} fromWaiting Whether the grant came from the waiting list (a Switch).
	 * @param {Decision[]} decisions The decisions to append the grant to.
	 * @returns {Lease} The granted Lease.
	 */
	#grantLease(queue, requester, now, fromWaiting, decisions) {
		const lease = {
			queueKey: queue.queueKey,
			sessionKey: requester.sessionKey,
			connId: requester.connId,
			tier: requester.tier,
			group: requester.group,
			// The terms this Lease is held under: the TTL term and the idle-yield window. The TTL
			// term only ever grows — a policy edit never shortens a Lease that is already held
			// (ADR 0002) — and the window is the one the Lease was granted under, for as long as
			// it is held (#105). The client derives its refresh period from the terms the Grant
			// carried, so the window the daemon enforces must be the one the client was told.
			terms: termsFor(queue.terms.leaseTtlMs, queue.terms.idleYieldMs),
			expires: now + queue.terms.leaseTtlMs,
			activity: !!requester.activity,
			lastActivityAt: now,
		};
		queue.holders.push(lease);
		this.#pushLease(lease);
		// A direct grant counts toward the parent-grant streak too: a parent that never queues must
		// not starve a queued child past agingAfterSlots.
		queue.parentGrantStreak = lease.tier === "parent" ? queue.parentGrantStreak + 1 : 0;
		// The grant carries the requester's `requestedKey` (the key it asked with, opaque to the
		// Scheduler) so the adapter echoes it per-request: a session queued on two group members gets
		// each grant keyed by the key that request used, never the other's (protocol: reply matching).
		// It carries the Lease's terms too (#105): the client refreshes on the terms the daemon
		// granted the Lease under, never on its own copy of the config.
		decisions.push({
			type: "grant",
			connId: lease.connId,
			queueKey: queue.queueKey,
			sessionKey: lease.sessionKey,
			tier: lease.tier,
			requestedKey: requester.requestedKey,
			terms: lease.terms,
		});
		if (fromWaiting) {
			// Handing the Slot to a new session is the Switch that re-prefills the KV cache.
			queue.switches++;
			decisions.push({ type: "switch", queueKey: queue.queueKey, sessionKey: lease.sessionKey, tier: lease.tier });
		}
		return lease;
	}

	/**
	 * @param {Queue} queue The queue.
	 * @param {number} now The wall-clock instant, in ms since the epoch.
	 * @param {Decision[]} [decisions] The decisions to append the grants to.
	 * @returns {Decision[]} The decisions.
	 */
	#grantNext(queue, now, decisions = []) {
		while (queue.waiting.length && queue.holders.length < queue.maxStreams) {
			const waiter = this.#pickNext(queue, now);
			if (!waiter) break;
			remove(queue.waiting, waiter);
			this.#grantLease(queue, waiter, now, true, decisions);
			remove(this.#connQueued.get(waiter.connId) ?? [], waiter);
		}
		return decisions;
	}

	// acquire: re-acquiring a held Slot is a TTL refresh (Grace window: a Parent woken by its Child
	// keeps the Slot, so the handback costs no Switch and logs nothing). A free Slot is a direct Grant.
	// Otherwise the request parks as a Waiter. Returns { granted, decisions }.
	/**
	 * @param {number | string} connId The connection the request came from.
	 * @param {string} sessionKey The session that asks.
	 * @param {string | undefined} tier The session's priority class ("parent" when the client names none).
	 * @param {string | undefined} group The session's group label.
	 * @param {boolean | undefined} activity Whether the session reports activity.
	 * @param {string} queueKey The Queue the session asks for.
	 * @param {string | undefined} requestedKey The key the request asked with.
	 * @param {number} now The wall-clock instant, in ms since the epoch.
	 * @returns {{ granted: true, refreshed: true, decisions: Decision[], terms: LeaseTerms } | { granted: boolean, refreshed: false, decisions: Decision[] }} The answer and the decisions to emit. A refresh carries the Lease's terms: the term may have grown since the Grant.
	 */
	acquire(connId, sessionKey, tier, group, activity, queueKey, requestedKey, now) {
		const queue = this.#queueFor(queueKey);
		tier = tier ?? "parent";
		const held = this.#leaseOf(queue, sessionKey);
		if (held) {
			refreshExpiry(held, queue, now);
			// The refresh reply repeats the Lease's terms: the term may have grown since the Grant.
			return { granted: true, refreshed: true, decisions: [], terms: held.terms };
		}
		/** @type {Decision[]} */
		const decisions = [];
		if (queue.holders.length < queue.maxStreams) {
			this.#grantLease(queue, { sessionKey, connId, activity, tier, group, requestedKey }, now, false, decisions);
			// The terms ride on the grant decision: the adapter puts them in the reply.
			return { granted: true, refreshed: false, decisions };
		}
		const waiter = { sessionKey, tier, group, connId, queueKey, requestedKey, enqueuedAt: now, activity: !!activity };
		queue.waiting.push(waiter);
		this.#pushQueued(connId, waiter);
		decisions.push({ type: "queued", connId, queueKey: queue.queueKey, sessionKey, tier });
		return { granted: false, refreshed: false, decisions };
	}

	// beat (keyed renew): the authoritative `granted` answer, and a query returning
	// { granted, refreshed, decisions } (spec #65 / ticket #68). A live lease in queue.holders that is
	// also this connection's lease for this session refreshes TTL and lastActivityAt (a keyed beat
	// prevents idle-yield), so `refreshed` is true exactly when a live lease's TTL was refreshed here.
	// granted:false tells the client the Slot is gone and it must re-queue. A beat is a pure query: it
	// emits no decision (no log line, no reply beyond the granted answer), so `decisions` is always empty.
	/**
	 * @param {number | string} connId The connection the request came from.
	 * @param {string} sessionKey The session that asks.
	 * @param {string} queueKey The Queue the session asks for.
	 * @param {number} now The wall-clock instant, in ms since the epoch.
	 * @returns {{ granted: boolean, refreshed: boolean, decisions: Decision[] }} The answer and the decisions to emit.
	 */
	beat(connId, sessionKey, queueKey, now) {
		const queue = this.#queueFor(queueKey);
		const lease = (this.#connLeases.get(connId) ?? []).find((l) => l.sessionKey === sessionKey && l.queueKey === queue.queueKey);
		if (!lease) return { granted: false, refreshed: false, decisions: [] };
		const granted = this.#leaseOf(queue, sessionKey) === lease;
		if (granted) {
			refreshExpiry(lease, queue, now);
			lease.lastActivityAt = now;
		}
		return { granted, refreshed: granted, decisions: [] };
	}

	// heartbeat (bare renew): keeps every lease of this connection alive. Refreshes TTL only, NOT
	// lastActivityAt, so a bare heartbeat does not prevent an idle-yield (ADR 0003). Like every Scheduler
	// entry it returns the decisions to emit; a heartbeat emits none (no log, no reply), so this is empty.
	/**
	 * @param {number | string} connId The connection the request came from.
	 * @param {number} now The wall-clock instant, in ms since the epoch.
	 * @returns {Decision[]} The decisions to emit.
	 */
	heartbeat(connId, now) {
		for (const lease of this.#connLeases.get(connId) ?? []) {
			refreshExpiry(lease, this.#queueFor(lease.queueKey), now);
		}
		return [];
	}

	/**
	 * @param {number | string} _connId The connection the request came from; a release frees the session's Lease, whichever connection holds it.
	 * @param {string} sessionKey The session that releases.
	 * @param {string} queueKey The Queue the session holds in.
	 * @param {number} now The wall-clock instant, in ms since the epoch.
	 * @returns {Decision[]} The decisions to emit.
	 */
	release(_connId, sessionKey, queueKey, now) {
		/** @type {Decision[]} */
		const decisions = [];
		const queue = this.#queues.get(queueKey);
		if (queue) this.#releaseLease(queue, sessionKey, undefined, now, decisions);
		return decisions;
	}

	/**
	 * @param {number | string} connId The connection the request came from.
	 * @param {string} sessionKey The session that cancels.
	 * @param {string} queueKey The Queue the session is queued in.
	 * @param {number} now The wall-clock instant, in ms since the epoch.
	 * @returns {Decision[]} The decisions to emit.
	 */
	cancel(connId, sessionKey, queueKey, now) {
		/** @type {Decision[]} */
		const decisions = [];
		const queue = this.#queues.get(queueKey);
		if (queue) {
			const w = queue.waiting.find((x) => x.sessionKey === sessionKey && x.connId === connId);
			if (w) {
				remove(queue.waiting, w);
				remove(this.#connQueued.get(connId) ?? [], w);
			}
			if (this.#leaseOf(queue, sessionKey)) this.#releaseLease(queue, sessionKey, undefined, now, decisions);
		}
		return decisions;
	}

	// retier (/queue promote|demote): re-tier a live queued waiter in place (no reply, like cancel).
	/**
	 * @param {number | string} connId The connection the request came from.
	 * @param {string} sessionKey The session that is re-tiered.
	 * @param {string} tier The new priority class.
	 */
	retier(connId, sessionKey, tier) {
		tier = tier === "child" ? "child" : "parent";
		for (const queue of this.#queues.values()) {
			const w = queue.waiting.find((x) => x.sessionKey === sessionKey && x.connId === connId);
			if (w) {
				w.tier = tier;
				break;
			}
		}
	}

	// disconnect: a close (or a `bye`) is an abort — release every lease it held, drop every queued
	// request. Replaces the daemon's cleanup/bye. The adapter decrements the Counted-client count AFTER
	// this returns. Iterate copies: the release splices the conn lease list, and a client can hold
	// several leases.
	/**
	 * @param {number | string} connId The connection that went away.
	 * @param {number} now The wall-clock instant, in ms since the epoch.
	 * @returns {Decision[]} The decisions to emit.
	 */
	disconnect(connId, now) {
		/** @type {Decision[]} */
		const decisions = [];
		for (const lease of [...(this.#connLeases.get(connId) ?? [])]) this.#releaseLease(this.#queueFor(lease.queueKey), lease.sessionKey, undefined, now, decisions);
		for (const w of [...(this.#connQueued.get(connId) ?? [])]) {
			remove(this.#queueFor(w.queueKey).waiting, w);
			remove(this.#connQueued.get(connId) ?? [], w);
		}
		this.#connLeases.delete(connId);
		this.#connQueued.delete(connId);
		return decisions;
	}

	// The single release primitive. The log names the cause: a TTL reclaim is an evict — the only
	// eviction reason after ADR 0002 — a yield is a release with reason "idle", a voluntary return a
	// bare release. Then hand the Slot on.
	/**
	 * @param {Queue} queue The queue.
	 * @param {string} sessionKey The session that releases.
	 * @param {string | undefined} reason The cause of the release ("ttl", "idle" or nothing).
	 * @param {number} now The wall-clock instant, in ms since the epoch.
	 * @param {Decision[]} decisions The decisions to append the release to.
	 */
	#releaseLease(queue, sessionKey, reason, now, decisions) {
		const lease = this.#leaseOf(queue, sessionKey);
		if (lease) {
			remove(queue.holders, lease);
			remove(this.#connLeases.get(lease.connId) ?? [], lease);
			if (reason === "ttl") decisions.push({ type: "evict", queueKey: queue.queueKey, sessionKey, tier: lease.tier, reason: "ttl" });
			else decisions.push({ type: "release", queueKey: queue.queueKey, sessionKey, tier: lease.tier, reason });
		}
		this.#grantNext(queue, now, decisions);
	}

	// sweepAt: TTL evict (unconditional, so `status` never reports a ghost holder) then idle-yield
	// (gated on the lease's own window being above zero, Waiters present, and the activity flag —
	// ADR 0003).
	/**
	 * @param {number} now The wall-clock instant, in ms since the epoch.
	 * @returns {Decision[]} The decisions to emit.
	 */
	sweepAt(now) {
		/** @type {Decision[]} */
		const decisions = [];
		for (const queue of this.#queues.values()) {
			for (const lease of [...queue.holders]) if (now > lease.expires) this.#releaseLease(queue, lease.sessionKey, "ttl", now, decisions);
			// Idle-yield: an activity-flagged holder silent past its window with waiters present yields
			// the slot. A lone holder is never disturbed; unflagged (old-client) leases are never yielded.
			if (queue.waiting.length > 0) {
				for (const lease of [...queue.holders]) {
					if (lease.terms.idleYieldMs > 0 && lease.activity && now - lease.lastActivityAt > lease.terms.idleYieldMs)
						this.#releaseLease(queue, lease.sessionKey, "idle", now, decisions);
				}
			}
		}
		return decisions;
	}

	// #redateLease: a rebuild re-dates a carried Holder's Lease under the Queue's terms. A shortened
	// TTL leaves the Lease untouched — the Holder keeps the terms it was granted under (ADR 0002) —
	// and a lengthened one applies at once: the term grows and the expiry follows it (#104).
	/**
	 * @param {Queue} queue The Queue the lease is carried to.
	 * @param {Lease} lease The carried lease.
	 * @param {number} now The wall-clock instant, in ms since the epoch.
	 */
	#redateLease(queue, lease, now) {
		if (queue.terms.leaseTtlMs > lease.terms.leaseTtlMs) refreshExpiry(lease, queue, now);
	}

	// rebuildQueues: the Queue rebuild of a config reload (#103). The Scheduler owns it end to end —
	// it iterates its own Queues, asks the second resolver where each one lives after the reload,
	// and carries that Queue's Holders, Waiters and switch count to the target key. The target's
	// rules are re-resolved (the policy the reload brought), every carried Holder's activity clock
	// restarts, and the Queues left over capacity are reported back to the adapter.
	//
	// The Adapter supplies the Addressing rule as the second resolver and never sees the Queues
	// themselves: the rule stays in src/addressing.js, which never sees Scheduler state (ADR 0005).
	/**
	 * @param {number} now The wall-clock instant, in ms since the epoch.
	 * @returns {{ decisions: Decision[], overCapacity: OverCapacityReport[] }} The decisions to emit and the over-capacity reports.
	 */
	rebuildQueues(now) {
		/** @type {Map<string, Queue>} */
		const rebuilt = new Map();
		for (const [key, queue] of this.#queues) {
			const target = this.#resolveQueueKey(key);
			let moved = rebuilt.get(target);
			if (!moved) rebuilt.set(target, (moved = this.#newQueue(target)));
			moved.switches += queue.switches; // a rebuild (group merge) carries the switch counts with it
			for (const lease of queue.holders) {
				lease.queueKey = target;
				// The terms are the Scheduler's: a policy edit never shortens a held Lease, and a
				// lengthened TTL applies at once (#104).
				this.#redateLease(moved, lease, now);
				// A policy edit starts every carried holder's activity clock afresh: the idle-yield
				// window a Lease is held under does not change (#105), so the fresh clock is the grace
				// that keeps a rebuild from yielding a holder on the spot (#81).
				lease.lastActivityAt = now;
				moved.holders.push(lease);
			}
			for (const waiter of queue.waiting) {
				waiter.queueKey = target;
				moved.waiting.push(waiter);
			}
		}
		// No-preemption (#49): the merged queue may start with more holders than its new maxStreams.
		// Report that transient over-capacity rather than exceed the limit silently (#82). The report
		// is taken before the grants below, so it counts the holders the move carried over.
		/** @type {OverCapacityReport[]} */
		const overCapacity = [];
		for (const queue of rebuilt.values()) {
			if (queue.holders.length > queue.maxStreams) overCapacity.push({ queueKey: queue.queueKey, holders: queue.holders.length, maxStreams: queue.maxStreams });
		}
		this.#queues.clear();
		for (const [key, queue] of rebuilt) this.#queues.set(key, queue);
		/** @type {Decision[]} */
		const decisions = [];
		for (const queue of this.#queues.values()) this.#grantNext(queue, now, decisions);
		return { decisions, overCapacity };
	}

	// snapshot: the raw queue state for the adapter's `status` view. It returns the live Holder (Lease)
	// and Waiter records plus the counters the status op needs (maxStreams, switches); it does NOT
	// build the wire shape. The adapter maps each record to the wire shape (sessionView, maxStreams
	// Infinity -> JSON null) so the Scheduler never serialises (ADR 0004: the status view is adapter-side).
	/**
	 * @returns {QueueSnapshot[]} The raw queue state.
	 */
	snapshot() {
		return [...this.#queues.values()].map((q) => ({
			queueKey: q.queueKey,
			maxStreams: q.maxStreams,
			switches: q.switches,
			holders: q.holders,
			waiting: q.waiting,
		}));
	}
}

/**
 * @param {any[]} list
 * @param {any} item
 */
function remove(list, item) {
	const at = list.indexOf(item);
	if (at >= 0) list.splice(at, 1);
}

// Refresh a Lease's expiry under the terms module's rule (ADR 0009): the TTL term grows to the
// longer of the Lease's own and the Queue's current one — a policy edit never shortens a Lease
// that is already held (ADR 0002) — and the expiry follows it. The idle-yield window stays the
// one the Lease was granted under.
/**
 * @param {Lease} lease
 * @param {Queue} queue
 * @param {number} now
 */
function refreshExpiry(lease, queue, now) {
	lease.terms = growTerms(lease.terms, queue.terms);
	lease.expires = now + lease.terms.leaseTtlMs;
}
