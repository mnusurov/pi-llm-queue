// The Decision vocabulary and its effects (ADR 0010, GLOSSARY "Decision"): the shape the
// Scheduler's answers take, the event-log line each one becomes, the `granted` reply, and the
// order a batch of them is emitted in. One module owns the mapping, so the wire and the event log
// cannot drift from the decisions that caused them.
//
// Plain JS on purpose (the src/config.js reason): the daemon imports it and Node's built-in type
// stripping is refused for files under node_modules, so a .ts import would kill an installed daemon.
//
// The module touches no filesystem and no socket: the log sink and the socket are injected, and so
// is the clock. The Scheduler's word for a session's priority class is `tier`; the wire's word is
// `role`. The rename happens here, at the effect, and nowhere else.
//
// What the module does not own: the status view (a projection of the Queue's state, not of a
// Decision), the config, the log file and the socket.

import { termsToWire } from "./lease-terms.js";

/**
 * The Scheduler's answer about one Queue event: what happened, to which session, in which Queue.
 * The Scheduler writes them, the Adapter emits them, and this module is the only place their shape
 * is written down.
 *
 * @typedef {object} Decision
 * @property {"grant" | "switch" | "queued" | "release" | "evict"} type The kind of answer.
 * @property {number | string} [connId] The connection the decision belongs to. Absent when the decision names no actor — a sweep's decisions belong to the Queue, not to a connection.
 * @property {string} queueKey The Queue the decision is about.
 * @property {string} sessionKey The session the decision is about.
 * @property {string} tier The session's priority class (`parent` or `child`).
 * @property {string} [requestedKey] The key the client asked with; a grant echoes it back.
 * @property {string} [reason] The cause of a release or an evict (`ttl`, `idle`).
 * @property {import("./lease-terms.js").LeaseTerms} [terms] The Lease's terms a grant carries (ADR 0009).
 */

/**
 * The Scheduler's other output shape: a Queue left with more holders than its new capacity allows
 * (no-preemption, #49/#82). The report is the `config` event's shape.
 *
 * @typedef {object} OverCapacityReport
 * @property {string} queueKey The Queue that is over capacity.
 * @property {number} holders How many holders it carries.
 * @property {number} maxStreams How many it may carry.
 */

/**
 * One line of the event log: the queue event, the session it is about, the session's role, and the
 * wall-clock instant it happened at.
 *
 * @typedef {object} EventLine
 * @property {string} event The event name.
 * @property {string} queueKey The Queue the event is about.
 * @property {string} sessionKey The session the event is about.
 * @property {string} role The session's priority class, under the wire's name.
 * @property {string} ts The wall-clock instant, as an ISO timestamp.
 * @property {string} [reason] The cause of a release or an evict.
 */

/**
 * The connection a decision belongs to, or null when it names none. A sweep's decisions name no
 * actor: they belong to the Queue. Both adapters file a decision by this rule instead of
 * re-deriving it.
 *
 * @param {Decision} decision The decision.
 * @returns {number | string | null} The connection's id, or null when the decision names none.
 */
export const decisionOwner = (decision) => decision.connId ?? null;

/**
 * The event-log line a decision becomes. The Scheduler's `tier` is renamed to the wire's `role` on
 * the way out: the two words meet here, at the effect, and nowhere else.
 *
 * @param {Decision} decision The decision.
 * @param {number} now The wall-clock instant of the event, in ms since the epoch.
 * @returns {EventLine} The line.
 */
export const decisionLogLine = (decision, now) => {
	const line = {
		event: decision.type,
		queueKey: decision.queueKey,
		sessionKey: decision.sessionKey,
		role: decision.tier,
		ts: new Date(now).toISOString(),
	};
	if (decision.reason) line.reason = decision.reason;
	return line;
};

/**
 * The `granted` reply: the grant itself, plus the Lease's terms (#105). One builder, two callers:
 * the grant path takes the terms off the Decision, and the refresh path — a re-acquire of a held
 * Slot, which produces a reply and no Decision — takes them off the acquire result.
 *
 * @param {string} queueKey The key the client asked with, echoed back.
 * @param {string} sessionKey The session that holds the Slot.
 * @param {import("./lease-terms.js").LeaseTerms} terms The Lease's terms.
 * @returns {{ op: "granted", queueKey: string, sessionKey: string, leaseTtlMs: number, idleYieldMs: number }} The reply.
 */
export const grantedReply = (queueKey, sessionKey, terms) => ({
	op: "granted",
	queueKey,
	sessionKey,
	...termsToWire(terms),
});

/**
 * The `config` event line of an over-capacity report: the Queue that is over capacity, how far
 * over it is, and why the line is there.
 *
 * @param {OverCapacityReport} report The report.
 * @param {number} now The wall-clock instant of the event, in ms since the epoch.
 * @returns {EventLine & { holders: number, maxStreams: number }} The line.
 */
export const overCapacityLine = (report, now) => ({
	event: "config",
	queueKey: report.queueKey,
	reason: "over-capacity",
	holders: report.holders,
	maxStreams: report.maxStreams,
	ts: new Date(now).toISOString(),
});

/**
 * Emit a batch of decisions: every decision becomes a log line, and a grant becomes a `granted`
 * reply.
 *
 * The batch order is the promise: every line is handed to the writer before any reply is handed to
 * the sender. A handover is two decisions — the grant and the switch it causes — and the reply
 * belongs to both, so a reply that left between their two lines would let a reader see a grant
 * with no switch (ADR 0004's amendment, #47).
 *
 * The clock is a function, and it is called per line: every line keeps its own timestamp, exactly
 * as it did before.
 *
 * @param {Decision[]} decisions The decisions to emit, in order.
 * @param {{ write: (line: EventLine) => void, send: (connId: number | string, msg: object) => void, now: () => number }} effects The log sink, the socket and the clock, injected.
 */
export const emit = (decisions, { write, send, now }) => {
	const replies = [];
	for (const decision of decisions) {
		write(decisionLogLine(decision, now()));
		if (decision.type !== "grant") continue;
		const owner = decisionOwner(decision);
		if (owner === null) continue;
		// The reply echoes the key the request asked with (the per-request `requestedKey` the
		// Decision carries), falling back to the resolved queue key for a direct grant.
		replies.push([owner, grantedReply(decision.requestedKey ?? decision.queueKey, decision.sessionKey, decision.terms)]);
	}
	for (const [connId, msg] of replies) send(connId, msg);
};
