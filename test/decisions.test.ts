// The Decision vocabulary and its effects (ADR 0010): the module that owns the shape the
// Scheduler answers with, the event-log line each answer becomes, the `granted` reply, and the
// order a batch is emitted in. The suite drives the module through its interface — no daemon, no
// socket, no filesystem — so the rules are asserted once, at their owner.
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { decisionLogLine, decisionOwner, emit, grantedReply, overCapacityLine } from "../src/decisions.js";
import type { Decision, EventLine } from "../src/decisions.js";

const TERMS = { leaseTtlMs: 30000, idleYieldMs: 100 };

const grant = (over: Partial<Decision> = {}): Decision => ({
	type: "grant",
	connId: "c1",
	queueKey: "http://model.test/v1",
	sessionKey: "A",
	tier: "parent",
	terms: TERMS,
	...over,
});

describe("the Decision vocabulary", () => {
	it("names the connection a decision belongs to", () => {
		assert.equal(decisionOwner(grant()), "c1");
	});

	it("names no connection for a decision that has no actor", () => {
		// A sweep's decisions belong to the Queue, not to a connection.
		assert.equal(decisionOwner({ type: "evict", queueKey: "q", sessionKey: "A", tier: "parent", reason: "ttl" }), null);
	});
});

describe("the event-log line", () => {
	it("names the event, the queue, the session and the role", () => {
		assert.deepEqual(decisionLogLine(grant(), 1_700_000_000_000), {
			event: "grant",
			queueKey: "http://model.test/v1",
			sessionKey: "A",
			role: "parent",
			ts: "2023-11-14T22:13:20.000Z",
		});
	});

	it("renames the Scheduler's tier to the wire's role", () => {
		const line = decisionLogLine(grant({ tier: "child" }), 0);
		assert.equal(line.role, "child", "the wire's word for the tier is `role`");
		assert.equal((line as Record<string, unknown>).tier, undefined, "the Scheduler's word does not travel to the wire");
	});

	it("carries the reason of a release and of an evict", () => {
		assert.equal(decisionLogLine({ type: "release", queueKey: "q", sessionKey: "A", tier: "parent", reason: "idle" }, 0).reason, "idle");
		assert.equal(decisionLogLine({ type: "evict", queueKey: "q", sessionKey: "A", tier: "parent", reason: "ttl" }, 0).reason, "ttl");
	});

	it("carries no reason when the event has none", () => {
		const line = decisionLogLine({ type: "queued", queueKey: "q", sessionKey: "A", tier: "parent" }, 0);
		assert.equal("reason" in line, false, "a queued event has no cause to name");
	});

	it("stamps every line with the instant it was given", () => {
		assert.equal(decisionLogLine(grant(), 0).ts, "1970-01-01T00:00:00.000Z");
		assert.equal(decisionLogLine(grant(), 1000).ts, "1970-01-01T00:00:01.000Z");
	});
});

describe("the granted reply", () => {
	it("carries the grant, the session and the Lease's terms", () => {
		assert.deepEqual(grantedReply("http://model.test/v1", "A", TERMS), {
			op: "granted",
			queueKey: "http://model.test/v1",
			sessionKey: "A",
			leaseTtlMs: 30000,
			idleYieldMs: 100,
		});
	});
});

describe("the over-capacity line", () => {
	it("names the queue, the cause and how far over it is", () => {
		assert.deepEqual(overCapacityLine({ queueKey: "q", holders: 3, maxStreams: 1 }, 0), {
			event: "config",
			queueKey: "q",
			reason: "over-capacity",
			holders: 3,
			maxStreams: 1,
			ts: "1970-01-01T00:00:00.000Z",
		});
	});
});

describe("emitting a batch", () => {
	// The trace is the call order: a line is written before a reply is sent, and the batch's lines
	// come out in the order the decisions came in.
	const traceOf = (decisions: Decision[]) => {
		const trace: string[] = [];
		const lines: EventLine[] = [];
		const replies: { connId: number | string; msg: Record<string, unknown> }[] = [];
		emit(decisions, {
			write: (line) => {
				trace.push(`write:${line.event}`);
				lines.push(line);
			},
			send: (connId, msg) => {
				trace.push(`send:${(msg as Record<string, unknown>).op}`);
				replies.push({ connId, msg: msg as Record<string, unknown> });
			},
			now: () => 0,
		});
		return { trace, lines, replies };
	};

	it("writes a line for every decision", () => {
		const { lines } = traceOf([
			grant(),
			{ type: "switch", queueKey: "q", sessionKey: "A", tier: "parent" },
			{ type: "queued", connId: "c2", queueKey: "q", sessionKey: "B", tier: "child" },
			{ type: "release", queueKey: "q", sessionKey: "A", tier: "parent" },
			{ type: "evict", queueKey: "q", sessionKey: "A", tier: "parent", reason: "ttl" },
		]);
		assert.deepEqual(lines.map((line) => line.event), ["grant", "switch", "queued", "release", "evict"]);
	});

	it("sends a reply only for a grant", () => {
		const { replies } = traceOf([
			{ type: "switch", queueKey: "q", sessionKey: "A", tier: "parent" },
			{ type: "queued", connId: "c2", queueKey: "q", sessionKey: "B", tier: "child" },
			{ type: "release", queueKey: "q", sessionKey: "A", tier: "parent" },
		]);
		assert.deepEqual(replies, [], "no decision but a grant produces a reply");
	});

	it("sends the grant's reply to the connection the decision names", () => {
		const { replies } = traceOf([grant({ connId: "c7" })]);
		assert.equal(replies.length, 1);
		assert.equal(replies[0].connId, "c7");
	});

	it("echoes the key the client asked with", () => {
		const { replies } = traceOf([grant({ requestedKey: "provider-name" })]);
		assert.equal(replies[0].msg.queueKey, "provider-name", "the reply carries the key the request asked with");
	});

	it("echoes the resolved queue key when the request named none", () => {
		const { replies } = traceOf([grant()]);
		assert.equal(replies[0].msg.queueKey, "http://model.test/v1");
	});

	it("carries the Lease's terms on the reply", () => {
		const { replies } = traceOf([grant()]);
		assert.equal(replies[0].msg.leaseTtlMs, 30000);
		assert.equal(replies[0].msg.idleYieldMs, 100);
	});

	// The promise the event log makes: every event a reply caused is on disk before that reply goes
	// out. A handover is two decisions — the grant and the switch it causes — so the reply must not
	// leave between their two lines.
	it("writes every line of the batch before it sends any reply", () => {
		const { trace } = traceOf([
			grant(),
			{ type: "switch", queueKey: "q", sessionKey: "A", tier: "parent" },
		]);
		assert.deepEqual(trace, ["write:grant", "write:switch", "send:granted"]);
	});

	it("stamps every line with its own instant", () => {
		let clock = 0;
		const lines: EventLine[] = [];
		emit([grant(), grant({ sessionKey: "B" })], {
			write: (line) => lines.push(line),
			send: () => {},
			now: () => (clock += 1000),
		});
		assert.deepEqual(lines.map((line) => line.ts), ["1970-01-01T00:00:01.000Z", "1970-01-01T00:00:02.000Z"]);
	});
});
