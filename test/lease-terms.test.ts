// The Lease's terms module (ADR 0009): the shape, the refresh period they imply, the wire
// encoding and the growth rule. No daemon and no socket: the rules are reachable through the
// module's own interface.
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { growTerms, periodForTerms, termsFor, termsFromWire, termsToWire } from "../src/lease-terms.js";

describe("the refresh period a Lease's terms imply", () => {
	it("refreshes at a third of the TTL", () => {
		assert.equal(periodForTerms(termsFor(30000, 0)), 10000);
		assert.equal(periodForTerms(termsFor(1000, 0)), 333);
	});

	it("floors the period at 250 ms, so it stays under the TTL floor", () => {
		// At the TTL floor (1000 ms) a 1 s period would equal the TTL, and a sweep landing between
		// two refreshes would evict a healthy Holder.
		assert.equal(periodForTerms(termsFor(750, 0)), 250);
	});

	it("bounds the period by half the idle-yield window", () => {
		assert.equal(periodForTerms(termsFor(30000, 1000)), 500, "a short idle window needs a refresh at half of it");
		assert.equal(periodForTerms(termsFor(1000, 30000)), 333, "a short TTL governs the period");
		assert.equal(periodForTerms(termsFor(30000, 30000)), 10000, "the TTL's third is already tighter than half the window");
	});

	it("imposes no bound when the idle-yield window is disabled", () => {
		assert.equal(periodForTerms(termsFor(30000, 0)), 10000);
	});
});

describe("the growth rule", () => {
	it("keeps the longer TTL of the two", () => {
		// A policy edit never shortens a Lease that is already held (ADR 0002): a lengthened TTL
		// applies at once, a shortened one at the next Grant.
		const granted = termsFor(1000, 30000);
		assert.equal(growTerms(granted, termsFor(60000, 30000)).leaseTtlMs, 60000, "a lengthened TTL applies at once");
		assert.equal(growTerms(granted, termsFor(500, 30000)).leaseTtlMs, 1000, "a shortened TTL never shortens a held Lease");
	});

	it("keeps the idle-yield window the Lease was granted under", () => {
		// A window the daemon lengthened under a live Holder would leave that Holder's client
		// refreshing slower than the window allows (#105).
		const granted = termsFor(1000, 300);
		assert.equal(growTerms(granted, termsFor(1000, 30000)).idleYieldMs, 300, "a lengthened window applies at the next Grant");
		assert.equal(growTerms(granted, termsFor(1000, 100)).idleYieldMs, 300, "a shortened window applies at the next Grant");
	});
});

describe("the wire encoding", () => {
	it("writes the terms under their wire names", () => {
		assert.deepEqual(termsToWire(termsFor(30000, 30000)), { leaseTtlMs: 30000, idleYieldMs: 30000 });
	});

	it("reads the terms back from the wire", () => {
		assert.deepEqual(termsFromWire({ op: "granted", queueKey: "q", sessionKey: "s", leaseTtlMs: 30000, idleYieldMs: 1000 }), {
			leaseTtlMs: 30000,
			idleYieldMs: 1000,
		});
	});

	it("reads no terms from a message that carries none", () => {
		// A daemon that sends no terms (an older one) leaves the client on its own policy: the
		// terms are never guessed.
		assert.equal(termsFromWire({ op: "granted", queueKey: "q", sessionKey: "s" }), null);
		assert.equal(termsFromWire({ leaseTtlMs: 30000 }), null, "both fields, or none");
		assert.equal(termsFromWire({ leaseTtlMs: 0, idleYieldMs: 1000 }), null, "a TTL of zero is no TTL");
		assert.equal(termsFromWire({ leaseTtlMs: 30000, idleYieldMs: -1 }), null, "a negative window is no window");
		assert.equal(termsFromWire(undefined), null);
	});

	it("round-trips the terms through the wire", () => {
		const terms = termsFor(30000, 1000);
		assert.deepEqual(termsFromWire(termsToWire(terms)), terms);
	});
});
