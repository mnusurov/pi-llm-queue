// The Scheduler's interface: the ten entries its callers and tests cross, and nothing else. The
// internals are private by construction (#115) — a caller that reached past an entry into a
// Queue's state broke it silently (a picked Waiter that stayed in the waiting list, an Aging
// streak that was not bumped), so the class forbids the call rather than trusting it.
//
// The surface is the contract, so it is asserted: a member that leaks back into it fails here.
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { Scheduler } from "../src/scheduler.js";
import { SchedulerHarness } from "./scheduler-harness.ts";

// The entries the Scheduler's callers cross.
const ENTRIES = ["acquire", "beat", "cancel", "disconnect", "heartbeat", "rebuildQueues", "release", "retier", "snapshot", "sweepAt"];

describe("the Scheduler's interface", () => {
	it("exposes the ten entries and nothing else", () => {
		const surface = Object.getOwnPropertyNames(Scheduler.prototype).filter((name) => name !== "constructor");
		assert.deepEqual(surface.sort(), [...ENTRIES].sort());
	});

	it("keeps its state off the instance", () => {
		// The Queue map, the connection scope and the two resolvers live in the class, not on the
		// object a caller holds.
		assert.deepEqual(Object.getOwnPropertyNames(new SchedulerHarness().scheduler), []);
	});
});
