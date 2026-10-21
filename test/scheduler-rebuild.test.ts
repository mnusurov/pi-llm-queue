// The Queue rebuild of a config reload (#103), in memory: the Scheduler owns it end to end. The Adapter
// asks Addressing for the rule (the Queue key a Queue lives under after the reload) and the
// Scheduler applies it — the Adapter never enumerates the Queues. Asserted through the Scheduler's
// interface: no config watcher, no socket, no debounce sleep. The socket-level hot-reload suites
// stay the integration oracle (test/daemon.test.ts).
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { QUEUE_KEY, QUEUE_KEY2, SchedulerHarness } from "./scheduler-harness.ts";
import type { Policy } from "./scheduler-harness.ts";

describe("the Queue rebuild (in memory)", () => {
	// The Addressing rule, in memory: the Queue key a Queue lives under after a config reload. The
	// driver takes it as a resolver, so the Scheduler asks for the target key itself (#103).
	const harnessWith = (remap: Record<string, string>, policy: Policy = { maxStreams: 1 }) =>
		new SchedulerHarness(policy, (queueKey) => remap[queueKey] ?? queueKey);

	it("moves a Queue to the key it lives under after a reload, carrying its Holders and Waiters", () => {
		const h = harnessWith({ [QUEUE_KEY]: "gpu" });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent", queueKey: QUEUE_KEY });
		h.acquire(b, "B", { role: "parent", queueKey: QUEUE_KEY });
		assert.deepEqual(h.waiters(QUEUE_KEY), ["B"], "B waits behind A");

		h.editPolicy({ maxStreams: 1 });

		assert.deepEqual(h.queueKeys(), ["gpu"], "the Queue lives under the group now");
		assert.deepEqual(h.holders("gpu"), ["A"], "the Holder is carried to the new key");
		assert.deepEqual(h.waiters("gpu"), ["B"], "the Waiter is carried to the new key");
		assert.deepEqual(h.holders(QUEUE_KEY), [], "the old key keeps no queue behind");
	});

	it("carries the switch count with the Queue", () => {
		const h = harnessWith({ [QUEUE_KEY]: "gpu" });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent", queueKey: QUEUE_KEY });
		h.acquire(b, "B", { role: "parent", queueKey: QUEUE_KEY }); // B waits
		h.release(a, "A", QUEUE_KEY); // the handover to a Waiter: one Switch
		assert.equal(h.switches(QUEUE_KEY), 1);
		assert.deepEqual(h.holders(QUEUE_KEY), ["B"]);

		h.editPolicy({ maxStreams: 1 });
		assert.equal(h.switches("gpu"), 1, "the rebuild carries the switch counts with it");
	});

	it("re-resolves the moved Queue's rules", () => {
		const h = harnessWith({ [QUEUE_KEY]: "gpu" });
		const a = h.connect();

		h.acquire(a, "A", { role: "parent", queueKey: QUEUE_KEY });
		h.editPolicy({ maxStreams: 3 });

		h.acquire(a, "B", { role: "parent", queueKey: "gpu" });
		h.acquire(a, "C", { role: "parent", queueKey: "gpu" });
		assert.deepEqual(h.holders("gpu"), ["A", "B", "C"], "the moved Queue answers to the reloaded rules");
	});

	it("grants whoever the new capacity frees", () => {
		const h = new SchedulerHarness({ maxStreams: 1 });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent", queueKey: QUEUE_KEY });
		h.acquire(b, "B", { role: "parent", queueKey: QUEUE_KEY });
		assert.deepEqual(h.grantsFor(b), [], "B waits behind A");

		h.editPolicy({ maxStreams: 2 });
		assert.deepEqual(h.grantedSessions(b), ["B"], "the rebuild grants whoever the new capacity frees");
	});

	it("reports a Queue left over capacity instead of exceeding the new limit silently", () => {
		// Two endpoint queues, one live holder each, merged into a group with one slot. No-preemption
		// means neither holder's lease can end: the transient over-capacity is reported, not hidden.
		const h = harnessWith({ [QUEUE_KEY]: "gpu", [QUEUE_KEY2]: "gpu" });
		const a = h.connect();
		const b = h.connect();

		h.acquire(a, "A", { role: "parent", queueKey: QUEUE_KEY });
		h.acquire(b, "B", { role: "parent", queueKey: QUEUE_KEY2 });

		const { overCapacity } = h.editPolicy({ maxStreams: 1 });

		assert.deepEqual(overCapacity, [{ queueKey: "gpu", holders: 2, maxStreams: 1 }], "the over-capacity is reported");
		assert.deepEqual(h.holders("gpu").sort(), ["A", "B"], "both holders stay: no-preemption cannot end a live lease");
	});
});
