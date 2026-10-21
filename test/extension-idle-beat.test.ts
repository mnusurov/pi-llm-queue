// Run-scoped keyed beat (#57): the extension beats for the whole live run — streaming and
// compute tools alike — and pauses only while the run sits in a blocking tool, so idle-yield
// fires for a Holder parked on another agent and never for one busy in a compute tool.
// Real daemon + stub api + the pi Extension harness (ADR 0006).
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { LeaseClient } from "../src/lease-client.ts";
import { closeAllClients, disposeAllExtensions, firstEvent, pollUntil, QUEUE_KEY, race, sleep, startExtension } from "./extension-harness.ts";

const STUB_API = "stub-idle-api";
const QUEUE2 = "http://model2.test/v1";

// idleYieldMs 1000 + sweep 200 ms: a beat gap shows up in ~1.0-1.2 s. leaseTtlMs 2000 and
// idleYieldMs 1000 put the client beat period at 500 ms (half the idle window, the terms module's
// period), so a run beats ~2× per idle window. The TTL
// floor here is the idle window, not config.js's 1000 ms: the TTL must stay ABOVE idleYieldMs with
// the sweep interval on top of it — at 2000 vs 1000 the idle-yield fires ~1.0-1.2 s into a silent
// Holder and the sweep evicts at ~2.0-2.2 s, so the blocking-tool test proves ADR 0003 and not a
// TTL evict; at the 1000 ms floor the two land on the same tick and the test proves the evict.
// daemonIdleExitMs 1e9: this suite reuses one daemon across tests, and each test's session_shutdown
// now says bye (#63) — with the production default 0 the last bye would kill the fixture between
// tests and the autostarted replacement would lose the queue state the next test asserts on.
const CONFIG = { defaults: { maxStreams: 1, idleYieldMs: 1000, leaseTtlMs: 2000, daemonIdleExitMs: 1000000000 }, queues: { stub: { maxStreams: 1 }, stub2: { maxStreams: 1 } } };
const MODELS = { stub: { baseUrl: QUEUE_KEY, api: STUB_API, models: [{ id: "m" }] }, stub2: { baseUrl: QUEUE2, api: STUB_API, models: [{ id: "m" }] } };

const startExt = () => startExtension({ config: CONFIG, models: MODELS, daemon: { sweepMs: 200 } });

const model = { api: STUB_API, baseUrl: QUEUE_KEY, provider: "stub", id: "m" } as any;
const model2 = { api: STUB_API, baseUrl: QUEUE2, provider: "stub2", id: "m" } as any;

describe("run-scoped keyed beat (idle-yield, #57)", () => {
	it("a compute tool longer than idleYieldMs keeps the beat and the Slot", async () => {
		const a = startExt();
		const b = startExt();
		const sa = a.startSession("CMP-A");
		const sb = b.startSession("CMP-B");
		await sa.dispatch("agent_start"); // grant → the run-scoped beat starts

		// A enters a compute tool (bash) and stays there 3× idleYieldMs with B queued behind it.
		await sa.dispatch("tool_execution_start", { toolCallId: "c1", toolName: "bash", args: { command: "make test" } });
		const eB = firstEvent(sb.stream(model));
		for (let i = 0; i < 3; i++) {
			await sleep(1000);
			const q = await a.status(QUEUE_KEY);
			assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["CMP-A"], `the compute-busy Holder keeps the Slot at ${1000 * (i + 1)} ms into the tool`);
			assert.deepEqual(q.waiting.map((w: any) => w.sessionKey), ["CMP-B"], "the waiter stays queued: beats flowed through the tool");
		}
		await sa.dispatch("tool_execution_end", { toolCallId: "c1", toolName: "bash", result: {}, isError: false });

		// A's next stream runs under the same lease: it streams at once and does not re-queue.
		const eA = firstEvent(sa.stream(model));
		assert.equal(await race(eA, 500), "done", "the Holder streams without re-queuing");
		const q = await a.status(QUEUE_KEY);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["CMP-A"], "A still holds the Slot");
		assert.deepEqual(q.waiting.map((w: any) => w.sessionKey), ["CMP-B"], "A never landed in the waiting list");
		assert.equal(await race(eB, 400), "blocked", "B stays behind the live run");

		await sa.dispatch("session_shutdown"); // A releases: B's turn
		assert.equal(await race(eB, 3000), "done", "B proceeds once A releases");
		await sb.dispatch("session_shutdown");
	});

	it("a blocking tool longer than idleYieldMs stops the beat and yields the Slot", async () => {
		const a = startExt();
		const b = startExt();
		const sa = a.startSession("BLK-A");
		const sb = b.startSession("BLK-B");
		await sa.dispatch("agent_start"); // grant → the run-scoped beat starts

		// A parks in `subagent` — the tool that waits on the very session queued behind it.
		await sa.dispatch("tool_execution_start", { toolCallId: "s1", toolName: "subagent", args: {} });
		const eB = firstEvent(sb.stream(model));
		assert.equal(await race(eB, 400), "blocked", "B queues behind the live Holder");
		assert.equal(await race(eB, 3000), "done", "A parked on its Child yields the Slot: B is granted");
		await sa.dispatch("tool_execution_end", { toolCallId: "s1", toolName: "subagent", result: {}, isError: false });

		// A's run lost the Slot: its next beat answers granted:false (beat stops) and its next
		// stream re-queues, proceeding once the slot frees — not streaming unqueued.
		const eA = firstEvent(sa.stream(model));
		assert.equal(await race(eA, 3000), "done", "the yielded run re-queues and proceeds when the slot frees");

		await sa.dispatch("session_shutdown");
		await sb.dispatch("session_shutdown");
	});

	it("a stream on a second queue does not destroy the run's beat on the first", async () => {
		// The beat is per held lease: a request that acquires a second queue must not replace the
		// run's beat, and its release must not stop it.
		const a = startExt();
		const b = startExt();
		const sa = a.startSession("MQ-A");
		const sb = b.startSession("MQ-B");
		await sa.dispatch("agent_start"); // run holds queue 1 → beat starts

		const raw = await a.connect();
		raw.send({ op: "acquire", queueKey: QUEUE2, sessionKey: "RAW-Q2", role: "parent" });
		assert.equal((await raw.next(3000)).op, "granted", "a raw client holds queue 2");

		const eB = firstEvent(sb.stream(model)); // queues behind the run on queue 1
		const eA2 = firstEvent(sa.stream(model2)); // queues behind RAW on queue 2
		// The run's queue-2 request must sit in the queue before RAW releases: poll for the enqueue, so
		// the release below can never overtake it.
		await pollUntil(async () => (await a.status(QUEUE2)).waiting.some((w: any) => w.sessionKey === "MQ-A"), 300, "waiting for the run's queue-2 request to enter the queue");
		raw.send({ op: "release", queueKey: QUEUE2, sessionKey: "RAW-Q2" }); // the run's queue-2 request lands
		assert.equal(await race(eA2, 3000), "done", "the queue-2 request streams and releases its own lease");
		raw.close();

		// 3× idleYieldMs after the queue-2 lease is gone: the run's queue-1 beat must have survived.
		for (let i = 0; i < 3; i++) {
			await sleep(1000);
			const q = await a.status(QUEUE_KEY);
			assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["MQ-A"], `the run keeps the queue-1 Slot at ${1000 * (i + 1)} ms`);
		}
		assert.equal(await race(eB, 400), "blocked", "the queue-1 waiter stays behind the live run");

		await sa.dispatch("session_shutdown");
		assert.equal(await race(eB, 3000), "done", "B proceeds once A releases");
		await sb.dispatch("session_shutdown");
	});

	it("clears the beat timer at the grace release: no beat after release", async () => {
		// Spy on the keyed beat: the run beat must stop when the grace release frees the Slot.
		const beats: string[] = [];
		const proto = LeaseClient.prototype as any;
		const orig = proto.beat;
		proto.beat = function (queueKey: string, sessionKey: string) {
			beats.push(sessionKey);
			return orig.call(this, queueKey, sessionKey);
		};
		try {
			const ext = startExt();
			const session = ext.startSession("TMR");
			await session.dispatch("agent_start"); // grant → beat starts
			// Poll to 2 beats: the period the terms ask for (leaseTtlMs 2000, idleYieldMs 1000) is 500 ms, so
			// the second lands ~1.0 s in. The bound is the 2500 ms the sleep spent; the assert below is the one it preceded.
			await pollUntil(() => beats.filter((s) => s === "TMR").length >= 2, 2500, "waiting for two beats from the live run");
			assert.ok(beats.filter((s) => s === "TMR").length >= 2, "the live run beats");
			beats.length = 0;
			await session.dispatch("agent_settled"); // grace 3000 ms → release
			// The release is a presence condition the daemon can be asked about: the Slot goes free at
			// GRACE_MS, a production constant. Poll it with the 3600 ms bound the sleep used.
			await pollUntil(async () => (await ext.status(QUEUE_KEY)).holders.length === 0, 3600, "waiting for the grace release to free the Slot");
			beats.length = 0;
			await sleep(2500); // absence window: 5 beat periods of quiet, the span this test always used
			assert.equal(beats.filter((s) => s === "TMR").length, 0, "no beat after the grace release: no timer leak");
			await session.dispatch("session_shutdown");
		} finally {
			proto.beat = orig;
		}
	});
});

after(() => {
	closeAllClients();
	disposeAllExtensions();
});
