// The Lease's terms decide the refresh (#105): the client refreshes on the terms the daemon granted
// the Lease under, never on its own copy of the config. The daemon here serves a config of its own,
// so the two copies disagree — the divergence that used to cost a live Holder its Slot. Real daemon
// + stub api + the pi Extension harness (ADR 0006).
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { closeAllClients, disposeAllExtensions, firstEvent, QUEUE_KEY, race, sleep, startExtension } from "./extension-harness.ts";

const STUB_API = "stub-terms-api";

// The daemon enforces a 1 s TTL and no idle-yield; the Extension's own copy of the config says a
// 60 s TTL — the edit the daemon never saw. A client that refreshed on its own copy would beat
// every 20 s and lose its Lease after the daemon's 1 s TTL.
const DAEMON_CONFIG = {
	defaults: { maxStreams: 1, leaseTtlMs: 1000, idleYieldMs: 0, daemonIdleExitMs: 1000000000 },
	queues: {},
};
const CLIENT_CONFIG = {
	defaults: { maxStreams: 1, leaseTtlMs: 60000, idleYieldMs: 30000, daemonIdleExitMs: 1000000000 },
	queues: { [QUEUE_KEY]: { maxStreams: 1 } },
};

const MODELS = { stub: { baseUrl: QUEUE_KEY, api: STUB_API, models: [{ id: "m" }] } };
const model = { api: STUB_API, baseUrl: QUEUE_KEY, provider: "stub", id: "m" } as any;

const startExt = () => startExtension({ config: CLIENT_CONFIG, daemonConfig: DAEMON_CONFIG, models: MODELS, daemon: { sweepMs: 100 } });

describe("the Lease's terms decide the refresh (#105)", () => {
	it("keeps a live Holder's Lease when the Extension's copy of the config disagrees with the daemon's", async () => {
		const ext = startExt();
		const holder = ext.startSession("TERMS-A");
		await holder.dispatch("agent_start"); // holds the queue under the daemon's 1 s TTL

		const waiter = ext.startSession("TERMS-B");
		const eB = firstEvent(waiter.stream(model));
		await sleep(2000); // 2× the TTL the daemon enforces: a client on its own copy (60 s) lapses here

		const q = await ext.status(QUEUE_KEY);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["TERMS-A"], "the Holder keeps its Slot: the refresh follows the granted terms");
		assert.deepEqual(q.waiting.map((w: any) => w.sessionKey), ["TERMS-B"], "the waiter stays queued");
		assert.equal(await race(eB, 300), "blocked", "the waiter never streams beside the Holder");

		await holder.dispatch("session_shutdown");
		assert.equal(await race(eB, 3000), "done", "the waiter proceeds once the Holder releases");
		await waiter.dispatch("session_shutdown");
	});

	it("keeps the Lease alive when the client reloads a config edit the daemon never saw", async () => {
		const ext = startExt();
		const holder = ext.startSession("RELOAD-A");
		await holder.dispatch("agent_start");

		const waiter = ext.startSession("RELOAD-B");
		const eB = firstEvent(waiter.stream(model));

		// The client sees an edit the daemon does not: its own copy's TTL is lengthened, so a client
		// that refreshed on its own copy would slow below the TTL the daemon enforces.
		const edited = { ...CLIENT_CONFIG, defaults: { ...CLIENT_CONFIG.defaults, leaseTtlMs: 300000 } };
		// fs.watch delivery latency varies with system load (macOS FSEvents coalesces), so re-save
		// while waiting for the reload. The saves are 500 ms apart: the watch debounces 250 ms, and a
		// faster save would keep resetting the debounce.
		for (let waited = 0; waited < 10000; waited += 500) {
			if (holder.notifications().some((n) => /config reloaded/.test(n.message))) break;
			ext.writeConfig(edited);
			await sleep(500);
		}
		assert.ok(holder.notifications().some((n) => /config reloaded/.test(n.message)), "the client reloaded its own copy of the config");

		await sleep(2000); // 2× the TTL the daemon enforces: the reload must not have slowed the refresh
		const q = await ext.status(QUEUE_KEY);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["RELOAD-A"], "the Holder keeps its Slot: the reload did not slow its refresh");
		assert.deepEqual(q.waiting.map((w: any) => w.sessionKey), ["RELOAD-B"], "the waiter stays queued");

		await holder.dispatch("session_shutdown");
		assert.equal(await race(eB, 3000), "done", "the waiter proceeds once the Holder releases");
		await waiter.dispatch("session_shutdown");
	});

	it("covers a Holder whose keyed Beat is paused with the connection-wide heartbeat, at the granted terms", async () => {
		const ext = startExt();
		const holder = ext.startSession("PARKED-A");
		await holder.dispatch("agent_start");
		// The keyed Beat pauses while the Run waits on another agent, so only the connection-wide
		// heartbeat can keep the Lease alive.
		await holder.dispatch("tool_execution_start", { toolCallId: "t1", toolName: "subagent", args: {} });

		const waiter = ext.startSession("PARKED-B");
		const eB = firstEvent(waiter.stream(model));
		await sleep(2000); // 2× the TTL the daemon enforces

		const q = await ext.status(QUEUE_KEY);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["PARKED-A"], "the parked Holder keeps its Slot: the bare heartbeat follows the granted terms");
		assert.deepEqual(q.waiting.map((w: any) => w.sessionKey), ["PARKED-B"], "the waiter stays queued");

		await holder.dispatch("tool_execution_end", { toolCallId: "t1", toolName: "subagent", result: {}, isError: false });
		await holder.dispatch("session_shutdown");
		assert.equal(await race(eB, 3000), "done", "the waiter proceeds once the Holder releases");
		await waiter.dispatch("session_shutdown");
	});
});

after(() => {
	closeAllClients();
	disposeAllExtensions();
});
