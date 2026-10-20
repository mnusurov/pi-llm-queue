// queueTimeoutMs seam (#8, warn-only since ADR 0003 §6): a parent whose queue wait exceeds
// queueTimeoutMs gets a visible warning and stays queued — passthrough is removed. Own agent dir
// + config so queueTimeoutMs is set at extension import; stub api provider, real daemon, no real
// endpoint.
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { closeAllClients, disposeAllExtensions, firstEvent, QUEUE_KEY, sleep, startExtension } from "./extension-harness.ts";

const STUB_API = "stub-timeout-api";

// queueTimeoutMs 300 ms: a parent queued behind a long holder is warned past the cap, not released.
// daemonIdleExitMs 1e9: the suite reuses one daemon fixture; session_shutdown says bye (#63) and
// the production default 0 would kill the fixture between tests.
const CONFIG = { defaults: { maxStreams: 1, queueTimeoutMs: 300, daemonIdleExitMs: 1000000000 }, queues: { stub: { maxStreams: 1 } } };
const MODELS = { stub: { baseUrl: QUEUE_KEY, api: STUB_API, models: [{ id: "m" }] } };
// The tier a session queues as comes from the env contract: pin parent so this test observes the
// parent queueTimeoutMs warning wherever the suite runs (a lane running these tests is itself a
// subagent child, and a child waits unbounded by design — no warning fires).
const startExt = () => startExtension({ config: CONFIG, models: MODELS, env: { PI_QUEUE_ROLE: "parent" } });

const model = { api: STUB_API, baseUrl: QUEUE_KEY, provider: "stub", id: "m" } as any;

describe("queueTimeoutMs visible warning", () => {
	it("warns a queued parent past queueTimeoutMs and keeps it queued (no passthrough)", async () => {
		const a = startExt();
		const b = startExt();
		const sa = a.startSession("HOLDER");
		const sb = b.startSession("TIMEOUT");

		await sa.dispatch("agent_start"); // A holds the slot for its whole run

		const eB = firstEvent(sb.stream(model)); // B queues behind A
		const outcome = await Promise.race([eB.then((event) => event.type), sleep(2000).then(() => "blocked")]);

		assert.equal(outcome, "blocked", "B stays queued past queueTimeoutMs (warn-only, no passthrough)");
		const warning = sb.notifications().find((n) => n.level === "warning" && n.message.includes("queueTimeoutMs"));
		assert.ok(warning, "a visible warning is shown when the queue wait exceeds queueTimeoutMs");

		await sa.dispatch("session_shutdown");
		const granted = await Promise.race([eB.then((event) => event.type), sleep(3000).then(() => "blocked")]);
		assert.equal(granted, "start", "B is granted and streams once the slot frees");
	});
});

after(() => {
	closeAllClients();
	disposeAllExtensions();
});
