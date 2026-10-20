// Per-queue lease policy (#81): the client's beats must follow the queue's own leaseTtlMs and
// idleYieldMs, never `defaults` alone, and a hot-reload that shortens a TTL must restart the live
// beats. Real daemon + stub api + the pi Extension harness (ADR 0006).
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { closeAllClients, disposeAllExtensions, firstEvent, pollUntil, race, sleep, startExtension } from "./extension-harness.ts";

const STUB_API = "stub-per-queue-api";
const TTL_URL = "http://short-ttl.test/v1";
const IDLE_URL = "http://short-idle.test/v1";
const RELOAD_URL = "http://reload-ttl.test/v1";
const RELOAD_IDLE_URL = "http://reload-idle.test/v1";

const MODELS = {
	ttl: { baseUrl: TTL_URL, api: STUB_API, models: [{ id: "m" }] },
	idle: { baseUrl: IDLE_URL, api: STUB_API, models: [{ id: "m" }] },
	reload: { baseUrl: RELOAD_URL, api: STUB_API, models: [{ id: "m" }] },
	reloadIdle: { baseUrl: RELOAD_IDLE_URL, api: STUB_API, models: [{ id: "m" }] },
};

// defaults.leaseTtlMs 6000 puts the defaults-only beat at 2000 ms. Each queue below overrides one
// field so a beat derived from `defaults` alone is provably too slow for it.
const configFor = (reloadTtlMs = 6000, reloadIdleMs = 30000) => ({
	defaults: { maxStreams: 1, leaseTtlMs: 6000, idleYieldMs: 30000, daemonIdleExitMs: 1000000000 },
	queues: {
		[TTL_URL]: { maxStreams: 1, leaseTtlMs: 1000 },
		[IDLE_URL]: { maxStreams: 1, idleYieldMs: 1000 },
		[RELOAD_URL]: { maxStreams: 1, leaseTtlMs: reloadTtlMs },
		[RELOAD_IDLE_URL]: { maxStreams: 1, leaseTtlMs: 6000, idleYieldMs: reloadIdleMs },
	},
});

const startExt = (config = configFor()) => startExtension({ config, models: MODELS });

const modelFor = (provider: string, baseUrl: string) => ({ api: STUB_API, baseUrl, provider, id: "m" }) as any;

describe("per-queue lease policy (#81)", () => {
	it("beats at a queue's own leaseTtlMs, not the defaults' period", async () => {
		const a = startExt();
		const b = startExt();
		const sa = a.startSession("PQ-TTL-A", { model: modelFor("ttl", TTL_URL) });
		const sb = b.startSession("PQ-TTL-B", { model: modelFor("ttl", TTL_URL) });
		await sa.dispatch("agent_start"); // holds the 1000 ms-TTL queue

		const eB = firstEvent(sb.stream(modelFor("ttl", TTL_URL)));
		await sleep(2500); // > 2 queue TTLs: a 2 s defaults beat would have let the TTL lapse

		const q = await a.status(TTL_URL);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["PQ-TTL-A"], "the holder beats at the queue's own TTL and keeps the Slot");
		assert.deepEqual(q.waiting.map((w: any) => w.sessionKey), ["PQ-TTL-B"], "the waiter stays queued");
		assert.equal(await race(eB, 300), "blocked", "B never streams beside A on a one-slot queue");

		await sa.dispatch("session_shutdown");
		assert.equal(await race(eB, 3000), "done", "B proceeds once A releases");
		await sb.dispatch("session_shutdown");
	});

	it("beats under a queue's idleYieldMs, so an active holder is never yielded between beats", async () => {
		const a = startExt();
		const b = startExt();
		const sa = a.startSession("PQ-IDLE-A", { model: modelFor("idle", IDLE_URL) });
		const sb = b.startSession("PQ-IDLE-B", { model: modelFor("idle", IDLE_URL) });
		await sa.dispatch("agent_start"); // holds the queue with a 1000 ms idle window

		const eB = firstEvent(sb.stream(modelFor("idle", IDLE_URL)));
		await sleep(2500); // > 2 idle windows: the old 2 s defaults beat left the holder silent

		const q = await a.status(IDLE_URL);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["PQ-IDLE-A"], "the active holder beats twice per idle window and keeps the Slot");
		assert.deepEqual(q.waiting.map((w: any) => w.sessionKey), ["PQ-IDLE-B"], "the waiter stays queued: no idle yield of a beating holder");
		assert.equal(await race(eB, 300), "blocked", "B stays behind the active A");

		await sa.dispatch("session_shutdown");
		assert.equal(await race(eB, 3000), "done", "B proceeds once A releases");
		await sb.dispatch("session_shutdown");
	});

	it("restarts live beats when a hot-reload shortens a queue's leaseTtlMs", async () => {
		const ext = startExt(configFor(6000)); // the reload queue: beats every 2000 ms
		const session = ext.startSession("PQ-RELOAD", { model: modelFor("reload", RELOAD_URL) });
		await session.dispatch("agent_start");

		ext.writeConfig(configFor(1000)); // beats must now be every 333 ms, well inside the new TTL
		await pollUntil(() => session.notifications().some((n) => /config reloaded/.test(n.message)), 3000, "waiting for the extension's config reload");

		// Long enough for the old 2000 ms beat to have fired once and the new TTL (~1000 ms) to lapse
		// before the next old beat would land (4000 ms): only a restarted beat keeps the holder.
		await sleep(3000);
		const q = await ext.status(RELOAD_URL);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["PQ-RELOAD"], "the reloaded TTL never evicts the live holder");

		await session.dispatch("session_shutdown");
	});

	it("restarts live beats when a hot-reload shortens a queue's idleYieldMs", async () => {
		const ext = startExt(configFor(6000, 30000)); // the reload-idle queue: beats every 2000 ms, 30 s idle window
		const a = ext.startSession("PQ-IDLE-RELOAD-A", { model: modelFor("reloadIdle", RELOAD_IDLE_URL) });
		const b = ext.startSession("PQ-IDLE-RELOAD-B", { model: modelFor("reloadIdle", RELOAD_IDLE_URL) });
		await a.dispatch("agent_start");
		const eB = firstEvent(b.stream(modelFor("reloadIdle", RELOAD_IDLE_URL)));

		ext.writeConfig(configFor(6000, 1000)); // a 1000 ms idle window now applies while B is queued
		await pollUntil(() => a.notifications().some((n) => /config reloaded/.test(n.message)), 3000, "waiting for the extension's config reload");

		// Two new idle windows: without an immediate beat under the new policy, the sweep yields A.
		await sleep(2000);
		const q = await ext.status(RELOAD_IDLE_URL);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["PQ-IDLE-RELOAD-A"], "a shortened idle window never yields the live holder");
		assert.deepEqual(q.waiting.map((w: any) => w.sessionKey), ["PQ-IDLE-RELOAD-B"], "the waiter stays queued");

		await a.dispatch("session_shutdown");
		assert.equal(await race(eB, 3000), "done", "B proceeds once A releases");
		await b.dispatch("session_shutdown");
	});
});

after(() => {
	closeAllClients();
	disposeAllExtensions();
});
