// Per-queue lease policy (#81): the client's beats must follow the queue's own leaseTtlMs and
// idleYieldMs, never `defaults` alone, and a hot-reload that shortens a TTL must restart the live
// beats. Real daemon + stub api + fake pi handlers, same seam as extension-idle-beat.test.ts.
import assert from "node:assert/strict";
import { describe, it, before, after } from "node:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAssistantMessageEventStream, registerApiProvider, unregisterApiProviders } from "@earendil-works/pi-ai/compat";
import { closeAllClients, connectClient, pollUntil, sleep, stopAllDaemons } from "./harness.ts";

const STUB_API = "stub-per-queue-api";
const TTL_URL = "http://short-ttl.test/v1";
const IDLE_URL = "http://short-idle.test/v1";
const RELOAD_URL = "http://reload-ttl.test/v1";
const RELOAD_IDLE_URL = "http://reload-idle.test/v1";

// Point the extension's module-level paths at a throwaway agent dir BEFORE importing it.
const agentDir = mkdtempSync(join(tmpdir(), "llm-queue-pq-"));
process.env.PI_CODING_AGENT_DIR = agentDir;
writeFileSync(
	join(agentDir, "models.json"),
	JSON.stringify({
		providers: {
			ttl: { baseUrl: TTL_URL, api: STUB_API, models: [{ id: "m" }] },
			idle: { baseUrl: IDLE_URL, api: STUB_API, models: [{ id: "m" }] },
			reload: { baseUrl: RELOAD_URL, api: STUB_API, models: [{ id: "m" }] },
			reloadIdle: { baseUrl: RELOAD_IDLE_URL, api: STUB_API, models: [{ id: "m" }] },
		},
	}),
);
const socketPath = join(agentDir, "llm-queue.sock");
const configPath = join(agentDir, "llm-queue.json");
const DAEMON = decodeURIComponent(new URL("../src/daemon.js", import.meta.url).pathname);

// defaults.leaseTtlMs 6000 puts the defaults-only beat at 2000 ms. Each queue below overrides one
// field so a beat derived from `defaults` alone is provably too slow for it.
const writeConfig = (reloadTtlMs = 6000, reloadIdleMs = 30000) =>
	writeFileSync(
		configPath,
		JSON.stringify({
			defaults: { maxStreams: 1, leaseTtlMs: 6000, idleYieldMs: 30000, daemonIdleExitMs: 1000000000 },
			queues: {
				[TTL_URL]: { maxStreams: 1, leaseTtlMs: 1000 },
				[IDLE_URL]: { maxStreams: 1, idleYieldMs: 1000 },
				[RELOAD_URL]: { maxStreams: 1, leaseTtlMs: reloadTtlMs },
				[RELOAD_IDLE_URL]: { maxStreams: 1, leaseTtlMs: 6000, idleYieldMs: reloadIdleMs },
			},
		}),
	);
writeConfig();

const { default: extension } = await import("../src/extension.ts");

const modelFor = (provider: string, baseUrl: string) => ({ api: STUB_API, baseUrl, provider, id: "m" }) as any;

function makePi() {
	const handlers: Record<string, ((e: any, ctx: any) => void)[]> = {};
	const registered: Record<string, any> = {};
	return {
		handlers,
		registered,
		on: (event: string, handler: (e: any, ctx: any) => void) => (handlers[event] ??= []).push(handler),
		registerProvider: (name: string, config: any) => (registered[name] = config),
		unregisterProvider: (name: string) => delete registered[name],
		registerCommand: () => {},
	};
}

function makeCtx(sessionId: string, model: any, ui: any = { notify: () => {}, setStatus: () => {} }) {
	return {
		sessionManager: { getSessionId: () => sessionId },
		model,
		signal: undefined,
		modelRegistry: { getRegisteredNativeProvider: () => undefined },
		ui,
	};
}

const start = (pi: ReturnType<typeof makePi>, sessionId: string, model: any, ui?: any) => {
	const ctx = makeCtx(sessionId, model, ui);
	pi.handlers["session_start"].forEach((h) => h({}, ctx));
	return ctx;
};

const firstEvent = (stream: AsyncIterable<any>) => (async () => { for await (const e of stream) return e; })();
const race = async (p: Promise<unknown>, ms: number) => Promise.race([p.then(() => "done"), sleep(ms).then(() => "blocked")]);

const queueStatus = async (queueKey: string) => {
	const raw = await connectClient(socketPath);
	raw.send({ op: "status" });
	const s = await raw.next(2000);
	raw.close();
	return s.queues.find((q: any) => q.queueKey === queueKey);
};

// Stub api: yields one event and ends. The blocking under test is the lease, not this.
registerApiProvider(
	{
		api: STUB_API,
		streamSimple: () => {
			const s = createAssistantMessageEventStream();
			s.push({
				type: "start",
				partial: { role: "assistant", content: [], api: STUB_API, provider: "stub", model: "m", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }, stopReason: "stop", timestamp: Date.now() },
			} as any);
			s.end();
			return s;
		},
	} as any,
	"test-per-queue-stub",
);

describe("per-queue lease policy (#81)", () => {
	let daemon: ChildProcess;

	before(async () => {
		daemon = spawn(process.execPath, [DAEMON, "--socket", socketPath, "--config", configPath, "--models-json", join(agentDir, "models.json"), "--sweep-ms", "200"], { stdio: "ignore" });
		daemon.unref();
		const c = await connectClient(socketPath);
		c.close();
	});

	it("beats at a queue's own leaseTtlMs, not the defaults' period", async () => {
		const a = makePi();
		const b = makePi();
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "PQ-TTL-A", modelFor("ttl", TTL_URL));
		const ctxB = start(b, "PQ-TTL-B", modelFor("ttl", TTL_URL));
		await a.handlers["agent_start"][0]({}, ctxA); // holds the 1000 ms-TTL queue

		const eB = firstEvent(b.registered["ttl"].streamSimple(modelFor("ttl", TTL_URL), {} as any, { sessionId: "PQ-TTL-B" } as any));
		await sleep(2500); // > 2 queue TTLs: a 2 s defaults beat would have let the TTL lapse

		const q = await queueStatus(TTL_URL);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["PQ-TTL-A"], "the holder beats at the queue's own TTL and keeps the Slot");
		assert.deepEqual(q.waiting.map((w: any) => w.sessionKey), ["PQ-TTL-B"], "the waiter stays queued");
		assert.equal(await race(eB, 300), "blocked", "B never streams beside A on a one-slot queue");

		a.handlers["session_shutdown"][0]({}, ctxA);
		assert.equal(await race(eB, 3000), "done", "B proceeds once A releases");
		b.handlers["session_shutdown"][0]({}, ctxB);
	});

	it("beats under a queue's idleYieldMs, so an active holder is never yielded between beats", async () => {
		const a = makePi();
		const b = makePi();
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "PQ-IDLE-A", modelFor("idle", IDLE_URL));
		const ctxB = start(b, "PQ-IDLE-B", modelFor("idle", IDLE_URL));
		await a.handlers["agent_start"][0]({}, ctxA); // holds the queue with a 1000 ms idle window

		const eB = firstEvent(b.registered["idle"].streamSimple(modelFor("idle", IDLE_URL), {} as any, { sessionId: "PQ-IDLE-B" } as any));
		await sleep(2500); // > 2 idle windows: the old 2 s defaults beat left the holder silent

		const q = await queueStatus(IDLE_URL);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["PQ-IDLE-A"], "the active holder beats twice per idle window and keeps the Slot");
		assert.deepEqual(q.waiting.map((w: any) => w.sessionKey), ["PQ-IDLE-B"], "the waiter stays queued: no idle yield of a beating holder");
		assert.equal(await race(eB, 300), "blocked", "B stays behind the active A");

		a.handlers["session_shutdown"][0]({}, ctxA);
		assert.equal(await race(eB, 3000), "done", "B proceeds once A releases");
		b.handlers["session_shutdown"][0]({}, ctxB);
	});

	it("restarts live beats when a hot-reload shortens a queue's leaseTtlMs", async () => {
		writeConfig(6000); // the reload queue: beats every 2000 ms
		const notes: string[] = [];
		const a = makePi();
		extension(a as any);
		const ctx = start(a, "PQ-RELOAD", modelFor("reload", RELOAD_URL), { notify: (text: string) => notes.push(text), setStatus: () => {} });
		await a.handlers["agent_start"][0]({}, ctx);

		writeConfig(1000); // beats must now be every 333 ms, well inside the new TTL
		await pollUntil(() => notes.some((n) => /config reloaded/.test(n)), 3000, "waiting for the extension's config reload");

		// Long enough for the old 2000 ms beat to have fired once and the new TTL (~1000 ms) to lapse
		// before the next old beat would land (4000 ms): only a restarted beat keeps the holder.
		await sleep(3000);
		const q = await queueStatus(RELOAD_URL);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["PQ-RELOAD"], "the reloaded TTL never evicts the live holder");

		a.handlers["session_shutdown"][0]({}, ctx);
	});

	it("restarts live beats when a hot-reload shortens a queue's idleYieldMs", async () => {
		writeConfig(6000, 30000); // the reload-idle queue: beats every 2000 ms, 30 s idle window
		const notes: string[] = [];
		const a = makePi();
		const b = makePi();
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "PQ-IDLE-RELOAD-A", modelFor("reloadIdle", RELOAD_IDLE_URL), { notify: (text: string) => notes.push(text), setStatus: () => {} });
		const ctxB = start(b, "PQ-IDLE-RELOAD-B", modelFor("reloadIdle", RELOAD_IDLE_URL));
		await a.handlers["agent_start"][0]({}, ctxA);
		const eB = firstEvent(b.registered["reloadIdle"].streamSimple(modelFor("reloadIdle", RELOAD_IDLE_URL), {} as any, { sessionId: "PQ-IDLE-RELOAD-B" } as any));

		writeConfig(6000, 1000); // a 1000 ms idle window now applies while B is queued
		await pollUntil(() => notes.some((n) => /config reloaded/.test(n)), 3000, "waiting for the extension's config reload");

		// Two new idle windows: without an immediate beat under the new policy, the sweep yields A.
		await sleep(2000);
		const q = await queueStatus(RELOAD_IDLE_URL);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["PQ-IDLE-RELOAD-A"], "a shortened idle window never yields the live holder");
		assert.deepEqual(q.waiting.map((w: any) => w.sessionKey), ["PQ-IDLE-RELOAD-B"], "the waiter stays queued");

		a.handlers["session_shutdown"][0]({}, ctxA);
		assert.equal(await race(eB, 3000), "done", "B proceeds once A releases");
		b.handlers["session_shutdown"][0]({}, ctxB);
	});
});

after(() => {
	unregisterApiProviders("test-per-queue-stub");
	closeAllClients();
	stopAllDaemons();
	spawnSync("pkill", ["-f", socketPath]);
});
