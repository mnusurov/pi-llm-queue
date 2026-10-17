// Provider-wrapper seam: the extension's registered streamSimple must serialise requests
// through the daemon. Uses a stub api provider (no real endpoint) and a real daemon.
import assert from "node:assert/strict";
import { describe, it, before, after } from "node:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAssistantMessageEventStream, registerApiProvider, unregisterApiProviders } from "@earendil-works/pi-ai/compat";
import { QUEUE_KEY, closeAllClients, connectClient, sleep, stopAllDaemons } from "./harness.ts";

const STUB_API = "stub-queue-api";

// Point the extension's module-level paths at a throwaway agent dir BEFORE importing it.
const agentDir = mkdtempSync(join(tmpdir(), "llm-queue-ext-"));
process.env.PI_CODING_AGENT_DIR = agentDir;
writeFileSync(join(agentDir, "llm-queue.json"), JSON.stringify({ defaults: { maxStreams: 1, idleQuantumMs: 600 }, queues: { stub: { maxStreams: 1 } } }));
writeFileSync(join(agentDir, "models.json"), JSON.stringify({ providers: { stub: { baseUrl: QUEUE_KEY, api: STUB_API, models: [{ id: "m" }] } } }));
const socketPath = join(agentDir, "llm-queue.sock");
const DAEMON = decodeURIComponent(new URL("../src/daemon.js", import.meta.url).pathname);

const { default: extension } = await import("../src/extension.ts");

const model = { api: STUB_API, baseUrl: QUEUE_KEY, provider: "stub", id: "m" } as any;

function makePi() {
	const handlers: Record<string, ((e: any, ctx: any) => void)[]> = {};
	const registered: Record<string, any> = {};
	return {
		handlers,
		registered,
		on: (event: string, handler: (e: any, ctx: any) => void) => (handlers[event] ??= []).push(handler),
		registerProvider: (name: string, config: any) => (registered[name] = config),
	};
}

function makeCtx(sessionId: string, signal?: AbortSignal) {
	return {
		sessionManager: { getSessionId: () => sessionId },
		model,
		signal,
		modelRegistry: { getRegisteredNativeProvider: () => undefined },
		ui: { notify: () => {} },
	};
}

const start = (pi: ReturnType<typeof makePi>, sessionId: string, signal?: AbortSignal) => {
	const ctx = makeCtx(sessionId, signal);
	pi.handlers["session_start"].forEach((h) => h({}, ctx));
	return ctx;
};

const firstEvent = (stream: AsyncIterable<any>) => (async () => { for await (const e of stream) return e; })();

describe("provider wrapper (queuedStream)", () => {
	let daemon: ChildProcess;

	// Run a daemon on the agent-dir socket up front so the extension's lease connects to a
	// live daemon (not fail-open) and serialises the streams.
	before(async () => {
		daemon = spawn(process.execPath, [DAEMON, "--socket", socketPath, "--config", join(agentDir, "llm-queue.json"), "--models-json", join(agentDir, "models.json")], { stdio: "ignore" });
		daemon.unref();
		const c = await connectClient(socketPath);
		c.close();
	});

	// Stub api: yields one event and ends. The blocking under test is the lease acquire, not this.
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
		"test-stub",
	);

	it("serialises a second session's queuedStream behind the holder until release", async () => {
		const a = makePi();
		const b = makePi();
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "A");
		start(b, "B");

		// Session A holds the slot for its whole run.
		await a.handlers["agent_start"][0]({}, ctxA);

		const streamB = b.registered["stub"].streamSimple;
		const sB = streamB(model, {} as any); // B is not held → queues for itself behind A
		const eB = firstEvent(sB);
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(400).then(() => "blocked")]),
			"blocked",
			"B's stream must not start while A holds the slot",
		);

		a.handlers["session_shutdown"][0]({}, ctxA); // A releases its lease
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(3000).then(() => "blocked")]),
			"done",
			"B's stream proceeds once the slot frees",
		);
	});

	it("streams immediately when the run already holds the slot (no self-queue)", async () => {
		const a = makePi();
		extension(a as any);
		const ctx = start(a, "S", new AbortController().signal);
		await a.handlers["agent_start"][0]({}, ctx); // acquires the slot for the run
		const stream = a.registered["stub"].streamSimple;

		const s = stream(model, {} as any);
		assert.equal(
			await Promise.race([firstEvent(s).then(() => "done"), sleep(500).then(() => "blocked")]),
			"done",
			"held request streams without queuing",
		);
		a.handlers["session_shutdown"][0]({}, ctx);
	});

	it("reports an error event when the stream implementation is missing", async () => {
		const a = makePi();
		extension(a as any);
		const ctx = start(a, "E");
		await a.handlers["agent_start"][0]({}, ctx); // holds QUEUE_KEY
		const stream = a.registered["stub"].streamSimple;
		const s = stream({ ...model, api: "no-such-api-xyz" }, {} as any); // held → no acquire; impl missing
		const ev = await firstEvent(s);
		assert.equal(ev.type, "error");
		a.handlers["session_shutdown"][0]({}, ctx);
	});

	it("releases the lease in finally when a non-held request errors", async () => {
		const a = makePi();
		extension(a as any);
		start(a, "R"); // no agent_start → the request queues for itself
		const stream = a.registered["stub"].streamSimple;
		const s = stream({ ...model, api: "no-such-api-xyz" }, {} as any); // acquires for R, impl missing
		assert.equal((await firstEvent(s)).type, "error");

		// The finally released R's lease, so a fresh session can take the slot.
		const b = makePi();
		extension(b as any);
		start(b, "T");
		const sB = b.registered["stub"].streamSimple(model, {} as any);
		assert.equal(
			await Promise.race([firstEvent(sB).then(() => "done"), sleep(3000).then(() => "blocked")]),
			"done",
			"R's lease was released on the error path",
		);
	});

	it("beats the lease on every held request, retaining the slot across tool executions", async () => {
		const a = makePi();
		const b = makePi();
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "KEEP");
		start(b, "STEAL");
		await a.handlers["agent_start"][0]({}, ctxA); // A holds the slot for its run

		const stream = a.registered["stub"].streamSimple;
		const sB = b.registered["stub"].streamSimple(model, {} as any); // queued behind A
		const eB = firstEvent(sB);

		// A keeps requesting (each request beats the lease) for ~2 idle quanta: B must not steal the slot.
		for (let i = 0; i < 8; i++) {
			await firstEvent(stream(model, {} as any));
			await sleep(150);
		}
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(700).then(() => "blocked")]),
			"blocked",
			"an active run keeps the slot while a session waits",
		);
		a.handlers["session_shutdown"][0]({}, ctxA);
		await eB; // B proceeds once A releases
	});

	it("keeps the slot through the settle grace window, then releases it (wake-chain protection)", async () => {
		const a = makePi();
		const b = makePi();
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "GRACE");
		start(b, "GRACE-B");
		await a.handlers["agent_start"][0]({}, ctxA);
		await a.handlers["agent_settled"][0]({}, ctxA); // 3 s grace: a wake chain keeps the slot

		// Keep the run active (a beat every 250 ms) so only the grace window can free the slot.
		const eB = firstEvent(b.registered["stub"].streamSimple(model, {} as any));
		for (let i = 0; i < 10; i++) {
			await firstEvent(a.registered["stub"].streamSimple(model, {} as any));
			await sleep(250);
		}
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(300).then(() => "blocked")]),
			"blocked",
			"the grace window keeps the slot after the run settles",
		);
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(2000).then(() => "blocked")]),
			"done",
			"the slot is released once the grace window expires",
		);
	});

	it("releases the held slot immediately on abort", async () => {
		const ac = new AbortController();
		const a = makePi();
		const b = makePi();
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "ABORT", ac.signal);
		start(b, "ABORT-B");
		await a.handlers["agent_start"][0]({}, ctxA); // holds the slot

		const eB = firstEvent(b.registered["stub"].streamSimple(model, {} as any));
		assert.equal(await Promise.race([eB.then(() => "done"), sleep(400).then(() => "blocked")]), "blocked");
		ac.abort();
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(3000).then(() => "blocked")]),
			"done",
			"Esc releases the held slot at once",
		);
	});

	it("re-queues when the daemon switches the slot away mid-run (quantum preemption)", async () => {
		const a = makePi();
		extension(a as any);
		const ctxA = start(a, "EVICTED");
		await a.handlers["agent_start"][0]({}, ctxA); // holds the slot
		const raw = await connectClient(socketPath);
		raw.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "RAW", role: "parent" }); // queued behind it

		await sleep(2500); // idle quantum (600 ms) + sweep: the holder is evicted, RAW is granted
		const stream = a.registered["stub"].streamSimple;
		const eA = firstEvent(stream(model, {} as any)); // its beat comes back ungranted: must re-queue
		for (let i = 0; i < 5; i++) {
			raw.send({ op: "renew", queueKey: QUEUE_KEY, sessionKey: "RAW" }); // RAW keeps its slot
			await sleep(150);
		}
		assert.equal(
			await Promise.race([eA.then(() => "done"), sleep(700).then(() => "blocked")]),
			"blocked",
			"a preempted run re-queues behind the new holder instead of streaming unqueued",
		);

		raw.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "RAW" });
		assert.equal(
			await Promise.race([eA.then(() => "done"), sleep(3000).then(() => "blocked")]),
			"done",
			"the preempted run proceeds once the slot frees",
		);
		raw.close();
		a.handlers["session_shutdown"][0]({}, ctxA);
	});
});

after(() => {
	unregisterApiProviders("test-stub");
	closeAllClients();
	stopAllDaemons();
	spawnSync("pkill", ["-f", socketPath]);
});
