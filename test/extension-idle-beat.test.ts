// Run-scoped keyed beat (#57): the extension beats for the whole live run — streaming and
// compute tools alike — and pauses only while the run sits in a blocking tool, so idle-yield
// fires for a Holder parked on another agent and never for one busy in a compute tool.
// Real daemon + stub api + fake pi handlers, same seam as extension-wrapper.test.ts.
import assert from "node:assert/strict";
import { describe, it, before, after } from "node:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAssistantMessageEventStream, registerApiProvider, unregisterApiProviders } from "@earendil-works/pi-ai/compat";
import { QUEUE_KEY, closeAllClients, connectClient, sleep, stopAllDaemons } from "./harness.ts";
import { LeaseClient } from "../src/lease-client.ts";

const STUB_API = "stub-idle-api";

// Point the extension's module-level paths at a throwaway agent dir BEFORE importing it.
const agentDir = mkdtempSync(join(tmpdir(), "llm-queue-idle-"));
process.env.PI_CODING_AGENT_DIR = agentDir;
// idleYieldMs 1500 + sweep 200 ms: a beat gap shows up in ~1.5-2.5 s. leaseTtlMs 3000 puts the
// client beat period at 1000 ms, so a run beats 1-2× per idle window.
writeFileSync(join(agentDir, "llm-queue.json"), JSON.stringify({ defaults: { maxStreams: 1, idleYieldMs: 1500, leaseTtlMs: 3000 }, queues: { stub: { maxStreams: 1 } } }));
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
		unregisterProvider: (name: string) => delete registered[name],
		registerCommand: () => {},
	};
}

function makeCtx(sessionId: string) {
	return {
		sessionManager: { getSessionId: () => sessionId },
		model,
		signal: undefined,
		modelRegistry: { getRegisteredNativeProvider: () => undefined },
		ui: { notify: () => {}, setStatus: () => {} },
	};
}

const start = (pi: ReturnType<typeof makePi>, sessionId: string) => {
	const ctx = makeCtx(sessionId);
	pi.handlers["session_start"].forEach((h) => h({}, ctx));
	return ctx;
};

const firstEvent = (stream: AsyncIterable<any>) => (async () => { for await (const e of stream) return e; })();
const race = async (p: Promise<unknown>, ms: number) => Promise.race([p.then(() => "done"), sleep(ms).then(() => "blocked")]);

const queueStatus = async () => {
	const raw = await connectClient(socketPath);
	raw.send({ op: "status" });
	const s = await raw.next(2000);
	raw.close();
	return s.queues.find((q: any) => q.queueKey === QUEUE_KEY);
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
	"test-idle-stub",
);

	describe("run-scoped keyed beat (idle-yield, #57)", () => {
	let daemon: ChildProcess;

	before(async () => {
		daemon = spawn(process.execPath, [DAEMON, "--socket", socketPath, "--config", join(agentDir, "llm-queue.json"), "--models-json", join(agentDir, "models.json"), "--sweep-ms", "200"], { stdio: "ignore" });
		daemon.unref();
		const c = await connectClient(socketPath);
		c.close();
	});

	it("a compute tool longer than idleYieldMs keeps the beat and the Slot", async () => {
		const a = makePi();
		const b = makePi();
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "CMP-A");
		const ctxB = start(b, "CMP-B");
		await a.handlers["agent_start"][0]({}, ctxA); // grant → the run-scoped beat starts

		// A enters a compute tool (bash) and stays there 3× idleYieldMs with B queued behind it.
		a.handlers["tool_execution_start"][0]({ toolCallId: "c1", toolName: "bash", args: { command: "make test" } }, ctxA);
		const eB = firstEvent(b.registered["stub"].streamSimple(model, {} as any));
		for (let i = 0; i < 3; i++) {
			await sleep(1500);
			const q = await queueStatus();
			assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["CMP-A"], `the compute-busy Holder keeps the Slot at ${1500 * (i + 1)} ms into the tool`);
			assert.deepEqual(q.waiting.map((w: any) => w.sessionKey), ["CMP-B"], "the waiter stays queued: beats flowed through the tool");
		}
		a.handlers["tool_execution_end"][0]({ toolCallId: "c1", toolName: "bash", result: {}, isError: false }, ctxA);

		// A's next stream runs under the same lease: it streams at once and does not re-queue.
		const eA = firstEvent(a.registered["stub"].streamSimple(model, {} as any, { sessionId: "CMP-A" } as any));
		assert.equal(await race(eA, 500), "done", "the Holder streams without re-queuing");
		const q = await queueStatus();
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["CMP-A"], "A still holds the Slot");
		assert.deepEqual(q.waiting.map((w: any) => w.sessionKey), ["CMP-B"], "A never landed in the waiting list");
		assert.equal(await race(eB, 400), "blocked", "B stays behind the live run");

		a.handlers["session_shutdown"][0]({}, ctxA); // A releases: B's turn
		assert.equal(await race(eB, 3000), "done", "B proceeds once A releases");
		b.handlers["session_shutdown"][0]({}, ctxB);
	});
	it("a blocking tool longer than idleYieldMs stops the beat and yields the Slot", async () => {
		const a = makePi();
		const b = makePi();
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "BLK-A");
		const ctxB = start(b, "BLK-B");
		await a.handlers["agent_start"][0]({}, ctxA); // grant → the run-scoped beat starts

		// A parks in `subagent` — the tool that waits on the very session queued behind it.
		a.handlers["tool_execution_start"][0]({ toolCallId: "s1", toolName: "subagent", args: {} }, ctxA);
		const eB = firstEvent(b.registered["stub"].streamSimple(model, {} as any));
		assert.equal(await race(eB, 400), "blocked", "B queues behind the live Holder");
		assert.equal(await race(eB, 3000), "done", "A parked on its Child yields the Slot: B is granted");
		a.handlers["tool_execution_end"][0]({ toolCallId: "s1", toolName: "subagent", result: {}, isError: false }, ctxA);

		// A's run lost the Slot: its next beat answers granted:false (beat stops) and its next
		// stream re-queues, proceeding once the slot frees — not streaming unqueued.
		const eA = firstEvent(a.registered["stub"].streamSimple(model, {} as any, { sessionId: "BLK-A" } as any));
		assert.equal(await race(eA, 3000), "done", "the yielded run re-queues and proceeds when the slot frees");

		a.handlers["session_shutdown"][0]({}, ctxA);
		b.handlers["session_shutdown"][0]({}, ctxB);
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
			const a = makePi();
			extension(a as any);
			const ctx = start(a, "TMR");
			await a.handlers["agent_start"][0]({}, ctx); // grant → beat starts
			await sleep(2500); // ≥ 2 beats at the 1000 ms beat period
			assert.ok(beats.filter((s) => s === "TMR").length >= 2, "the live run beats");
			beats.length = 0;
			await a.handlers["agent_settled"][0]({}, ctx); // grace 3000 ms → release
			await sleep(3600); // grace expired: the release stopped the beat
			beats.length = 0;
			await sleep(2500); // 2× the beat period of quiet
			assert.equal(beats.filter((s) => s === "TMR").length, 0, "no beat after the grace release: no timer leak");
			a.handlers["session_shutdown"][0]({}, ctx);
		} finally {
			proto.beat = orig;
		}
	});
});

after(() => {
	unregisterApiProviders("test-idle-stub");
	closeAllClients();
	stopAllDaemons();
	spawnSync("pkill", ["-f", socketPath]);
});
