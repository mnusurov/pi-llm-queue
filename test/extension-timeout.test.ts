// queueTimeoutMs seam (#8): a parent whose queue wait exceeds queueTimeoutMs passes through
// with a visible warning (no hard error). Own agent dir + config so queueTimeoutMs is set at
// extension import; stub api provider, real daemon, no real endpoint.
import assert from "node:assert/strict";
import { describe, it, before, after } from "node:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAssistantMessageEventStream, registerApiProvider, unregisterApiProviders } from "@earendil-works/pi-ai/compat";
import { QUEUE_KEY, closeAllClients, connectClient, sleep, stopAllDaemons } from "./harness.ts";

const STUB_API = "stub-timeout-api";

const agentDir = mkdtempSync(join(tmpdir(), "llm-queue-timeout-"));
process.env.PI_CODING_AGENT_DIR = agentDir;
// queueTimeoutMs 300 ms: a parent queued behind a long holder times out and passes through.
writeFileSync(join(agentDir, "llm-queue.json"), JSON.stringify({ defaults: { maxStreams: 1, queueTimeoutMs: 300 }, queues: { stub: { maxStreams: 1 } } }));
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
		registerCommand: (_name: string, _config: any) => {},
	};
}

function makeCtx(sessionId: string, ui: any) {
	return {
		sessionManager: { getSessionId: () => sessionId },
		model,
		signal: undefined,
		modelRegistry: { getRegisteredNativeProvider: () => undefined },
		ui,
	};
}

const start = (pi: ReturnType<typeof makePi>, sessionId: string, ui: any) => {
	const ctx = makeCtx(sessionId, ui);
	pi.handlers["session_start"].forEach((h) => h({}, ctx));
	return ctx;
};

const firstEvent = (stream: AsyncIterable<any>) => (async () => { for await (const e of stream) return e; })();

describe("queueTimeoutMs visible warning", () => {
	let daemon: ChildProcess;

	before(async () => {
		daemon = spawn(process.execPath, [DAEMON, "--socket", socketPath, "--config", join(agentDir, "llm-queue.json"), "--models-json", join(agentDir, "models.json")], { stdio: "ignore" });
		daemon.unref();
		const c = await connectClient(socketPath);
		c.close();
	});

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
		"test-timeout",
	);

	it("warns a queued parent past queueTimeoutMs and keeps it queued (no passthrough)", async () => {
		const a = makePi();
		const b = makePi();
		const notices: [string, string][] = [];
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "HOLDER", { notify: () => {}, setStatus: () => {} });
		start(b, "TIMEOUT", { notify: (text: string, level: string) => notices.push([text, level]), setStatus: () => {} });

		await a.handlers["agent_start"][0]({}, ctxA); // A holds the slot for its whole run

		const eB = firstEvent(b.registered["stub"].streamSimple(model, {} as any)); // B queues behind A
		const outcome = await Promise.race([eB.then((ev) => ev.type), sleep(2000).then(() => "blocked")]);

		assert.equal(outcome, "blocked", "B stays queued past queueTimeoutMs (warn-only, no passthrough)");
		const warning = notices.find(([text, level]) => level === "warning" && text.includes("queueTimeoutMs"));
		assert.ok(warning, "a visible warning is shown when the queue wait exceeds queueTimeoutMs");

		// Release A: B is granted and streams.
		a.handlers["session_shutdown"][0]({}, ctxA);
		const granted = await Promise.race([eB.then((ev) => ev.type), sleep(3000).then(() => "blocked")]);
		assert.equal(granted, "start", "B is granted and streams once the slot frees");
	});
});

after(() => {
	unregisterApiProviders("test-timeout");
	closeAllClients();
	stopAllDaemons();
	spawnSync("pkill", ["-f", socketPath]);
});
