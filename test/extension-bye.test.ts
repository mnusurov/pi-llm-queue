// Bye on session_shutdown: the extension announces the departure to the daemon, so the daemon
// exits the moment the last session shuts down — while the pi process is still alive. The
// socket-close fallback (#60) only fires when the process dies; a pi that lingers after its last
// session (slow teardown, hung shutdown) must not keep stale daemon code resident, because the
// next pi would then connect to the old daemon instead of autostarting a fresh one.
// Real daemon + stub api + fake pi handlers, same seam as extension-idle-beat.test.ts.
import assert from "node:assert/strict";
import { describe, it, before, after } from "node:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAssistantMessageEventStream, registerApiProvider, unregisterApiProviders } from "@earendil-works/pi-ai/compat";
import { QUEUE_KEY, closeAllClients, connectClient, exited, stopAllDaemons } from "./harness.ts";

const STUB_API = "stub-bye-api";

// Point the extension's module-level paths at a throwaway agent dir BEFORE importing it.
const agentDir = mkdtempSync(join(tmpdir(), "llm-queue-bye-"));
process.env.PI_CODING_AGENT_DIR = agentDir;
// No daemonIdleExitMs: the production default 0 — exit at once when the last counted client leaves.
writeFileSync(join(agentDir, "llm-queue.json"), JSON.stringify({ defaults: { maxStreams: 1 }, queues: { stub: { maxStreams: 1 } } }));
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

const queueStatus = async () => {
	const raw = await connectClient(socketPath);
	raw.send({ op: "status" });
	const s = await raw.next(2000);
	raw.close();
	return s.queues.find((q: any) => q.queueKey === QUEUE_KEY);
};

// Stub api: yields one event and ends. The shutdown under test is the lease, not this.
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
	"test-bye-stub",
);

describe("bye on session_shutdown (#63): the daemon exits while pi is still alive", () => {
	let daemon: ChildProcess;

	before(async () => {
		daemon = spawn(process.execPath, [DAEMON, "--socket", socketPath, "--config", join(agentDir, "llm-queue.json"), "--models-json", join(agentDir, "models.json")], { stdio: "ignore" });
		daemon.unref();
		const c = await connectClient(socketPath);
		c.close();
	});

	after(() => {
		unregisterApiProviders("test-bye-stub");
		daemon?.kill();
		stopAllDaemons();
		closeAllClients();
		spawnSync("pkill", ["-f", socketPath]); // daemons the extension autostarted on reconnect
	});

	it("the last session's shutdown exits the daemon with the client process still alive", async () => {
		const a = makePi();
		extension(a as any);
		const ctx = start(a, "BYE-A");
		await a.handlers["agent_start"][0]({}, ctx); // acquire: the connection becomes a counted client

		const q = await queueStatus();
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["BYE-A"], "A holds the Slot before shutdown");

		a.handlers["session_shutdown"][0]({}, ctx); // the extension says bye and closes the lease socket
		assert.ok(await exited(daemon, 2000), "bye exits the daemon while this test process — standing in for pi — is alive");
	});
});
