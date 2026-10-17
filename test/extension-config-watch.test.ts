// The README install path: "llm-queue.json is optional ... Create it to activate the queue".
// A config created after pi starts must be picked up by the running session (audit finding 4).
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { QUEUE_KEY, sleep } from "./harness.ts";

const STUB_API = "stub-watch-api";

// Point the extension's module-level paths at a throwaway agent dir with NO config file.
const agentDir = mkdtempSync(join(tmpdir(), "llm-queue-watch-"));
process.env.PI_CODING_AGENT_DIR = agentDir;
writeFileSync(join(agentDir, "models.json"), JSON.stringify({ providers: { stub: { baseUrl: QUEUE_KEY, api: STUB_API, models: [{ id: "m" }] } } }));
const socketPath = join(agentDir, "llm-queue.sock");

const { default: extension } = await import("../src/extension.ts");

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

const makeCtx = (sessionId: string) => ({
	sessionManager: { getSessionId: () => sessionId },
	model: { api: STUB_API, baseUrl: QUEUE_KEY, provider: "stub", id: "m" },
	signal: undefined,
	modelRegistry: { getRegisteredNativeProvider: () => undefined },
	ui: { notify: () => {}, setStatus: () => {} },
});

describe("config created after startup", () => {
	it("wraps the provider once the config file appears, without restarting pi", async () => {
		const pi = makePi();
		extension(pi as any);
		pi.handlers["session_start"].forEach((h) => h({}, makeCtx("LATE")));
		assert.equal(pi.registered["stub"], undefined, "no config at startup: nothing is queued");

		writeFileSync(join(agentDir, "llm-queue.json"), JSON.stringify({ defaults: { maxStreams: 1 }, queues: { stub: { maxStreams: 1 } } }));
		await sleep(1500); // fs event + 250 ms reload debounce
		assert.ok(pi.registered["stub"], "the config created after session_start is picked up by the running session");
	});
});

after(() => spawnSync("pkill", ["-f", socketPath]));
