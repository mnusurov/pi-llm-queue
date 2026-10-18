// Fresh install: a missing llm-queue.json means an inert queue. The first session_start must
// scaffold a starter config from models.json and make the queue live in that same session.
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const STUB_API = "stub-starter-api";
const LOCAL_URL = "http://192.168.1.50:8000/v1";
const CLOUD_URL = "https://api.openai.com/v1";
const NATIVE_URL = "http://127.0.0.1:8080/v1";

// Point the extension's module-level paths at a throwaway agent dir with NO config file.
const agentDir = mkdtempSync(join(tmpdir(), "llm-queue-starter-"));
process.env.PI_CODING_AGENT_DIR = agentDir;
writeFileSync(
	join(agentDir, "models.json"),
	JSON.stringify({
		providers: {
			strata: { baseUrl: LOCAL_URL, api: STUB_API, models: [{ id: "m" }] },
			extra: { baseUrl: LOCAL_URL, api: STUB_API, models: [{ id: "m" }] },
			openai: { baseUrl: CLOUD_URL, api: "openai-completions" },
			native: { baseUrl: NATIVE_URL, api: STUB_API },
		},
	}),
);
const configPath = join(agentDir, "llm-queue.json");

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

const makeCtx = (notifies: string[]) => ({
	sessionManager: { getSessionId: () => "STARTER" },
	model: { api: STUB_API, baseUrl: LOCAL_URL, provider: "strata", id: "m" },
	signal: undefined,
	// The built-in llama-style provider: native-owned endpoints must stay out of the starter.
	modelRegistry: { getRegisteredNativeProvider: (name: string) => (name === "native" ? { name } : undefined) },
	ui: { notify: (message: string) => notifies.push(message), setStatus: () => {} },
});

const startSession = (pi: any, notifies: string[]) => pi.handlers["session_start"].forEach((h: any) => h({}, makeCtx(notifies)));

describe("starter config on first launch", () => {
	it("creates the config once, notifies, and queues the self-hosted providers in the same session", () => {
		const pi = makePi();
		const notifies: string[] = [];
		extension(pi as any);
		assert.equal(existsSync(configPath), false, "importing the extension must not write the config");

		startSession(pi, notifies);

		const written = JSON.parse(readFileSync(configPath, "utf8"));
		assert.deepEqual(written, { schemaVersion: 1, queues: { [LOCAL_URL]: { enabled: true }, [CLOUD_URL]: { enabled: false } } });
		assert.equal(notifies.filter((m) => m.includes(configPath)).length, 1, "one notify naming the created file");
		assert.ok(notifies[0].includes("1 self-hosted"), `notify names the enabled endpoint count: ${notifies[0]}`);
		assert.deepEqual(Object.keys(pi.registered).sort(), ["extra", "strata"], "providers on the self-hosted endpoint are wrapped");
		assert.equal(pi.registered["native"], undefined, "native-owned endpoint is excluded");
		assert.equal(pi.registered["openai"], undefined, "cloud endpoint stays passthrough");
	});

	it("leaves an existing config untouched and stays quiet", () => {
		const handWritten = JSON.stringify({ schemaVersion: 1, queues: { strata: { maxStreams: 4 } } });
		writeFileSync(configPath, handWritten);
		const pi = makePi();
		const notifies: string[] = [];

		extension(pi as any);
		startSession(pi, notifies);

		assert.equal(readFileSync(configPath, "utf8"), handWritten, "a hand-tuned config is never overwritten");
		assert.deepEqual(notifies, [], "an established setup gets no starter notify");
	});
});
