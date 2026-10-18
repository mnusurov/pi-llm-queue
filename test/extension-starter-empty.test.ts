// Nothing to scaffold: a machine with no self-hosted, non-native endpoint must stay untouched —
// no file, no notify, no wrapped provider.
import assert from "node:assert/strict";
import { it } from "node:test";
import { existsSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const agentDir = mkdtempSync(join(tmpdir(), "llm-queue-starter-empty-"));
process.env.PI_CODING_AGENT_DIR = agentDir;
writeFileSync(join(agentDir, "models.json"), JSON.stringify({ providers: { openai: { baseUrl: "https://api.openai.com/v1" }, native: { baseUrl: "http://127.0.0.1:8080/v1" } } }));
const configPath = join(agentDir, "llm-queue.json");

const { default: extension } = await import("../src/extension.ts");

it("creates no config when every endpoint is cloud or native-owned", () => {
	const handlers: Record<string, ((e: any, ctx: any) => void)[]> = {};
	const registered: Record<string, any> = {};
	const notifies: string[] = [];
	extension({
		on: (event: string, handler: (e: any, ctx: any) => void) => (handlers[event] ??= []).push(handler),
		registerProvider: (name: string, config: any) => (registered[name] = config),
		unregisterProvider: (name: string) => delete registered[name],
		registerCommand: () => {},
	} as any);
	handlers["session_start"].forEach((h) =>
		h(
			{},
			{
				sessionManager: { getSessionId: () => "EMPTY" },
				model: { api: "openai-completions", baseUrl: "https://api.openai.com/v1", provider: "openai", id: "m" },
				signal: undefined,
				modelRegistry: { getRegisteredNativeProvider: (name: string) => (name === "native" ? { name } : undefined) },
				ui: { notify: (message: string) => notifies.push(message), setStatus: () => {} },
			},
		),
	);

	assert.equal(existsSync(configPath), false, "no file is created");
	assert.deepEqual(notifies, [], "nothing is notified");
	assert.deepEqual(registered, {});
});
