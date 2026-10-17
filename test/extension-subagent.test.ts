// Integration seam: the tool_call handler must force subagent launches to async:false only on
// serialised (single-slot) queues. Uses a real pi SDK session + a stub provider (no real endpoint).
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Type } from "typebox";
import { createAssistantMessageEventStream, registerApiProvider, unregisterApiProviders } from "@earendil-works/pi-ai/compat";
import {
	createAgentSession,
	DefaultResourceLoader,
	ModelRuntime,
	SessionManager,
} from "@earendil-works/pi-coding-agent";
import type { Model } from "@earendil-works/pi-ai/compat";

const STUB_API = "stub-subagent-api";
const SERIAL_KEY = "http://serial.test/v1";
const PARALLEL_KEY = "http://parallel.test/v1";

// Point the extension's module-level paths at a throwaway agent dir BEFORE importing it.
const agentDir = mkdtempSync(join(tmpdir(), "llm-queue-sub-"));
process.env.PI_CODING_AGENT_DIR = agentDir;
writeFileSync(
	join(agentDir, "llm-queue.json"),
	JSON.stringify({
		defaults: { maxStreams: 1 },
		queues: { serial: { match: ["serial_prov"], maxStreams: 1 }, par: { match: ["par_prov"], maxStreams: 2 } },
	}),
);
writeFileSync(
	join(agentDir, "models.json"),
	JSON.stringify({
		providers: {
			serial_prov: { baseUrl: SERIAL_KEY, api: STUB_API, models: [{ id: "m" }] },
			par_prov: { baseUrl: PARALLEL_KEY, api: STUB_API, models: [{ id: "m" }] },
		},
	}),
);
const socketPath = join(agentDir, "llm-queue.sock");

const { default: extension } = await import("../src/extension.ts");

// Stub provider: the first request per provider returns a subagent tool call (async:true);
// every later request stops, so the run ends after the tool executes.
const calls = new Map<string, number>();
let nextToolName = "subagent"; // the absent-tool test flips this to a tool the hook ignores
function stubStream(model: any) {
	const stream = createAssistantMessageEventStream();
	const n = calls.get(model.provider) ?? 0;
	calls.set(model.provider, n + 1);
	const base: any = {
		role: "assistant",
		content: [],
		api: STUB_API,
		provider: model.provider,
		model: model.id,
		usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
		stopReason: "stop",
		timestamp: Date.now(),
	};
	if (n === 0) {
		const toolCall: any = { type: "toolCall", id: "tc1", name: nextToolName, arguments: { agent: "a", task: "t", async: true } };
		base.content = [toolCall];
		base.stopReason = "toolUse";
		stream.push({ type: "start", partial: base });
		stream.push({ type: "toolcall_start", contentIndex: 0, partial: base });
		stream.push({ type: "toolcall_end", contentIndex: 0, toolCall, partial: base });
		stream.push({ type: "done", reason: "toolUse", message: base });
	} else {
		base.content = [{ type: "text", text: "done" }];
		stream.push({ type: "start", partial: base });
		stream.push({ type: "done", reason: "stop", message: base });
	}
	stream.end();
	return stream;
}
registerApiProvider(
	{ api: STUB_API, streamSimple: stubStream, stream: stubStream } as any,
	"test-stub-subagent",
);

// A stub "subagent" tool: the model calls it; the queue extension's tool_call handler rewrites
// its async argument. The tool itself does nothing (no child is launched in this seam test).
const subagentTool = {
	name: "subagent",
	label: "Subagent",
	description: "Launch a subagent",
	parameters: Type.Object({ agent: Type.String(), task: Type.String(), async: Type.Optional(Type.Boolean()) }),
	execute: async () => ({ content: [{ type: "text", text: "stub" }], details: {} }),
};

async function runSession(model: Model<any>, tools: any[] = [subagentTool]): Promise<unknown> {
	const modelRuntime = await ModelRuntime.create({ modelsPath: join(agentDir, "models.json"), refreshOnCreate: false });
	await modelRuntime.setRuntimeApiKey("serial_prov", "test");
	await modelRuntime.setRuntimeApiKey("par_prov", "test");

	const observed: unknown[] = [];
	const resourceLoader = new DefaultResourceLoader({
		cwd: agentDir,
		agentDir,
		// queue extension first (mutates async), observer second (records the post-mutation value).
		extensionFactories: [extension, (pi: any) => pi.on("tool_call", (e: any) => { if (e.toolName === "subagent") observed.push(e.input.async); })],
	});
	await resourceLoader.reload();

	const { session } = await createAgentSession({
		modelRuntime,
		model,
		resourceLoader,
		customTools: tools,
		noTools: "builtin",
		sessionManager: SessionManager.inMemory(),
	});
	try {
		await session.prompt("launch a subagent");
	} finally {
		session.dispose();
	}
	return observed[0];
}

describe("subagent launch serialisation (tool_call async rewrite)", () => {
	it("forces async:false on a single-slot (serialised) queue", async () => {
		const runtime = await ModelRuntime.create({ modelsPath: join(agentDir, "models.json"), refreshOnCreate: false });
		const model = runtime.getModel("serial_prov", "m")!;
		assert.equal(await runSession(model), false, "a serialised queue must rewrite async:true to false");
	});

	it("leaves async:true on a multi-slot (parallel) queue", async () => {
		const runtime = await ModelRuntime.create({ modelsPath: join(agentDir, "models.json"), refreshOnCreate: false });
		const model = runtime.getModel("par_prov", "m")!;
		assert.equal(await runSession(model), true, "a parallel queue must not rewrite async");
	});

	it("is a no-op when the subagent tool is absent (model calls a non-subagent tool)", async () => {
		const runtime = await ModelRuntime.create({ modelsPath: join(agentDir, "models.json"), refreshOnCreate: false });
		const model = runtime.getModel("serial_prov", "m")!;
		const otherTool = { ...subagentTool, name: "other" };
		nextToolName = "other";
		try {
			// No subagent tool registered; the model calls "other". The hook must not touch it.
			assert.equal(await runSession(model, [otherTool]), undefined, "no subagent tool → no async rewrite");
		} finally {
			nextToolName = "subagent";
		}
	});
});

after(() => {
	unregisterApiProviders("test-stub-subagent");
	spawnSync("pkill", ["-f", socketPath]);
});
