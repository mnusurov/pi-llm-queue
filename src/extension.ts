// pi-llm-queue extension: wraps configured providers so every model request of a session
// waits for the endpoint's slot, keeping the KV cache warm for one session at a time.
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { createAssistantMessageEventStream, getApiProvider, openAICompletionsApi } from "@earendil-works/pi-ai/compat";
import type {
	Api,
	AssistantMessageEventStream,
	Model,
	SimpleStreamOptions,
	TranscriptContext,
} from "@earendil-works/pi-ai/compat";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { LeaseClient, type Role } from "./lease-client.ts";

const AGENT_DIR = process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent");
const CONFIG_PATH = join(AGENT_DIR, "llm-queue.json");
const SOCKET_PATH = join(AGENT_DIR, "llm-queue.sock");
const DAEMON_PATH = new URL("daemon.js", import.meta.url).pathname;

interface QueueEntry {
	maxStreams?: number;
	blockSubagentLaunch?: boolean;
}

function loadJson(path: string): Record<string, any> {
	try {
		return JSON.parse(readFileSync(path, "utf8"));
	} catch {
		return {};
	}
}

function loadQueues(): Record<string, QueueEntry> {
	return (loadJson(CONFIG_PATH).queues ?? {}) as Record<string, QueueEntry>;
}

export default function (pi: ExtensionAPI) {
	const queues = loadQueues();
	// Queue keys are provider names (wrapped here) or endpoint baseUrls (daemon-side policy).
	// Only providers declared in models.json can be wrapped: that is where their api is known.
	const modelsJson = loadJson(join(AGENT_DIR, "models.json")).providers ?? {};
	const providers = Object.keys(queues).filter((key) => !key.includes("://") && modelsJson[key]);
	if (!providers.length) return;

	const lease = new LeaseClient({ socketPath: SOCKET_PATH, daemonPath: DAEMON_PATH, configPath: CONFIG_PATH });
	const role: Role = process.env.PI_QUEUE_ROLE === "child" ? "child" : "parent";
	let sessionKey = process.env.PI_QUEUE_SESSION ?? `pid-${process.pid}`;

	const rememberSession = (_: unknown, ctx: { sessionManager: { getSessionId(): string } }) => {
		sessionKey = ctx.sessionManager.getSessionId();
	};
	pi.on("session_start", rememberSession);
	pi.on("agent_start", rememberSession);

	const queuedStream = (
		model: Model<Api>,
		context: TranscriptContext,
		options?: SimpleStreamOptions,
	): AssistantMessageEventStream => {
		const out = createAssistantMessageEventStream();
		const queueKey = model.baseUrl;
		(async () => {
			try {
				await lease.acquire(queueKey, sessionKey, role);
				const impl = model.api === "openai-completions" ? openAICompletionsApi() : getApiProvider(model.api);
				if (!impl) throw new Error(`pi-llm-queue: no stream implementation for api "${model.api}"`);
				for await (const event of impl.streamSimple(model, context, options)) out.push(event);
				out.end();
			} catch (error) {
				out.push({
					type: "error",
					reason: "error",
					error: {
						role: "assistant",
						content: [],
						api: model.api,
						provider: model.provider,
						model: model.id,
						usage: {
							input: 0,
							output: 0,
							cacheRead: 0,
							cacheWrite: 0,
							totalTokens: 0,
							cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
						},
						stopReason: "error",
						errorMessage: error instanceof Error ? error.message : String(error),
						timestamp: Date.now(),
					},
				});
				out.end();
			} finally {
				lease.release(queueKey, sessionKey);
			}
		})();
		return out;
	};

	for (const provider of providers) {
		pi.registerProvider(provider, {
			api: modelsJson[provider].api ?? "openai-completions",
			streamSimple: queuedStream,
		});
	}
}
