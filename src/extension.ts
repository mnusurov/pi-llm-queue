// pi-llm-queue extension: wraps configured providers so a session holds the endpoint's slot for
// its whole run, keeping the KV cache warm for one session at a time.
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
const MODELS_JSON_PATH = join(AGENT_DIR, "models.json");
const SOCKET_PATH = join(AGENT_DIR, "llm-queue.sock");
const DAEMON_PATH = new URL("daemon.js", import.meta.url).pathname;
// Wake chains (parent woken by a child) must not thrash the slot: spec says 2-5 s.
const GRACE_MS = 3000;

interface QueueEntry {
	maxStreams?: number;
}

function loadJson(path: string): Record<string, any> {
	try {
		return JSON.parse(readFileSync(path, "utf8"));
	} catch {
		return {};
	}
}

/**
 * Providers to wrap: queue keys naming a provider directly, and queue keys naming an
 * endpoint that a provider serves (providers on one endpoint share one queue).
 */
export function resolveQueueProviders(
	queues: Record<string, QueueEntry>,
	providers: Record<string, any>,
): string[] {
	const names = new Set<string>();
	for (const key of Object.keys(queues)) {
		if (providers[key]) names.add(key);
		else for (const [name, provider] of Object.entries(providers)) if (provider?.baseUrl === key) names.add(name);
	}
	return [...names];
}

export default function (pi: ExtensionAPI) {
	const modelsJson = loadJson(MODELS_JSON_PATH).providers ?? {};
	const queues = (loadJson(CONFIG_PATH).queues ?? {}) as Record<string, QueueEntry>;
	const providers = resolveQueueProviders(queues, modelsJson);
	if (!providers.length) return;

	const lease = new LeaseClient({
		socketPath: SOCKET_PATH,
		daemonPath: DAEMON_PATH,
		configPath: CONFIG_PATH,
		modelsJsonPath: MODELS_JSON_PATH,
	});
	const role: Role = process.env.PI_QUEUE_ROLE === "child" ? "child" : "parent";

	// shortcut: one lease per process; a process running several sessions needs per-session keys.
	let sessionKey = "";
	let heldQueueKey: string | null = null;
	let releaseTimer: ReturnType<typeof setTimeout> | null = null;

	const releaseSlot = () => {
		if (releaseTimer) {
			clearTimeout(releaseTimer);
			releaseTimer = null;
		}
		if (heldQueueKey && sessionKey) lease.release(heldQueueKey, sessionKey);
		heldQueueKey = null;
	};

	pi.on("session_start", (_event, ctx) => {
		sessionKey = ctx.sessionManager.getSessionId();
	});

	// Turn-based affinity: acquire at run start, release after the run settles (grace),
	// release at once on abort.
	pi.on("agent_start", async (_event, ctx) => {
		sessionKey = ctx.sessionManager.getSessionId();
		if (releaseTimer) {
			clearTimeout(releaseTimer); // a run starting inside the grace window keeps the slot
			releaseTimer = null;
		}
		const model = ctx.model;
		if (!model || !providers.includes(model.provider)) return;
		await lease.acquire(model.baseUrl, sessionKey, role);
		heldQueueKey = model.baseUrl;
		ctx.signal?.addEventListener("abort", releaseSlot, { once: true });
	});

	pi.on("agent_settled", () => {
		releaseTimer = setTimeout(releaseSlot, GRACE_MS);
		releaseTimer.unref();
	});

	pi.on("session_shutdown", () => releaseSlot());

	const queuedStream = (
		model: Model<Api>,
		context: TranscriptContext,
		options?: SimpleStreamOptions,
	): AssistantMessageEventStream => {
		const out = createAssistantMessageEventStream();
		const queueKey = model.baseUrl;
		const held = heldQueueKey === queueKey;
		(async () => {
			try {
				// Requests outside a run (compaction, classifier, nested calls) queue for themselves.
				if (!held) await lease.acquire(queueKey, sessionKey, role);
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
				if (!held) lease.release(queueKey, sessionKey);
			}
		})();
		return out;
	};

	for (const provider of providers) {
		pi.registerProvider(provider, {
			api: modelsJson[provider]?.api ?? "openai-completions",
			streamSimple: queuedStream,
		});
	}
}
