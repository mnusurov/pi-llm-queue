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
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { LeaseClient, type Role } from "./lease-client.ts";

const AGENT_DIR = process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent");
const CONFIG_PATH = join(AGENT_DIR, "llm-queue.json");
const MODELS_JSON_PATH = join(AGENT_DIR, "models.json");
const SOCKET_PATH = join(AGENT_DIR, "llm-queue.sock");
// URL.pathname is percent-encoded: decode so paths with spaces/non-ASCII resolve.
const DAEMON_PATH = decodeURIComponent(new URL("daemon.js", import.meta.url).pathname);
// Wake chains (parent woken by a child) must not thrash the slot: spec says 2-5 s.
const GRACE_MS = 3000;

interface QueueEntry {
	maxStreams?: number;
	enabled?: boolean;
}

function loadJson(path: string): Record<string, any> {
	try {
		return JSON.parse(readFileSync(path, "utf8"));
	} catch {
		return {};
	}
}

/** A queue key matches a provider named directly, or an endpoint a provider serves. */
function keyMatches(key: string, providers: Record<string, any>): boolean {
	if (providers[key]) return true;
	return Object.values(providers).some((p: any) => p?.baseUrl === key);
}

/**
 * Providers to wrap: queue keys naming a provider directly, and queue keys naming an
 * endpoint that a provider serves (providers on one endpoint share one queue).
 * Keys whose entry is `enabled: false` are skipped.
 */
export function resolveQueueProviders(
	queues: Record<string, QueueEntry>,
	providers: Record<string, any>,
): string[] {
	const names = new Set<string>();
	for (const [key, entry] of Object.entries(queues)) {
		if (entry?.enabled === false) continue;
		if (providers[key]) names.add(key);
		else for (const [name, provider] of Object.entries(providers)) if (provider?.baseUrl === key) names.add(name);
	}
	return [...names];
}

/**
 * Role from the neutral env contract, read as plain strings (zero imports, so the queue
 * works with or without pi-subagents): explicit PI_QUEUE_ROLE wins, then a pi-subagents
 * child hint, then default parent.
 */
export function resolveRole(env: Record<string, string | undefined>): Role {
	const explicit = env.PI_QUEUE_ROLE;
	if (explicit === "child" || explicit === "parent") return explicit;
	if (env.PI_SUBAGENT_CHILD === "1" || env.PI_SUBAGENT_PARENT_SESSION) return "child";
	return "parent";
}

export default function (pi: ExtensionAPI) {
	const modelsJson = loadJson(MODELS_JSON_PATH).providers ?? {};
	const config = loadJson(CONFIG_PATH);
	const queues = (config.queues ?? {}) as Record<string, QueueEntry>;
	// Heartbeat at a third of the lease TTL (floor 1 s): a live session never loses its slot.
	const heartbeatMs = Math.max(1000, Math.floor((config.defaults?.leaseTtlMs ?? 30000) / 3));

	const lease = new LeaseClient({
		socketPath: SOCKET_PATH,
		daemonPath: DAEMON_PATH,
		configPath: CONFIG_PATH,
		modelsJsonPath: MODELS_JSON_PATH,
		heartbeatMs,
	});
	const envRole: Role = resolveRole(process.env);
	const group = process.env.PI_QUEUE_GROUP;
	// /queue promote|demote overrides the env-resolved tier per session (one process can run several).
	const roleOverride = new Map<string, Role>();
	const currentRole = (): Role => roleOverride.get(sessionKey) ?? envRole;

	// shortcut: one lease per process; a process running several sessions needs per-session keys.
	let sessionKey = "";
	let heldQueueKey: string | null = null;
	let releaseTimer: ReturnType<typeof setTimeout> | null = null;
	let providers: string[] = [];
	let registered = false;

	const releaseSlot = () => {
		if (releaseTimer) {
			clearTimeout(releaseTimer);
			releaseTimer = null;
		}
		if (heldQueueKey && sessionKey) lease.release(heldQueueKey, sessionKey);
		heldQueueKey = null;
	};

	const queuedStream = (
		model: Model<Api>,
		context: TranscriptContext,
		options?: SimpleStreamOptions,
	): AssistantMessageEventStream => {
		const out = createAssistantMessageEventStream();
		const queueKey = model.baseUrl;
		const wasHeld = heldQueueKey === queueKey;
		(async () => {
			let selfQueued = false;
			try {
				// Requests outside a run (compaction, classifier, nested calls) queue for themselves
				// and release when done. A held request beats its lease: the run keeps the slot
				// across tool executions. A beat answered ungranted means the daemon switched the
				// slot away (idle/monopoly quantum) — the run re-queues before it streams again.
				if (wasHeld && (await lease.beat(queueKey, sessionKey))) {
					// slot retained for the run
				} else {
					await lease.acquire(queueKey, sessionKey, currentRole(), group, options?.signal);
					if (wasHeld) heldQueueKey = queueKey; // the run re-took its slot
					else selfQueued = true;
				}
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
				if (selfQueued) lease.release(queueKey, sessionKey);
			}
		})();
		return out;
	};

	// /queue [promote|demote]: promote/demote this session's tier, or print queue status.
	pi.registerCommand("queue", {
		description: "Queue status, or promote/demote this session's tier",
		handler: async (args: string, ctx: ExtensionContext) => {
			const arg = args.trim().toLowerCase();
			if (arg === "promote" || arg === "demote") {
				const role: Role = arg === "promote" ? "parent" : "child";
				const sid = ctx.sessionManager.getSessionId();
				roleOverride.set(sid, role);
				lease.setRole(sid, role); // re-tier a request already queued behind a long run
				ctx.ui.notify(`pi-llm-queue: session tier is now ${role}`, "info");
				return;
			}
			const s = await lease.status();
			ctx.ui.notify(s ? JSON.stringify(s.queues) : "pi-llm-queue: daemon unreachable", "info");
		},
	});

	// Registration is deferred to the first session so ctx gives us the model registry
	// (native-provider detection) and ui.notify (unmatched keys, api mismatches).
	const registerQueuedProviders = (ctx: ExtensionContext) => {
		providers = resolveQueueProviders(queues, modelsJson);

		// Queue keys naming a provider absent from models.json (finding 7).
		for (const key of Object.keys(queues)) {
			if (queues[key]?.enabled === false) continue;
			if (!keyMatches(key, modelsJson)) ctx.ui.notify(`pi-llm-queue: queue key "${key}" names no provider in models.json`, "warning");
		}

		for (const provider of providers) {
			// Provider ids owned by native extensions (e.g. built-in llama) must not be wrapped (finding 8).
			if (ctx.modelRegistry.getRegisteredNativeProvider?.(provider)) {
				ctx.ui.notify(`pi-llm-queue: "${provider}" is owned by a native extension, left unqueued`, "info");
				continue;
			}
			// Register with the api the wrapped models use; a model whose api differs
			// bypasses the queue (composer matches streamSimple on api equality) (finding 6).
			const models = modelsJson[provider]?.models ?? [];
			const api = modelsJson[provider]?.api ?? models[0]?.api ?? "openai-completions";
			const offApi = [...new Set<string>(models.map((m: any) => m.api).filter(Boolean))].filter((a) => a !== api);
			if (offApi.length)
				ctx.ui.notify(`pi-llm-queue: "${provider}" models with api ${offApi.join(", ")} bypass the queue (queued for "${api}")`, "warning");
			pi.registerProvider(provider, { api, streamSimple: queuedStream });
		}
	};

	pi.on("session_start", (_event, ctx) => {
		sessionKey = ctx.sessionManager.getSessionId();
		if (!registered) {
			registered = true;
			registerQueuedProviders(ctx);
		}
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
		// Attach the abort path before awaiting: Esc while queued must cancel (finding 3).
		ctx.signal?.addEventListener("abort", releaseSlot, { once: true });
		await lease.acquire(model.baseUrl, sessionKey, currentRole(), group, ctx.signal);
		// An abort already released the slot: do not claim a lease we no longer hold.
		if (!ctx.signal?.aborted) heldQueueKey = model.baseUrl;
	});

	pi.on("agent_settled", () => {
		releaseTimer = setTimeout(releaseSlot, GRACE_MS);
		releaseTimer.unref();
	});

	pi.on("session_shutdown", () => releaseSlot());
}
