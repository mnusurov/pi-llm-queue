// pi-llm-queue extension: wraps configured providers so a session holds the endpoint's slot for
// its whole run, keeping the KV cache warm for one session at a time.
import { existsSync, renameSync, unlinkSync, watch, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join, basename, dirname } from "node:path";
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
import { loadValidConfig, readJson, groupKeyFor, queueEntryFor, SCHEMA_VERSION } from "./config.js";

const AGENT_DIR = process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent");
const CONFIG_PATH = join(AGENT_DIR, "llm-queue.json");
const MODELS_JSON_PATH = join(AGENT_DIR, "models.json");
const SOCKET_PATH = join(AGENT_DIR, "llm-queue.sock");
// URL.pathname is percent-encoded: decode so paths with spaces/non-ASCII resolve.
const DAEMON_PATH = decodeURIComponent(new URL("daemon.js", import.meta.url).pathname);
// Wake chains (parent woken by a child) must not thrash the slot: spec says 2-5 s.
const GRACE_MS = 3000;
// Shown when a parent's queue wait exceeds queueTimeoutMs and the request passes through unqueued.
const TIMEOUT_WARNING = "pi-llm-queue: queue wait exceeded queueTimeoutMs — passing through unqueued";

interface QueueEntry {
	maxStreams?: number;
	enabled?: boolean;
	/** BaseUrls and/or provider names that share this queue (overrides baseUrl keying). */
	match?: string[];
	/** Force subagent launches to block the parent (async:false) so they serialise on this queue. */
	blockSubagentLaunch?: boolean;
}

const LOCAL_DOMAIN = /\.(local|lan|home\.arpa)$/;

/**
 * Self-hosted endpoint: loopback, RFC1918, link-local, or a local/mDNS domain. Public IPs and
 * other hostnames are cloud. A provider with no baseUrl (pure cloud API) is not self-hosted.
 */
export function isLocalEndpoint(baseUrl?: string): boolean {
	if (!baseUrl) return false;
	let host: string;
	try {
		host = new URL(baseUrl).hostname.replace(/^\[|\]$/g, "");
	} catch {
		return false;
	}
	if (host === "localhost" || LOCAL_DOMAIN.test(host)) return true;
	const v4 = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.\d{1,3}$/.exec(host);
	if (v4) {
		const [a, b] = [Number(v4[1]), Number(v4[2])];
		return a === 127 || a === 10 || (a === 192 && b === 168) || (a === 172 && b >= 16 && b <= 31) || (a === 169 && b === 254);
	}
	return host === "::1";
}

/**
 * Starter config for a fresh install: one queue per endpoint baseUrl (deduplicated, sorted),
 * self-hosted enabled with the default single slot, cloud as passthrough, native-owned providers
 * excluded. Null when there is nothing to scaffold: no self-hosted, non-native endpoint.
 */
export function buildStarterConfig(providers: Record<string, any>, isNativeOwned: (name: string) => boolean) {
	const collected: Record<string, { enabled: boolean }> = {};
	for (const [name, provider] of Object.entries(providers ?? {})) {
		const baseUrl = provider?.baseUrl;
		if (!baseUrl || baseUrl in collected || isNativeOwned(name)) continue;
		collected[baseUrl] = { enabled: isLocalEndpoint(baseUrl) };
	}
	const keys = Object.keys(collected).sort();
	if (!keys.some((key) => collected[key].enabled)) return null;
	const queues: Record<string, { enabled: boolean }> = {};
	for (const key of keys) queues[key] = collected[key];
	return { schemaVersion: SCHEMA_VERSION, queues };
}

/** Providers a queue key addresses: a provider named directly, or every provider serving that endpoint. */
function matchingProviders(key: string, providers: Record<string, any>): string[] {
	if (providers[key]) return [key];
	return Object.keys(providers).filter((name) => providers[name]?.baseUrl === key);
}

/**
 * Queue key for a model: the group whose `match` names its provider or endpoint (so one
 * endpoint can be split per provider, and several endpoints merged), else its baseUrl.
 * The daemon resolves the same rule through the shared `groupKeyFor`.
 */
export function resolveQueueKey(
	queues: Record<string, QueueEntry>,
	model: { provider: string; baseUrl: string },
	providers: Record<string, any> = {},
): string {
	return groupKeyFor(queues, providers, model);
}

/**
 * Providers to wrap: queue keys naming a provider directly, and queue keys naming an
 * endpoint that a provider serves (providers on one endpoint share one queue). A group's
 * `match` list contributes its providers the same way. Keys whose entry is `enabled: false`
 * are skipped.
 */
export function resolveQueueProviders(
	queues: Record<string, QueueEntry>,
	providers: Record<string, any>,
): string[] {
	const names = new Set<string>();
	for (const [key, entry] of Object.entries(queues)) {
		if (entry?.enabled === false) continue;
		for (const member of [key, ...(entry?.match ?? [])]) for (const provider of matchingProviders(member, providers)) names.add(provider);
	}
	return [...names];
}

/**
 * Whether a subagent launch on this model's queue must be forced to async:false.
 * Per-queue override, then defaults, then the single-slot default (maxStreams == 1 serialises).
 * The entry is resolved the way the daemon resolves policy: group key, endpoint key, then a
 * provider-name key naming the model's provider.
 */
export function resolveBlockSubagentLaunch(
	queues: Record<string, QueueEntry>,
	defaults: { maxStreams?: number; blockSubagentLaunch?: boolean } | undefined,
	model: { provider: string; baseUrl: string },
	providers: Record<string, any> = {},
): boolean {
	const entry = queueEntryFor(queues, providers, model);
	if (!entry || entry.enabled === false) return false; // no active queue (or passthrough): fully parallel, never blocks
	const maxStreams = entry.maxStreams ?? defaults?.maxStreams ?? 1;
	return entry.blockSubagentLaunch ?? defaults?.blockSubagentLaunch ?? maxStreams === 1;
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
	const modelsJson = readJson(MODELS_JSON_PATH).providers ?? {};
	// Startup config load: any failure falls back to defaults and surfaces through ui.notify at
	// session_start (ui does not exist at module load).
	const loaded = loadValidConfig(CONFIG_PATH);
	let config = loaded.config;
	const configErrors = loaded.errors;
	let queues = (config.queues ?? {}) as Record<string, QueueEntry>;
	// Heartbeat at a third of the lease TTL: a live session never loses its slot. The floor must stay
	// under the leaseTtlMs floor (1000 ms), or a sweep landing between two beats evicts a healthy holder.
	const heartbeatMs = Math.max(250, Math.floor((config.defaults?.leaseTtlMs ?? 30000) / 3));

	const lease = new LeaseClient({
		socketPath: SOCKET_PATH,
		daemonPath: DAEMON_PATH,
		configPath: CONFIG_PATH,
		modelsJsonPath: MODELS_JSON_PATH,
		heartbeatMs,
		failOpen: config.defaults?.failOpen,
		queueTimeoutMs: config.defaults?.queueTimeoutMs,
	});
	const envRole: Role = resolveRole(process.env);
	const group = process.env.PI_QUEUE_GROUP;
	// /queue promote|demote overrides the env-resolved tier per session (one process can run several).
	const roleOverride = new Map<string, Role>();
	const currentRole = (): Role => roleOverride.get(sessionKey) ?? envRole;

	// Footer indicator while this session waits for a slot: queue position + elapsed wait,
	// refreshed every second, cleared in sync with the grant.
	let ui: ExtensionContext["ui"] | null = null;
	let queuedInfo: { position: number; holders: string[]; startedAt: number } | null = null;
	let indicatorTimer: ReturnType<typeof setInterval> | null = null;

	const renderIndicator = () =>
		ui?.setStatus(
			"llm-queue",
			queuedInfo
				? `llm-queue #${queuedInfo.position} · ${Math.round((Date.now() - queuedInfo.startedAt) / 1000)}s · behind ${queuedInfo.holders.join(", ") || "?"}`
				: undefined,
		);

	const showQueued = (info: { position: number; holders: string[] }) => {
		queuedInfo = { ...info, startedAt: queuedInfo?.startedAt ?? Date.now() };
		indicatorTimer ??= setInterval(renderIndicator, 1000);
		indicatorTimer.unref();
		renderIndicator();
	};

	const clearQueued = () => {
		if (indicatorTimer) {
			clearInterval(indicatorTimer);
			indicatorTimer = null;
		}
		if (queuedInfo) {
			queuedInfo = null;
			renderIndicator();
		}
	};

	// shortcut: one lease per process; a process running several sessions needs per-session keys.
	let sessionKey = "";
	let heldQueueKey: string | null = null;
	let releaseTimer: ReturnType<typeof setTimeout> | null = null;
	let providers: string[] = [];
	// Only what we registered: unregistering a native-owned provider would drop the built-in.
	let registeredProviders = new Set<string>();
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
		const queueKey = resolveQueueKey(queues, model, modelsJson);
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
					const reason = await lease.acquire(queueKey, sessionKey, currentRole(), group, options?.signal, showQueued);
					clearQueued();
					if (reason === "timeout") ui?.notify(TIMEOUT_WARNING, "warning");
					if (reason === "granted") {
						if (wasHeld) heldQueueKey = queueKey; // the run re-took its slot
						else selfQueued = true;
					}
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
				// The stream is over: report completion so the daemon can switch the slot again. Without
				// this the lease stays in-flight until its TTL reclaims it, and a waiter never gets its turn.
				else if (heldQueueKey === queueKey) lease.beat(queueKey, sessionKey, true).catch(() => {});
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
			if (!s) {
				ctx.ui.notify("pi-llm-queue: daemon unreachable", "info");
				return;
			}
			const lines = s.queues.map((q: any) => {
				const depth = (role: string) => q.waiting.filter((w: any) => w.role === role).length;
				return `${q.queueKey}: slot ${q.holders.join(", ") || "free"} (${q.holders.length}/${q.maxStreams === null ? "passthrough" : q.maxStreams}) · waiting parent ${depth("parent")}, child ${depth("child")} · switches ${q.switches}`;
			});
			ctx.ui.notify(lines.length ? lines.join("\n") : "pi-llm-queue: no queues", "info");
		},
	});

	// Serialise subagent launches on single-slot queues: force async:false so the parent blocks
	// and launches queue one-at-a-time (batch = one workflow call → one wake). No-op when the
	// subagent tool is absent: tool_call never fires for a tool that isn't installed.
	pi.on("tool_call", (event, ctx) => {
		if (event.toolName !== "subagent") return;
		const model = ctx.model;
		if (!model) return;
		if (resolveBlockSubagentLaunch(queues, config.defaults, model, modelsJson) && event.input.async !== false)
			event.input.async = false;
	});

	// Registration is deferred to the first session so ctx gives us the model registry
	// (native-provider detection) and ui.notify (unmatched keys, api mismatches).
	const registerQueuedProviders = (ctx: ExtensionContext) => {
		providers = resolveQueueProviders(queues, modelsJson);

		// Queue keys naming no provider in models.json (directly, or through a group's match) (finding 7).
		for (const [key, entry] of Object.entries(queues)) {
			if (entry?.enabled === false) continue;
			if (![key, ...(entry?.match ?? [])].some((k) => matchingProviders(k, modelsJson).length))
				ctx.ui.notify(`pi-llm-queue: queue key "${key}" names no provider in models.json`, "warning");
		}

		const wrapped = new Set<string>();
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
			wrapped.add(provider);
		}
		for (const provider of registeredProviders) if (!wrapped.has(provider)) pi.unregisterProvider(provider);
		registeredProviders = wrapped;
	};

	// Hot-reload: a config edit re-resolves the wrapped provider set (pi applies provider
	// registration immediately); the daemon reloads the same file for queue policy.
	// shortcut: leaseTtlMs (client heartbeat period), failOpen and queueTimeoutMs are read once at
	// connect and take effect only on pi restart; the daemon hot-reloads its own leaseTtlMs.
	const watchConfig = (ctx: ExtensionContext) => {
		let debounce: ReturnType<typeof setTimeout> | null = null;
		// Watch the directory, not the file: an atomic-save editor replaces the file and a
		// file watcher goes dead; a config created after startup needs a watcher too.
		const watcher = watch(dirname(CONFIG_PATH), (_event, file) => {
			if (file && file !== basename(CONFIG_PATH)) return;
			clearTimeout(debounce ?? undefined);
			debounce = setTimeout(() => {
				if (!existsSync(CONFIG_PATH)) return; // config deleted: keep the current wrap set
				const next = loadValidConfig(CONFIG_PATH);
				if (next.errors.length) {
					// A half-written file (invalid JSON) is a silent no-op: the last good config is kept.
					if (!next.invalidJson) ctx.ui.notify(`pi-llm-queue: config not reloaded:\n${next.errors.join("\n")}`, "error");
					return;
				}
				config = next.config;
				queues = (config.queues ?? {}) as Record<string, QueueEntry>;
				registerQueuedProviders(ctx);
				ctx.ui.notify("pi-llm-queue: config reloaded", "info");
			}, 250);
			debounce.unref();
		});
		watcher.on("error", () => {});
		watcher.unref();
	};

	// Fresh install: no config file means the queue is inert. Write a starter built from the user's
	// own models.json so the very first launch queues, then reload it and register in this session.
	const scaffoldStarterConfig = (ctx: ExtensionContext) => {
		if (existsSync(CONFIG_PATH)) return;
		const starter = buildStarterConfig(modelsJson, (name) => !!ctx.modelRegistry.getRegisteredNativeProvider?.(name));
		if (!starter) return;
		const tmp = `${CONFIG_PATH}.tmp-${process.pid}`;
		try {
			writeFileSync(tmp, `${JSON.stringify(starter, null, 2)}\n`);
			// rename overwrites, so the create-if-missing guard is checked right before it; a racing
			// first-launch is last-writer-wins with identical content.
			if (existsSync(CONFIG_PATH)) return;
			renameSync(tmp, CONFIG_PATH);
		} catch {
			return; // unwritable agent dir: stay inert as before
		} finally {
			try {
				unlinkSync(tmp);
			} catch {}
		}
		const next = loadValidConfig(CONFIG_PATH);
		if (next.errors.length) return;
		config = next.config;
		queues = (config.queues ?? {}) as Record<string, QueueEntry>;
		const enabled = Object.values(queues).filter((entry) => entry?.enabled !== false).length;
		ctx.ui.notify(`pi-llm-queue: created ${CONFIG_PATH} — ${enabled} self-hosted endpoint(s) queued. Tune "enabled" / "maxStreams" there, see status with /queue.`, "info");
	};

	pi.on("session_start", (_event, ctx) => {
		sessionKey = ctx.sessionManager.getSessionId();
		ui = ctx.ui;
		if (configErrors.length) ctx.ui.notify(`pi-llm-queue: config rejected, running with defaults:\n${configErrors.join("\n")}`, "error");
		if (!registered) {
			registered = true;
			scaffoldStarterConfig(ctx);
			registerQueuedProviders(ctx);
			watchConfig(ctx);
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
		const queueKey = resolveQueueKey(queues, model, modelsJson);
		const reason = await lease.acquire(queueKey, sessionKey, currentRole(), group, ctx.signal, showQueued);
		clearQueued();
		if (reason === "timeout") ctx.ui.notify(TIMEOUT_WARNING, "warning");
		// Only a grant claims a lease: a timeout/abort/fail-open must not mark the slot held.
		if (reason === "granted") heldQueueKey = queueKey;
	});

	pi.on("agent_settled", () => {
		releaseTimer = setTimeout(releaseSlot, GRACE_MS);
		releaseTimer.unref();
	});

	pi.on("session_shutdown", () => {
		releaseSlot();
		clearQueued(); // a shutdown while queued must not leave the indicator up
	});
}
