// pi-llm-queue extension: wraps configured providers so a session holds the endpoint's slot for
// its whole run, keeping the KV cache warm for one session at a time.
import { existsSync, readFileSync, renameSync, unlinkSync, watch, writeFileSync } from "node:fs";
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
import { LeaseClient, type Role, type StatusSession } from "./lease-client.ts";
import { loadValidConfig, DEFAULTS, keyedBeatMs, SCHEMA_VERSION } from "./config.js";
import { addressEndpoint, addressModel, keyAddressesEndpoint, matchingProviders, membersOf, queuedProviders, readProviders } from "./addressing.js";
import { RunLifecycle, SessionRegistry, type Clock, type LeasePort } from "./run-lifecycle.ts";

const AGENT_DIR = process.env.PI_CODING_AGENT_DIR ?? join(homedir(), ".pi", "agent");
const CONFIG_PATH = join(AGENT_DIR, "llm-queue.json");
const MODELS_JSON_PATH = join(AGENT_DIR, "models.json");
const SOCKET_PATH = join(AGENT_DIR, "llm-queue.sock");
// URL.pathname is percent-encoded: decode so paths with spaces/non-ASCII resolve.
const DAEMON_PATH = decodeURIComponent(new URL("daemon.js", import.meta.url).pathname);
// The only defaults field a Starter config writes: a Parent's queue-wait cap, above a long Child
// generation (the 2026-10-18 run held the Slot 11+ minutes) so a stuck queue warns visibly while
// the normal case stays silent. Children keep an unbounded wait — this cap is parent-only.
const STARTER_QUEUE_TIMEOUT_MS = 120000;

interface QueueEntry {
	maxStreams?: number;
	enabled?: boolean;
	/** Per-queue lease TTL: the daemon honours it, so the client's beats must too. */
	leaseTtlMs?: number;
	/** Per-queue idle-yield window: the client must beat at most half of it. */
	idleYieldMs?: number;
	/** BaseUrls and/or provider names that share this queue (overrides baseUrl keying). */
	match?: string[];
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
 * excluded, and a `defaults` block carrying only the parent queue-wait cap. Null when there is
 * nothing to queue: no self-hosted, non-native endpoint.
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
	return { schemaVersion: SCHEMA_VERSION, defaults: { queueTimeoutMs: STARTER_QUEUE_TIMEOUT_MS }, queues };
}

/**
 * Queue keys a Starter config would add to an existing file: self-hosted endpoints served by a
 * non-native provider that no existing entry addresses. Add-only — a key that is already present,
 * tuned or `enabled: false`, is left alone, and a cloud endpoint is never added, so the file's own
 * choices always win and a deliberate passthrough stays a deliberate passthrough.
 */
function reconcileStarterKeys(
	providers: Record<string, any>,
	queues: Record<string, QueueEntry>,
	isNativeOwned: (name: string) => boolean,
): string[] {
	const keys = Object.keys(queues ?? {});
	const added = new Set<string>();
	for (const [name, provider] of Object.entries(providers ?? {})) {
		const baseUrl = provider?.baseUrl;
		if (!baseUrl || isNativeOwned(name) || !isLocalEndpoint(baseUrl)) continue;
		// An endpoint an entry already addresses — its own key, a provider-name key naming it, or a
		// Group's match — is queued (or disabled on purpose); adding the baseUrl key would split it
		// into a second queue and take the Slot away from the user's entry.
		const covered = keys.some((key) => membersOf(key, queues[key]).some((member) => keyAddressesEndpoint(member, providers, baseUrl)));
		if (!covered) added.add(baseUrl);
	}
	return [...added].sort();
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
	const modelsJson = readProviders(MODELS_JSON_PATH);
	// Startup config load: any failure falls back to defaults and surfaces through ui.notify at
	// session_start (ui does not exist at module load).
	const loaded = loadValidConfig(CONFIG_PATH);
	let config = loaded.config;
	const configErrors = loaded.errors;
	let queues = (config.queues ?? {}) as Record<string, QueueEntry>;

	// The beat period a queue's Holder needs: its own leaseTtlMs and idleYieldMs, resolved exactly
	// as the daemon does (entry field → defaults field → DEFAULTS). Deriving it from `defaults` alone
	// lets a per-queue TTL lapse or a short idle window yield an actively beating Holder (#81).
	const queueBeatMs = (queueKey: string): number => {
		const entry = addressEndpoint(queueKey, queues, modelsJson).entry as QueueEntry | undefined;
		const ttl = entry?.leaseTtlMs ?? config.defaults?.leaseTtlMs ?? DEFAULTS.leaseTtlMs;
		const idleYield = entry?.idleYieldMs ?? config.defaults?.idleYieldMs ?? DEFAULTS.idleYieldMs;
		return keyedBeatMs(ttl, idleYield);
	};
	// The connection-wide bare renew refreshes every lease on the socket: it must run at the
	// smallest period any live lease could need, so it also covers a queue the defaults under-serve.
	const smallestBeatMs = (): number => {
		let min = Infinity;
		for (const [key, entry] of Object.entries(queues)) if (entry?.enabled !== false) min = Math.min(min, queueBeatMs(key));
		return Number.isFinite(min)
			? min
			: keyedBeatMs(config.defaults?.leaseTtlMs ?? DEFAULTS.leaseTtlMs, config.defaults?.idleYieldMs ?? DEFAULTS.idleYieldMs);
	};

	const lease = new LeaseClient({
		socketPath: SOCKET_PATH,
		daemonPath: DAEMON_PATH,
		configPath: CONFIG_PATH,
		modelsJsonPath: MODELS_JSON_PATH,
		heartbeatMs: smallestBeatMs(),
		failOpen: config.defaults?.failOpen,
		queueTimeoutMs: config.defaults?.queueTimeoutMs,
	});
	const envRole: Role = resolveRole(process.env);
	const group = process.env.PI_QUEUE_GROUP;
	// /queue promote|demote overrides the env-resolved tier per session (one process can run several).
	const roleOverride = new Map<string, Role>();
	const currentRole = (sessionKey: string): Role => roleOverride.get(sessionKey) ?? envRole;

	const queueKeyFor = (model: ExtensionContext["model"]): string | null =>
		model && providers.includes(model.provider) ? addressModel(model, queues, modelsJson).queueKey : null;

	// The per-session lease port: the LeaseClient still keys every operation by session, so the
	// session key, tier, and group are bound here and the lifecycle never resolves any of them.
	const leasePortFor = (sessionKey: string): LeasePort => ({
		acquire: (queueKey, signal, handlers) =>
			lease.acquire(queueKey, sessionKey, currentRole(sessionKey), group, signal, handlers?.onQueued, handlers?.onWaitCap),
		release: (queueKey) => lease.release(queueKey, sessionKey),
		beat: (queueKey) => lease.beat(queueKey, sessionKey),
	});

	// Clocks at the edge (CODING_STANDARDS): the lifecycle takes time as a port, so its unit tests
	// move time exactly. unref keeps a pending Grace/Beat timer from holding pi's process open.
	const clock: Clock = {
		now: () => Date.now(),
		setTimeout: (fn, ms) => {
			const timer = setTimeout(fn, ms);
			timer.unref();
			return timer;
		},
		clearTimeout: (timer) => clearTimeout(timer as ReturnType<typeof setTimeout>),
	};

	const noUi = { status() {}, notify() {} };
	const uiPort = (ui: ExtensionContext["ui"]) => ({
		status: (text: string | undefined) => ui.setStatus("llm-queue", text),
		notify: (message: string, level: "info" | "warning" | "error") => ui.notify(message, level),
	});
	const registry = new SessionRegistry(
		(sessionKey) =>
			new RunLifecycle({
				sessionKey,
				lease: leasePortFor(sessionKey),
				clock,
				ui: noUi,
				beatMs: queueBeatMs,
				// Read live on each tool start, so a config edit applies without a restart.
				blockingTools: () => config.defaults?.blockingTools ?? DEFAULTS.blockingTools,
			}),
	);

	let providers: string[] = [];
	// Only what we registered: unregistering a native-owned provider would drop the built-in.
	let registeredProviders = new Set<string>();
	let registered = false;

	const queuedStream = (
		model: Model<Api>,
		context: TranscriptContext,
		options?: SimpleStreamOptions,
	): AssistantMessageEventStream => {
		const out = createAssistantMessageEventStream();
		const queueKey = addressModel(model, queues, modelsJson).queueKey;
		// The agent tags every request with its session id, so two sessions in one process keep
		// separate leases; a request without one resolves to the Slot's holder, or the newest session.
		const sessionKey = options?.sessionId ?? registry.ownerFor(queueKey);
		registry
			.for(sessionKey)
			.withSlot(queueKey, options?.signal, async () => {
				const impl = model.api === "openai-completions" ? openAICompletionsApi() : getApiProvider(model.api);
				if (!impl) throw new Error(`pi-llm-queue: no stream implementation for api "${model.api}"`);
				for await (const event of impl.streamSimple(model, context, options)) out.push(event);
				out.end();
			})
			.catch((error) => {
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
			});
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
			// A session reads as `id tier [group]`: the tier it holds or queues as, and its group label.
			const label = (entry: StatusSession) => `${entry.sessionKey} ${entry.role}${entry.group ? ` ${entry.group}` : ""}`;
			const lines = s.queues.map((q) => {
				const depth = (role: Role) => q.waiting.filter((w) => w.role === role).length;
				const waiters = q.waiting.map(label).join(", ");
				return `${q.queueKey}: slot ${q.holders.map(label).join(", ") || "free"} (${q.holders.length}/${q.maxStreams === null ? "passthrough" : q.maxStreams}) · waiting parent ${depth("parent")}, child ${depth("child")}${waiters ? ` (${waiters})` : ""} · switches ${q.switches}`;
			});
			ctx.ui.notify(lines.length ? lines.join("\n") : "pi-llm-queue: no queues", "info");
		},
	});

	// Registration is deferred to the first session so ctx gives us the model registry
	// (native-provider detection) and ui.notify (unmatched keys, api mismatches).
	const registerQueuedProviders = (ctx: ExtensionContext) => {
		providers = queuedProviders(queues, modelsJson);

		// Queue keys naming no provider in models.json (directly, or through a group's match).
		for (const [key, entry] of Object.entries(queues)) {
			if (entry?.enabled === false) continue;
			if (!membersOf(key, entry).some((k) => matchingProviders(k, modelsJson).length))
				ctx.ui.notify(`pi-llm-queue: queue key "${key}" names no provider in models.json`, "warning");
		}

		const wrapped = new Set<string>();
		for (const provider of providers) {
			// Provider ids owned by native extensions (e.g. built-in llama) must not be wrapped.
			if (ctx.modelRegistry.getRegisteredNativeProvider?.(provider)) {
				ctx.ui.notify(`pi-llm-queue: "${provider}" is owned by a native extension, left unqueued`, "info");
				continue;
			}
			// Register with the api the wrapped models use; a model whose api differs streams
			// unqueued (composer matches streamSimple on api equality).
			const models = modelsJson[provider]?.models ?? [];
			const api = modelsJson[provider]?.api ?? models[0]?.api ?? "openai-completions";
			const offApi = [...new Set<string>(models.map((m: any) => m.api).filter(Boolean))].filter((a) => a !== api);
			if (offApi.length)
				ctx.ui.notify(`pi-llm-queue: "${provider}" models with api ${offApi.join(", ")} stream unqueued (queued for "${api}")`, "warning");
			pi.registerProvider(provider, { api, streamSimple: queuedStream });
			wrapped.add(provider);
		}
		for (const provider of registeredProviders) if (!wrapped.has(provider)) pi.unregisterProvider(provider);
		registeredProviders = wrapped;
	};

	// Hot-reload: a config edit re-resolves the wrapped provider set (pi applies provider
	// registration immediately); the daemon reloads the same file for queue policy. The client's
	// beat periods follow the same edit, and live beats restart under the new period (#81).
	// shortcut: failOpen and queueTimeoutMs are read once at connect and take effect only on pi
	// restart; the daemon hot-reloads its own policy.
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
				lease.setHeartbeatMs(smallestBeatMs());
				registry.restartBeats(); // a live holder is never evicted by a policy edit
				registerQueuedProviders(ctx);
				ctx.ui.notify("pi-llm-queue: config reloaded", "info");
			}, 250);
			debounce.unref();
		});
		watcher.on("error", () => {});
		watcher.unref();
	};

	// Fresh install: no config file means the queue is inert. Write a Starter built from the user's
	// own models.json so the very first launch queues, then reload it and register in this session.
	// An existing file is reconciled add-only (ADR 0001): a self-hosted endpoint with no Queue key
	// gains one, and nothing already in the file is modified, so manual tuning and an explicit
	// `enabled: false` stay stable across restarts.
	const scaffoldStarterConfig = (ctx: ExtensionContext) => {
		const isNativeOwned = (name: string) => !!ctx.modelRegistry.getRegisteredNativeProvider?.(name);
		let current: string | null;
		try {
			current = existsSync(CONFIG_PATH) ? readFileSync(CONFIG_PATH, "utf8") : null;
		} catch {
			return; // an unreadable config (a directory, EACCES) must not throw in session_start
		}
		if (current === null) {
			const starter = buildStarterConfig(modelsJson, isNativeOwned);
			if (!starter || !writeStarterConfig(null, starter)) return;
			if (!adoptStarterConfig()) return;
			const enabled = Object.values(queues).filter((entry) => entry?.enabled !== false).length;
			ctx.ui.notify(`pi-llm-queue: created ${CONFIG_PATH} — ${enabled} self-hosted endpoint(s) queued. Tune "enabled" / "maxStreams" there, see status with /queue.`, "info");
			return;
		}
		const loaded = loadValidConfig(CONFIG_PATH);
		// An invalid file is already reported by the config-rejected notify at session_start; writing
		// into it would bury that report, so reconcile leaves the file to the user.
		if (loaded.errors.length) return;
		const present = (loaded.config.queues ?? {}) as Record<string, QueueEntry>;
		const added = reconcileStarterKeys(modelsJson, present, isNativeOwned);
		if (!added.length) return;
		// Add-only: existing entries and the `defaults` block are carried over untouched.
		const reconciled = { ...loaded.config, queues: { ...present, ...Object.fromEntries(added.map((key) => [key, { enabled: true }])) } };
		if (!writeStarterConfig(current, reconciled)) return;
		if (!adoptStarterConfig()) return;
		ctx.ui.notify(
			`pi-llm-queue: added ${added.join(", ")} to ${CONFIG_PATH} — self-hosted endpoint(s) queued; nothing existing was changed. Tune "enabled" / "maxStreams" there, see status with /queue.`,
			"info",
		);
	};

	// Adopt the file just written: the same session must queue the created / reconciled keys, and
	// registerQueuedProviders runs right after this in session_start.
	const adoptStarterConfig = () => {
		const next = loadValidConfig(CONFIG_PATH);
		if (next.errors.length) return false;
		config = next.config;
		queues = (config.queues ?? {}) as Record<string, QueueEntry>;
		return true;
	};

	// Atomic write: tmp file, then rename. The guard is re-checked immediately before the rename —
	// `expected` is the content the write is based on (null for a create) — so a racing session_start
	// or a user edit wins and this write is dropped rather than clobbering it.
	const writeStarterConfig = (expected: string | null, data: unknown) => {
		const tmp = `${CONFIG_PATH}.tmp-${process.pid}`;
		try {
			writeFileSync(tmp, `${JSON.stringify(data, null, 2)}\n`);
			if ((existsSync(CONFIG_PATH) ? readFileSync(CONFIG_PATH, "utf8") : null) !== expected) return false;
			renameSync(tmp, CONFIG_PATH);
			return true;
		} catch {
			return false; // unwritable agent dir: stay inert as before
		} finally {
			try {
				unlinkSync(tmp);
			} catch {}
		}
	};

	pi.on("session_start", (_event, ctx) => {
		registry.for(ctx.sessionManager.getSessionId()).setUi(uiPort(ctx.ui));
		if (configErrors.length) ctx.ui.notify(`pi-llm-queue: config rejected, running with defaults:\n${configErrors.join("\n")}`, "error");
		if (!registered) {
			registered = true;
			scaffoldStarterConfig(ctx);
			registerQueuedProviders(ctx);
			watchConfig(ctx);
		}
	});

	pi.on("agent_start", async (_event, ctx) => {
		await registry.for(ctx.sessionManager.getSessionId()).startRun(queueKeyFor(ctx.model), ctx.signal);
	});

	// Blocking-tool nesting (#57): while a tool that waits on another agent or the user is in
	// flight, the run-scoped Beat pauses, so idle-yield hands the Slot to the very agent the Run
	// waits on. Compute tools keep beating. The set is config: defaults.blockingTools.
	pi.on("tool_execution_start", (event, ctx) =>
		registry.for(ctx.sessionManager.getSessionId()).parkOnTool(event.toolCallId, event.toolName),
	);
	pi.on("tool_execution_end", (event, ctx) => registry.for(ctx.sessionManager.getSessionId()).leaveTool(event.toolCallId));

	pi.on("agent_settled", (_event, ctx) => registry.for(ctx.sessionManager.getSessionId()).settleRun());

	pi.on("session_shutdown", (_event, ctx) => {
		const sessionKey = ctx.sessionManager.getSessionId();
		registry.for(sessionKey).shutdown();
		// The last session of this pi process says bye: the daemon counts this connection, so the
		// idle-exit arms at the word and a lingering process cannot keep stale daemon code alive.
		if (registry.forget(sessionKey)) lease.bye();
	});
}
