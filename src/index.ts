/**
 * pi-llm-queue pi extension.
 *
 * Tracer bullet: wrap configured providers so every model request waits for a
 * per-session slot from the local daemon before it reaches the endpoint, and
 * autostart the daemon on session start. Fail-open everywhere.
 */
import type {
	ExtensionAPI,
	ExtensionContext,
	ProviderConfig,
} from "@earendil-works/pi-coding-agent";
import {
	type Api,
	type AssistantMessageEventStream,
	createAssistantMessageEventStream,
	type Model,
	type SimpleStreamOptions,
	type TranscriptContext,
} from "@earendil-works/pi-ai/compat";
import { streamSimple as openaiCompletionsStreamSimple } from "@earendil-works/pi-ai/api/openai-completions";
import {
	defaultConfigPath,
	loadConfig,
	resolveQueueKey,
	resolveQueueSettings,
	type ResolvedConfig,
	type QueueSettings,
} from "./lib/config.ts";
import { LeaseClient, type Role } from "./lib/lease-client.ts";
import { ensureDaemon } from "./lib/autostart.ts";
import {
	acquireForRequest,
	releaseLease,
	resolveGroup,
	resolveRole,
	startHeartbeat,
	stopHeartbeat,
	type SessionLeaseState,
} from "./lib/gate.ts";

/** Grace period after a run settles: protects parent-woken-by-child wake chains. */
const SETTLE_GRACE_MS = 3000;

function errorStream(model: Model<Api>, message: string, aborted: boolean): AssistantMessageEventStream {
	const stream = createAssistantMessageEventStream();
	stream.push({
		type: "error",
		reason: aborted ? "aborted" : "error",
		error: {
			role: "assistant",
			content: [],
			api: model.api,
			provider: model.provider,
			model: model.id,
			usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
			stopReason: aborted ? "aborted" : "error",
			timestamp: Date.now(),
			errorMessage: message,
		},
	});
	stream.end();
	return stream;
}

export default function (pi: ExtensionAPI): void {
	const { config: initialConfig, error } = loadConfig(defaultConfigPath());
	let config: ResolvedConfig = initialConfig;
	if (error) queueMicrotask(() => pi.on("session_start", (_e, ctx) => ctx.ui.notify(`llm-queue: ${error}`, "warning")));

	const state: SessionLeaseState = { sessionKey: "", role: resolveRole(), group: resolveGroup() };
	const client = new LeaseClient(undefined, {
		failOpen: config.defaults.failOpen,
		onLeaseExpired: (leaseId) => {
			if (state.leaseId === leaseId) {
				stopHeartbeat(state);
				state.leaseId = undefined;
				state.queueKey = undefined;
			}
		},
	});

	let daemonChecked = false;
	let releaseTimer: NodeJS.Timeout | undefined;

	const cancelScheduledRelease = () => {
		if (releaseTimer) {
			clearTimeout(releaseTimer);
			releaseTimer = undefined;
		}
	};

	const releaseNow = () => {
		cancelScheduledRelease();
		void releaseLease({ client, state });
	};

	function queueKeyFor(model: Model<Api>): string | null {
		return resolveQueueKey(config, model.provider, model.baseUrl);
	}

	function settingsFor(model: Model<Api>): QueueSettings | null {
		return resolveQueueSettings(config, model.provider, model.baseUrl);
	}

	function makeStreamSimple(): NonNullable<ProviderConfig["streamSimple"]> {
		return (model: Model<Api>, context: TranscriptContext, options?: SimpleStreamOptions) => {
			if (model.api !== "openai-completions") {
				return errorStream(model, `pi-llm-queue: unsupported api ${model.api}`, false);
			}
			const queueKey = queueKeyFor(model);
			const settings = settingsFor(model);
			if (!queueKey || !settings) {
				return openaiCompletionsStreamSimple(model as Model<"openai-completions">, context, options);
			}
			const stream = createAssistantMessageEventStream();
			void (async () => {
				try {
					const grant = await acquireForRequest(
						{ client, state, settings, queueKey, onWarn: (message) => ctxNotify(message, "warning") },
						options?.signal,
					);
					if (!grant.granted) {
						const aborted = errorStream(model, "pi-llm-queue: request aborted while queued", true);
						for await (const event of aborted) stream.push(event);
						stream.end();
						return;
					}
					startHeartbeat(state, client, Math.max(1000, Math.floor(settings.leaseTtlMs / 3)));
					const inner = openaiCompletionsStreamSimple(model as Model<"openai-completions">, context, options);
					for await (const event of inner) stream.push(event);
					stream.end();
				} catch (inner) {
					const failed = errorStream(model, `pi-llm-queue: ${(inner as Error).message}`, false);
					for await (const event of failed) stream.push(event);
					stream.end();
				}
			})();
			return stream;
		};
	}

	let notifyCtx: ExtensionContext | undefined;
	function ctxNotify(message: string, level: "info" | "warning" | "error"): void {
		notifyCtx?.ui.notify(message, level);
	}

	function registerWrappers(ctx: ExtensionContext): void {
		const providers = new Map<string, { baseUrl?: string; apis: Set<string> }>();
		for (const model of ctx.modelRegistry.getAll()) {
			const entry = providers.get(model.provider) ?? { baseUrl: model.baseUrl, apis: new Set<string>() };
			entry.apis.add(model.api);
			if (model.baseUrl) entry.baseUrl = model.baseUrl;
			providers.set(model.provider, entry);
		}
		for (const [provider, entry] of providers) {
			if (!entry.apis.has("openai-completions")) continue;
			if (!resolveQueueKey(config, provider, entry.baseUrl)) continue;
			pi.registerProvider(provider, { api: "openai-completions", streamSimple: makeStreamSimple() });
		}
	}

	pi.on("session_start", async (event, ctx) => {
		notifyCtx = ctx;
		state.sessionKey = ctx.sessionManager.getSessionId();
		state.role = resolveRole();
		state.group = resolveGroup();
		const reloaded = loadConfig(defaultConfigPath());
		config = reloaded.config;
		registerWrappers(ctx);
		if (!daemonChecked) {
			daemonChecked = true;
			void ensureDaemon().then((result) => {
				if (!result.running) ctx.ui.notify("pi-llm-queue: daemon unavailable, requests pass through unqueued", "warning");
			});
		}
	});

	pi.on("agent_start", async () => {
		cancelScheduledRelease();
	});

	pi.on("agent_settled", async (event) => {
		if (event.aborted) {
			releaseNow();
			return;
		}
		cancelScheduledRelease();
		releaseTimer = setTimeout(() => {
			releaseTimer = undefined;
			void releaseLease({ client, state });
		}, SETTLE_GRACE_MS);
		releaseTimer.unref?.();
	});

	pi.on("session_shutdown", async () => {
		releaseNow();
		client.close();
	});

	pi.registerCommand("queue", {
		description: "Show pi-llm-queue slot holder, queue depth, and session-switch counts",
		handler: async (_name, ctx) => {
			const queues = await client.status();
			if (!queues) {
				ctx.ui.notify("pi-llm-queue: daemon unreachable (requests pass through unqueued)", "warning");
				return;
			}
			const lines = queues.map((queue) => {
				const holders = queue.holders.map((holder) => `${holder.sessionKey} (${holder.role})`).join(", ") || "none";
				return `${queue.key}\n  slot: ${holders}\n  waiting: parent ${queue.waiting.parent}, child ${queue.waiting.child}\n  switches: ${queue.switches}, grants: ${queue.grants}`;
			});
			const role: Role = state.role;
			const header = `pi-llm-queue — session ${state.sessionKey || "?"} as ${role}${state.leaseId ? ` holds ${state.leaseId}` : ""}`;
			ctx.ui.notify(lines.length ? `${header}\n${lines.join("\n")}` : `${header}\n  no queues`, "info");
		},
	});
}
