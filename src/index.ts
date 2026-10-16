/**
 * pi-llm-queue pi extension.
 *
 * Tracer bullet: wrap configured providers so every model request waits for a
 * per-session slot from the local daemon before it reaches the endpoint, and
 * autostart the daemon on session start. Fail-open everywhere.
 */
import type { ExtensionAPI, ExtensionContext, ProviderConfig } from "@earendil-works/pi-coding-agent";
import {
	type Api,
	type AssistantMessageEventStream,
	type Model,
	createAssistantMessageEventStream,
} from "@earendil-works/pi-ai/compat";
import { streamSimple as openaiCompletionsStreamSimple } from "@earendil-works/pi-ai/api/openai-completions";
import {
	defaultConfigPath,
	loadConfig,
	resolveQueueKey,
	type ResolvedConfig,
} from "./lib/config.ts";
import { LeaseClient } from "./lib/lease-client.ts";
import { ensureDaemon } from "./lib/autostart.ts";
import { releaseLease, resolveGroup, resolveRole, stopHeartbeat, type SessionLeaseState } from "./lib/gate.ts";
import { queuedStream, type PumpableStream, type WrapperModel } from "./lib/wrapper.ts";

/** Grace period after a run settles: protects parent-woken-by-child wake chains. */
const SETTLE_GRACE_MS = 3000;

function toPumpable(stream: AssistantMessageEventStream): PumpableStream {
	return stream as unknown as PumpableStream;
}

function makeErrorStream(model: WrapperModel, message: string, aborted: boolean): PumpableStream {
	const stream = toPumpable(createAssistantMessageEventStream());
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
			errorMessage: message,
			timestamp: Date.now(),
		},
	});
	stream.end();
	return stream;
}

export default function (pi: ExtensionAPI): void {
	const loaded = loadConfig(defaultConfigPath());
	let config: ResolvedConfig = loaded.config;

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

	let notifyCtx: ExtensionContext | undefined;
	const warn = (message: string) => notifyCtx?.ui.notify(message, "warning");
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

	const streamSimple: NonNullable<ProviderConfig["streamSimple"]> = (model, context, options) =>
		queuedStream(
			{
				client,
				state,
				config,
				delegate: (wrapped, ctx, opts) =>
					toPumpable(openaiCompletionsStreamSimple(wrapped as Model<"openai-completions">, ctx as never, opts as never)),
				createStream: () => toPumpable(createAssistantMessageEventStream()),
				errorStream: makeErrorStream,
				onWarn: warn,
			},
			{ api: model.api, provider: model.provider, id: model.id, baseUrl: model.baseUrl },
			context,
			options,
		) as unknown as AssistantMessageEventStream;

	/**
	 * Wrap every queued provider whose models speak openai-completions.
	 * Providers owned by another extension (native provider, OAuth login flow)
	 * are left alone: a legacy-config registration would replace their base.
	 */
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
			if (ctx.modelRegistry.getRegisteredNativeProvider(provider)) continue;
			if (ctx.modelRegistry.getRegisteredProviderConfig(provider)?.oauth) continue;
			pi.registerProvider(provider, { api: "openai-completions", streamSimple });
		}
	}

	pi.on("session_start", async (_event, ctx) => {
		notifyCtx = ctx;
		if (loaded.error) ctx.ui.notify(`llm-queue: ${loaded.error}`, "warning");
		state.sessionKey = ctx.sessionManager.getSessionId();
		state.role = resolveRole();
		state.group = resolveGroup();
		config = loadConfig(defaultConfigPath()).config;
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
			const header = `pi-llm-queue — session ${state.sessionKey || "?"} as ${state.role}${state.leaseId ? ` holds ${state.leaseId}` : ""}`;
			ctx.ui.notify(lines.length ? `${header}\n${lines.join("\n")}` : `${header}\n  no queues`, "info");
		},
	});
}
