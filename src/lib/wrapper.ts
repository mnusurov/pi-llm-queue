/**
 * The provider-wrapper gate, kept free of pi imports so it is testable with a
 * stub client and a stub delegate. `src/index.ts` supplies the real stream
 * factory and the built-in openai-completions delegate.
 */
import type { LeaseClient } from "./lease-client.ts";
import type { ResolvedConfig, QueueSettings } from "./config.ts";
import { resolveQueueKey, resolveQueueSettings } from "./config.ts";
import { acquireForRequest, startHeartbeat, type SessionLeaseState } from "./gate.ts";

/** Minimal shape of pi's AssistantMessageEventStream that the wrapper uses. */
export interface PumpableStream {
	push(event: unknown): void;
	end(): void;
	[Symbol.asyncIterator](): AsyncIterator<unknown>;
}

export type StreamDelegate = (model: WrapperModel, context: unknown, options: WrapperOptions | undefined) => PumpableStream;

export interface WrapperModel {
	api: string;
	provider: string;
	id: string;
	baseUrl?: string;
}

export interface WrapperOptions {
	signal?: AbortSignal;
}

export interface WrapperDeps {
	client: LeaseClient;
	state: SessionLeaseState;
	config: ResolvedConfig;
	delegate: StreamDelegate;
	createStream: () => PumpableStream;
	errorStream: (model: WrapperModel, message: string, aborted: boolean) => PumpableStream;
	onWarn?: (message: string) => void;
}

function settingsFor(deps: WrapperDeps, model: WrapperModel): { queueKey: string; settings: QueueSettings } | null {
	const queueKey = resolveQueueKey(deps.config, model.provider, model.baseUrl);
	if (!queueKey) return null;
	const settings = resolveQueueSettings(deps.config, model.provider, model.baseUrl);
	return settings ? { queueKey, settings } : null;
}

async function pump(from: PumpableStream, to: PumpableStream): Promise<void> {
	for await (const event of from) to.push(event);
	to.end();
}

/**
 * Queue the request for a slot, then delegate to the real stream. Never
 * rejects: a lost/aborted grant becomes an aborted assistant message, so a
 * queue problem can never hard-fail a run.
 */
export function queuedStream(deps: WrapperDeps, model: WrapperModel, context: unknown, options?: WrapperOptions): PumpableStream {
	const target = settingsFor(deps, model);
	if (model.api !== "openai-completions") return deps.errorStream(model, `pi-llm-queue: unsupported api ${model.api}`, false);
	if (!target) return deps.delegate(model, context, options);
	const out = deps.createStream();
	void (async () => {
		try {
			const grant = await acquireForRequest(
				{ client: deps.client, state: deps.state, settings: target.settings, queueKey: target.queueKey, onWarn: deps.onWarn },
				options?.signal,
			);
			if (!grant.granted) {
				const aborted = grant.aborted === true;
				await pump(
					deps.errorStream(
						model,
						aborted ? "pi-llm-queue: request left the queue before it was granted" : "pi-llm-queue: no slot available and failOpen is false",
						aborted,
					),
					out,
				);
				return;
			}
			startHeartbeat(deps.state, deps.client, Math.max(1000, Math.floor(target.settings.leaseTtlMs / 3)));
			await pump(deps.delegate(model, context, options), out);
		} catch (error) {
			await pump(deps.errorStream(model, `pi-llm-queue: ${(error as Error).message}`, false), out);
		}
	})();
	return out;
}
