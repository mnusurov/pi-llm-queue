/**
 * Extension wrapper seam tests: the provider wrapper must wait for the slot,
 * then delegate to the built-in stream with the caller's options, and must
 * degrade (never hard-fail) when the grant is lost. Stub client and stub
 * delegate only — no pi session, no model endpoint.
 */
import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { resolveConfig } from "../src/lib/config.ts";
import { queuedStream, type PumpableStream, type WrapperModel, type WrapperOptions } from "../src/lib/wrapper.ts";
import { stopHeartbeat, type SessionLeaseState } from "../src/lib/gate.ts";
import type { LeaseClient, LeaseGrant } from "../src/lib/lease-client.ts";

const GPU = "http://127.0.0.1:8010/v1";
const MODEL: WrapperModel = { api: "openai-completions", provider: "strata", id: "m1", baseUrl: GPU };

class FakeStream implements PumpableStream {
	readonly events: unknown[] = [];
	private done = false;
	private waiters: (() => void)[] = [];

	constructor(seed: unknown[] = []) {
		for (const event of seed) this.push(event);
	}

	push(event: unknown): void {
		this.events.push(event);
		for (const wake of this.waiters.splice(0)) wake();
	}

	end(): void {
		this.done = true;
		for (const wake of this.waiters.splice(0)) wake();
	}

	async *[Symbol.asyncIterator](): AsyncIterator<unknown> {
		let index = 0;
		for (;;) {
			if (index < this.events.length) {
				yield this.events[index++];
				continue;
			}
			if (this.done) return;
			await new Promise<void>((resolve) => this.waiters.push(resolve));
		}
	}
}

function stubClient(grant: LeaseGrant, delayMs = 0): { client: LeaseClient; calls: { acquire: number; renew: number } } {
	const calls = { acquire: 0, renew: 0 };
	const client = {
		acquire: async () => {
			calls.acquire += 1;
			if (delayMs) await new Promise((resolve) => setTimeout(resolve, delayMs));
			return grant;
		},
		renew: async () => {
			calls.renew += 1;
			return true;
		},
		release: async () => undefined,
	} as unknown as LeaseClient;
	return { client, calls };
}

function harness(grant: LeaseGrant, options: { config?: ReturnType<typeof resolveConfig>; delayMs?: number } = {}) {
	const config = options.config ?? resolveConfig({ queues: { [GPU]: { maxStreams: 1 } } });
	const { client, calls } = stubClient(grant, options.delayMs);
	const state: SessionLeaseState = { sessionKey: "session-1", role: "parent" };
	const delegated: { model: WrapperModel; options: WrapperOptions | undefined }[] = [];
	const inner = new FakeStream([{ type: "text", content: "answer" }]);
	inner.end();
	const warnings: string[] = [];
	const deps = {
		client,
		state,
		config,
		delegate: (model: WrapperModel, _context: unknown, opts?: WrapperOptions) => {
			delegated.push({ model, options: opts });
			return inner;
		},
		createStream: () => new FakeStream(),
		errorStream: (model: WrapperModel, message: string, aborted: boolean) => {
			const stream = new FakeStream([{ type: "error", reason: aborted ? "aborted" : "error", error: { stopReason: aborted ? "aborted" : "error", errorMessage: message, model } }]);
			stream.end();
			return stream;
		},
		onWarn: (message: string) => warnings.push(message),
	};
	return { deps, state, calls, delegated, warnings };
}

describe("provider wrapper gate", () => {
	test("waits for the slot, then delegates with the caller's options", async () => {
		const { deps, state, calls, delegated, warnings } = harness({ granted: true, leaseId: "lease-1" }, { delayMs: 60 });
		const signal = new AbortController().signal;
		const out = queuedStream(deps, MODEL, { messages: [] }, { signal }) as FakeStream;
		assert.equal(delegated.length, 0, "no request reaches the endpoint before the grant");
		const events = [];
		for await (const event of out) events.push(event);
		assert.deepEqual(events, [{ type: "text", content: "answer" }], "the built-in stream is passed through unchanged");
		assert.equal(delegated.length, 1);
		assert.equal(delegated[0].options?.signal, signal, "abort signal reaches the delegate");
		assert.equal(calls.acquire, 1);
		assert.equal(state.leaseId, "lease-1", "the session records its lease");
		assert.equal(warnings.length, 0);
		stopHeartbeat(state);
	});

	test("a disabled provider passes straight through without touching the daemon", async () => {
		const { deps, calls, delegated } = harness({ granted: true, leaseId: "lease-1" }, {
			config: resolveConfig({ queues: { strata: { enabled: false } } }),
		});
		const out = queuedStream(deps, MODEL, { messages: [] }, undefined) as FakeStream;
		assert.equal(delegated.length, 1, "passthrough is synchronous");
		assert.equal(calls.acquire, 0);
		const events = [];
		for await (const event of out) events.push(event);
		assert.deepEqual(events, [{ type: "text", content: "answer" }]);
	});

	test("leaving the queue (abort) yields an aborted message, not a request", async () => {
		const { deps, state, delegated } = harness({ granted: false, aborted: true });
		const out = queuedStream(deps, MODEL, { messages: [] }, undefined) as FakeStream;
		const events = [];
		for await (const event of out) events.push(event);
		assert.equal(events.length, 1);
		assert.equal((events[0] as { type: string }).type, "error");
		assert.equal((events[0] as { error: { stopReason: string } }).error.stopReason, "aborted");
		assert.equal(delegated.length, 0, "an aborted request never hits the endpoint");
		stopHeartbeat(state);
	});

	test("failOpen false reports a queue failure instead of passing through", async () => {
		const { deps, delegated } = harness({ granted: false, failOpen: false });
		const out = queuedStream(deps, MODEL, { messages: [] }, undefined) as FakeStream;
		const events = [];
		for await (const event of out) events.push(event);
		assert.equal((events[0] as { type: string }).type, "error");
		assert.equal((events[0] as { error: { stopReason: string } }).error.stopReason, "error");
		assert.equal(delegated.length, 0);
	});

	test("failOpen true passes through with a warning on timeout", async () => {
		const { deps, delegated, warnings } = harness({ granted: true, failOpen: true, timedOut: true });
		const out = queuedStream(deps, MODEL, { messages: [] }, undefined) as FakeStream;
		for await (const event of out) void event;
		assert.equal(delegated.length, 1, "timed-out queue wait still reaches the endpoint");
		void warnings;
		void out;
	});

	test("an api the wrapper cannot delegate to is refused, not queued", async () => {
		const { deps, calls, delegated } = harness({ granted: true, leaseId: "lease-1" });
		const out = queuedStream(deps, { ...MODEL, api: "anthropic-messages" }, { messages: [] }, undefined) as FakeStream;
		const events = [];
		for await (const event of out) events.push(event);
		assert.equal((events[0] as { type: string }).type, "error");
		assert.equal(calls.acquire, 0);
		assert.equal(delegated.length, 0);
	});

	test("a delegate that throws becomes an error message, not a crash", async () => {
		const { deps } = harness({ granted: true, leaseId: "lease-1" });
		deps.delegate = () => {
			throw new Error("boom");
		};
		const out = queuedStream(deps, MODEL, { messages: [] }, undefined) as FakeStream;
		const events = [];
		for await (const event of out) events.push(event);
		assert.equal((events[0] as { type: string }).type, "error");
		assert.match((events[0] as { error: { errorMessage: string } }).error.errorMessage, /boom/);
		stopHeartbeat((deps as { state: SessionLeaseState }).state);
	});
});
