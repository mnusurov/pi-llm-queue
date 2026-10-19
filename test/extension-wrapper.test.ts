// Provider-wrapper seam: the extension's registered streamSimple must serialise requests
// through the daemon. Uses a stub api provider (no real endpoint) and a real daemon.
import assert from "node:assert/strict";
import { describe, it, before, after } from "node:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAssistantMessageEventStream, registerApiProvider, unregisterApiProviders } from "@earendil-works/pi-ai/compat";
import { QUEUE_KEY, closeAllClients, connectClient, sleep, stopAllDaemons } from "./harness.ts";

const STUB_API = "stub-queue-api";

// Point the extension's module-level paths at a throwaway agent dir BEFORE importing it.
const agentDir = mkdtempSync(join(tmpdir(), "llm-queue-ext-"));
process.env.PI_CODING_AGENT_DIR = agentDir;
writeFileSync(join(agentDir, "llm-queue.json"), JSON.stringify({ defaults: { maxStreams: 1 }, queues: { stub: { maxStreams: 1 }, "free-api": { enabled: false } } }));
writeFileSync(join(agentDir, "models.json"), JSON.stringify({ providers: { stub: { baseUrl: QUEUE_KEY, api: STUB_API, models: [{ id: "m" }] } } }));
const socketPath = join(agentDir, "llm-queue.sock");
const DAEMON = decodeURIComponent(new URL("../src/daemon.js", import.meta.url).pathname);

const { default: extension } = await import("../src/extension.ts");

const model = { api: STUB_API, baseUrl: QUEUE_KEY, provider: "stub", id: "m" } as any;

function makePi() {
	const handlers: Record<string, ((e: any, ctx: any) => void)[]> = {};
	const registered: Record<string, any> = {};
	const commands: Record<string, any> = {};
	return {
		handlers,
		registered,
		commands,
		on: (event: string, handler: (e: any, ctx: any) => void) => (handlers[event] ??= []).push(handler),
		registerProvider: (name: string, config: any) => (registered[name] = config),
		unregisterProvider: (name: string) => delete registered[name],
		registerCommand: (name: string, config: any) => (commands[name] = config),
	};
}

function makeCtx(sessionId: string, signal?: AbortSignal, ui: any = { notify: () => {}, setStatus: () => {} }) {
	return {
		sessionManager: { getSessionId: () => sessionId },
		model,
		signal,
		modelRegistry: { getRegisteredNativeProvider: () => undefined },
		ui,
	};
}

const start = (pi: ReturnType<typeof makePi>, sessionId: string, signal?: AbortSignal, ui?: any) => {
	const ctx = makeCtx(sessionId, signal, ui);
	pi.handlers["session_start"].forEach((h) => h({}, ctx));
	return ctx;
};

const firstEvent = (stream: AsyncIterable<any>) => (async () => { for await (const e of stream) return e; })();

// The single queue the stub provider maps to, straight from the daemon.
const queueStatus = async () => {
	const raw = await connectClient(socketPath);
	raw.send({ op: "status" });
	const s = await raw.next(2000);
	raw.close();
	return s.queues.find((q: any) => q.queueKey === QUEUE_KEY);
};

describe("provider wrapper (queuedStream)", () => {
	let daemon: ChildProcess;

	// Run a daemon on the agent-dir socket up front so the extension's lease connects to a
	// live daemon (not fail-open) and serialises the streams.
	before(async () => {
		daemon = spawn(process.execPath, [DAEMON, "--socket", socketPath, "--config", join(agentDir, "llm-queue.json"), "--models-json", join(agentDir, "models.json")], { stdio: "ignore" });
		daemon.unref();
		const c = await connectClient(socketPath);
		c.close();
	});

	// Stub api: yields one event and ends. The blocking under test is the lease acquire, not this.
	registerApiProvider(
		{
			api: STUB_API,
			streamSimple: () => {
				const s = createAssistantMessageEventStream();
				s.push({
					type: "start",
					partial: { role: "assistant", content: [], api: STUB_API, provider: "stub", model: "m", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }, stopReason: "stop", timestamp: Date.now() },
				} as any);
				s.end();
				return s;
			},
		} as any,
		"test-stub",
	);

	it("serialises a second session's queuedStream behind the holder until release", async () => {
		const a = makePi();
		const b = makePi();
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "A");
		start(b, "B");

		// Session A holds the slot for its whole run.
		await a.handlers["agent_start"][0]({}, ctxA);

		const streamB = b.registered["stub"].streamSimple;
		const sB = streamB(model, {} as any); // B is not held → queues for itself behind A
		const eB = firstEvent(sB);
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(400).then(() => "blocked")]),
			"blocked",
			"B's stream must not start while A holds the slot",
		);

		a.handlers["session_shutdown"][0]({}, ctxA); // A releases its lease
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(3000).then(() => "blocked")]),
			"done",
			"B's stream proceeds once the slot frees",
		);
	});

	it("keeps two sessions in one process on separate leases (holder A, waiter B)", async () => {
		// One extension instance, two sessions: module-level singleton state would collapse them.
		const pi = makePi();
		extension(pi as any);
		const ctxA = start(pi, "ONE-A");
		const ctxB = start(pi, "ONE-B");
		await pi.handlers["agent_start"][0]({}, ctxA); // A acquires the slot
		const startedB = pi.handlers["agent_start"][0]({}, ctxB); // B queues behind A
		await sleep(200);

		let q = await queueStatus();
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["ONE-A"], "A holds the slot");
		assert.deepEqual(
			q.waiting.map((w: any) => w.sessionKey),
			["ONE-B"],
			"B queues behind A",
		);

		// A's held request beats under its own session key and streams at once — not re-queued under B.
		const eA = firstEvent(pi.registered["stub"].streamSimple(model, {} as any, { sessionId: "ONE-A" } as any));
		assert.equal(
			await Promise.race([eA.then(() => "done"), sleep(700).then(() => "blocked")]),
			"done",
			"A's held request streams immediately",
		);

		q = await queueStatus();
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["ONE-A"], "B did not evict A");

		pi.handlers["session_shutdown"][0]({}, ctxA); // A releases: B's turn
		await startedB;
		pi.handlers["session_shutdown"][0]({}, ctxB);
	});

	it("joins the run's in-flight acquire instead of double-queuing the session (#55)", async () => {
		// pi does not await agent_start, so a run's first request can land before the grant. It must
		// join the run's acquire; queueing again releases the slot after every stream (KV re-prefill).
		const pi = makePi();
		extension(pi as any);
		const ctxA = start(pi, "RACE-A");
		const ctxB = start(pi, "RACE-B");

		const startedA = pi.handlers["agent_start"][0]({}, ctxA); // in flight, not awaited
		const eA = firstEvent(pi.registered["stub"].streamSimple(model, {} as any, { sessionId: "RACE-A" } as any));
		const eB = firstEvent(pi.registered["stub"].streamSimple(model, {} as any, { sessionId: "RACE-B" } as any));
		await startedA;

		let q = await queueStatus();
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["RACE-A"], "A holds the slot");
		assert.deepEqual(
			q.waiting.map((w: any) => w.sessionKey),
			["RACE-B"],
			"only B queues: A did not enqueue a second time",
		);

		await eA; // A's request ran under the run's own lease
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(400).then(() => "blocked")]),
			"blocked",
			"B stays blocked while A's run holds the slot",
		);
		q = await queueStatus();
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["RACE-A"], "A's stream did not release the run's slot");

		pi.handlers["session_shutdown"][0]({}, ctxA); // A releases: B's turn
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(3000).then(() => "blocked")]),
			"done",
			"B proceeds once the run releases",
		);
		pi.handlers["session_shutdown"][0]({}, ctxB);
	});

	it("shows a footer indicator while queued and clears it when the slot is granted", async () => {
		const a = makePi();
		const b = makePi();
		const status: [string, string | undefined][] = [];
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "IND-A");
		const ctxB = start(b, "IND-B", undefined, {
			notify: () => {},
			setStatus: (key: string, text: string | undefined) => status.push([key, text]),
		});
		await a.handlers["agent_start"][0]({}, ctxA); // A holds the slot for its run

		const eB = firstEvent(b.registered["stub"].streamSimple(model, {} as any)); // B queues for itself
		await sleep(1500);
		const shown = status.filter(([, t]) => t).pop()?.[1] ?? "";
		assert.ok(shown, "the indicator appears while the session waits");
		assert.match(shown, /#1/, "the indicator reports the queue position");
		assert.match(shown, /\d+s/, "the indicator reports the elapsed wait");
		assert.match(shown, /IND-A/, "the indicator names the slot holder");
		a.handlers["session_shutdown"][0]({}, ctxA); // A releases: B's turn (a live holder is never preempted)
		assert.equal(await Promise.race([eB.then(() => "done"), sleep(4000).then(() => "blocked")]), "done");
		assert.equal(status[status.length - 1][1], undefined, "the indicator clears in sync with the grant");

		b.handlers["session_shutdown"][0]({}, ctxB);
	});

	it("streams immediately when the run already holds the slot (no self-queue)", async () => {
		const a = makePi();
		extension(a as any);
		const ctx = start(a, "S", new AbortController().signal);
		await a.handlers["agent_start"][0]({}, ctx); // acquires the slot for the run
		const stream = a.registered["stub"].streamSimple;

		const s = stream(model, {} as any);
		assert.equal(
			await Promise.race([firstEvent(s).then(() => "done"), sleep(500).then(() => "blocked")]),
			"done",
			"held request streams without queuing",
		);
		a.handlers["session_shutdown"][0]({}, ctx);
	});

	it("reports an error event when the stream implementation is missing", async () => {
		const a = makePi();
		extension(a as any);
		const ctx = start(a, "E");
		await a.handlers["agent_start"][0]({}, ctx); // holds QUEUE_KEY
		const stream = a.registered["stub"].streamSimple;
		const s = stream({ ...model, api: "no-such-api-xyz" }, {} as any); // held → no acquire; impl missing
		const ev = await firstEvent(s);
		assert.equal(ev.type, "error");
		a.handlers["session_shutdown"][0]({}, ctx);
	});

	it("releases the lease in finally when a non-held request errors", async () => {
		const a = makePi();
		extension(a as any);
		start(a, "R"); // no agent_start → the request queues for itself
		const stream = a.registered["stub"].streamSimple;
		const s = stream({ ...model, api: "no-such-api-xyz" }, {} as any); // acquires for R, impl missing
		assert.equal((await firstEvent(s)).type, "error");

		// The finally released R's lease, so a fresh session can take the slot.
		const b = makePi();
		extension(b as any);
		start(b, "T");
		const sB = b.registered["stub"].streamSimple(model, {} as any);
		assert.equal(
			await Promise.race([firstEvent(sB).then(() => "done"), sleep(3000).then(() => "blocked")]),
			"done",
			"R's lease was released on the error path",
		);
	});

	it("beats the lease on every held request, retaining the slot across tool executions", async () => {
		const a = makePi();
		const b = makePi();
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "KEEP");
		start(b, "STEAL");
		await a.handlers["agent_start"][0]({}, ctxA); // A holds the slot for its run

		const stream = a.registered["stub"].streamSimple;
		const sB = b.registered["stub"].streamSimple(model, {} as any); // queued behind A
		const eB = firstEvent(sB);

		// A keeps requesting for ~1.2 s while B waits: a live holder is never preempted (#49).
		for (let i = 0; i < 8; i++) {
			await firstEvent(stream(model, {} as any));
			await sleep(150);
		}
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(700).then(() => "blocked")]),
			"blocked",
			"an active run keeps the slot while a session waits",
		);
		a.handlers["session_shutdown"][0]({}, ctxA);
		await eB; // B proceeds once A releases
	});

	it("keeps the slot through the settle grace window, then releases it (wake-chain protection)", async () => {
		const a = makePi();
		const b = makePi();
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "GRACE");
		start(b, "GRACE-B");
		await a.handlers["agent_start"][0]({}, ctxA);
		await a.handlers["agent_settled"][0]({}, ctxA); // 3 s grace: a wake chain keeps the slot

		const eB = firstEvent(b.registered["stub"].streamSimple(model, {} as any));
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(300).then(() => "blocked")]),
			"blocked",
			"the grace window keeps the slot after the run settles",
		);

		// A parent woken by its child starts a new run inside the grace: the slot is kept.
		await a.handlers["agent_start"][0]({}, ctxA);
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(500).then(() => "blocked")]),
			"blocked",
			"a run starting inside the grace keeps the slot",
		);

		// The woken run settles with no further activity: the slot frees once the grace expires.
		await a.handlers["agent_settled"][0]({}, ctxA);
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(4000).then(() => "blocked")]),
			"done",
			"the slot is released once the grace window expires",
		);
		a.handlers["session_shutdown"][0]({}, ctxA);
	});

	it("keeps the slot when a request lands inside the settle grace window (#55)", async () => {
		const a = makePi();
		const b = makePi();
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "GRACE2");
		start(b, "GRACE2-B");
		await a.handlers["agent_start"][0]({}, ctxA);
		await a.handlers["agent_settled"][0]({}, ctxA); // 3 s grace armed

		const eB = firstEvent(b.registered["stub"].streamSimple(model, {} as any));
		await sleep(2000); // still inside the grace

		// A lone request (compaction/classifier) arrives in the grace: it must not have its slot
		// released mid-stream; the release is re-armed for the quiet period that follows (defect 3).
		await firstEvent(a.registered["stub"].streamSimple(model, {} as any, { sessionId: "GRACE2" } as any));
		await sleep(1500); // past the original deadline (~3 s from settle)
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(300).then(() => "blocked")]),
			"blocked",
			"the in-grace request kept the slot past the original grace deadline",
		);

		// No further activity: the re-armed grace releases the slot once the session goes quiet.
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(4000).then(() => "blocked")]),
			"done",
			"the re-armed grace releases the slot once the session goes quiet",
		);
		a.handlers["session_shutdown"][0]({}, ctxA);
	});

	it("releases the held slot immediately on abort", async () => {
		const ac = new AbortController();
		const a = makePi();
		const b = makePi();
		extension(a as any);
		extension(b as any);
		const ctxA = start(a, "ABORT", ac.signal);
		start(b, "ABORT-B");
		await a.handlers["agent_start"][0]({}, ctxA); // holds the slot

		const eB = firstEvent(b.registered["stub"].streamSimple(model, {} as any));
		assert.equal(await Promise.race([eB.then(() => "done"), sleep(400).then(() => "blocked")]), "blocked");
		ac.abort();
		assert.equal(
			await Promise.race([eB.then(() => "done"), sleep(3000).then(() => "blocked")]),
			"done",
			"Esc releases the held slot at once",
		);
	});

	it("re-queues when the slot is gone mid-run instead of streaming unqueued", async () => {
		const a = makePi();
		extension(a as any);
		const ctxA = start(a, "EVICTED");
		await a.handlers["agent_start"][0]({}, ctxA); // holds the slot
		const raw = await connectClient(socketPath);
		raw.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "RAW", role: "parent" }); // queued behind it

		// The lease is gone (a TTL lapse of a starved heartbeat, a disconnect release: the daemon
		// answers a beat ungranted). Here a release on its sessionKey takes the slot for RAW.
		raw.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "EVICTED" });
		assert.deepEqual(await raw.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "RAW" }, "the freed slot goes to the waiter");
		assert.deepEqual(await raw.next(3000), { op: "ok" });

		const stream = a.registered["stub"].streamSimple;
		const eA = firstEvent(stream(model, {} as any)); // its beat comes back ungranted: must re-queue
		for (let i = 0; i < 5; i++) {
			raw.send({ op: "renew", queueKey: QUEUE_KEY, sessionKey: "RAW" }); // RAW keeps its slot
			await sleep(150);
		}
		assert.equal(
			await Promise.race([eA.then(() => "done"), sleep(700).then(() => "blocked")]),
			"blocked",
			"a run that lost its slot re-queues behind the new holder instead of streaming unqueued",
		);

		raw.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "RAW" });
		assert.equal(
			await Promise.race([eA.then(() => "done"), sleep(3000).then(() => "blocked")]),
			"done",
			"the run that lost its slot proceeds once the slot frees",
		);
		raw.close();
		a.handlers["session_shutdown"][0]({}, ctxA);
	});
	it("prints passthrough for a disabled queue instead of a null slot count", async () => {
		const a = makePi();
		extension(a as any);
		start(a, "QUEUE");
		const raw = await connectClient(socketPath);
		raw.send({ op: "acquire", queueKey: "free-api", sessionKey: "FREE", role: "parent" }); // the passthrough queue
		assert.equal((await raw.next(3000)).op, "granted");

		const notes: string[] = [];
		const ctx = makeCtx("QUEUE", undefined, { notify: (text: string) => notes.push(text), setStatus: () => {} });
		await a.commands["queue"].handler("", ctx);
		assert.match(notes.join("\n"), /free-api: slot FREE parent \(1\/passthrough\)/, "status must not read as a daemon bug");

		raw.send({ op: "release", queueKey: "free-api", sessionKey: "FREE" });
		raw.close();
		a.handlers["session_shutdown"][0]({}, ctx);
	});

	it("renders tier and group label for Holders and Waiters in the status line", async () => {
		// The tier a session queues as comes from the env contract: pin it so the rendered line is
		// deterministic wherever the suite runs (a lane running these tests is itself a child).
		const savedRole = process.env.PI_QUEUE_ROLE;
		process.env.PI_QUEUE_ROLE = "parent";
		const a = makePi();
		extension(a as any);
		const ctx = start(a, "TIER-PARENT");
		await a.handlers["agent_start"][0]({}, ctx); // the parent session holds the slot for its run

		const raw = await connectClient(socketPath);
		raw.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "TIER-CHILD", role: "child", group: "scout" });
		await assert.rejects(raw.next(200), /no message from daemon within/, "the child waits behind the parent");

		const notes: string[] = [];
		const statusCtx = makeCtx("TIER-PARENT", undefined, { notify: (text: string) => notes.push(text), setStatus: () => {} });
		await a.commands["queue"].handler("", statusCtx);
		const line = notes.join("\n");
		assert.match(line, /TIER-PARENT parent/, "a Holder is named with its tier beside its session id");
		assert.match(line, /TIER-CHILD child scout/, "a Waiter is named with its tier and group label");

		raw.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "TIER-PARENT" });
		raw.close();
		a.handlers["session_shutdown"][0]({}, ctx);
		if (savedRole === undefined) delete process.env.PI_QUEUE_ROLE;
		else process.env.PI_QUEUE_ROLE = savedRole;
	});
});

describe("config hot-reload (provider set)", () => {
	it("unregisters a provider a config edit disables, so it passes through unqueued", async () => {
		const a = makePi();
		extension(a as any);
		start(a, "RELOAD");
		assert.ok(a.registered["stub"], "wrapped before the edit");

		writeFileSync(join(agentDir, "llm-queue.json"), JSON.stringify({ defaults: { maxStreams: 1 }, queues: { stub: { enabled: false }, "free-api": { enabled: false } } }));
		await sleep(600); // reload debounce
		assert.equal(a.registered["stub"], undefined, "a disabled provider is unregistered: passthrough, no pi restart");
	});
});

after(() => {
	unregisterApiProviders("test-stub");
	closeAllClients();
	stopAllDaemons();
	spawnSync("pkill", ["-f", socketPath]);
});
