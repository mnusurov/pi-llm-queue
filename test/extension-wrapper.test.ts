// Provider-wrapper seam: the extension's registered streamSimple must serialise requests
// through the daemon. Uses a stub api provider (no real endpoint) and a real daemon.
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { closeAllClients, disposeAllExtensions, firstEvent, pollUntil, QUEUE_KEY, race, sleep, startExtension } from "./extension-harness.ts";

const STUB_API = "stub-queue-api";
const QUEUE_KEY2 = "http://model2.test/v1";

// daemonIdleExitMs 1e9: the suite reuses one daemon fixture; session_shutdown says bye (#63) and
// the production default 0 would kill the fixture between tests.
const CONFIG = { defaults: { maxStreams: 1, daemonIdleExitMs: 1000000000 }, queues: { stub: { maxStreams: 1 }, stub2: { maxStreams: 1 }, "free-api": { enabled: false } } };
const MODELS = { stub: { baseUrl: QUEUE_KEY, api: STUB_API, models: [{ id: "m" }] }, stub2: { baseUrl: QUEUE_KEY2, api: STUB_API, models: [{ id: "m" }] } };

const startExt = () => startExtension({ config: CONFIG, models: MODELS });

const model = { api: STUB_API, baseUrl: QUEUE_KEY, provider: "stub", id: "m" } as any;
const model2 = { api: STUB_API, baseUrl: QUEUE_KEY2, provider: "stub2", id: "m" } as any;

describe("provider wrapper (queuedStream)", () => {
	it("serialises a second session's queuedStream behind the holder until release", async () => {
		const a = startExt();
		const b = startExt();
		const sa = a.startSession("A");
		const sb = b.startSession("B");

		await sa.dispatch("agent_start");

		const eB = firstEvent(sb.stream(model)); // B is not held → queues for itself behind A
		assert.equal(await race(eB, 400), "blocked", "B's stream must not start while A holds the slot");

		await sa.dispatch("session_shutdown"); // A releases its lease
		assert.equal(await race(eB, 3000), "done", "B's stream proceeds once the slot frees");
	});

	it("keeps two sessions in one process on separate leases (holder A, waiter B)", async () => {
		// One extension instance, two sessions: module-level singleton state would collapse them.
		const ext = startExt();
		const sa = ext.startSession("ONE-A");
		const sb = ext.startSession("ONE-B");
		await sa.dispatch("agent_start"); // A acquires the slot
		const startedB = sb.dispatch("agent_start"); // B queues behind A
		await sleep(200);

		let q = await ext.status(QUEUE_KEY);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["ONE-A"], "A holds the slot");
		assert.deepEqual(
			q.waiting.map((w: any) => w.sessionKey),
			["ONE-B"],
			"B queues behind A",
		);

		// A's held request beats under its own session key and streams at once — not re-queued under B.
		const eA = firstEvent(sa.stream(model));
		assert.equal(await race(eA, 700), "done", "A's held request streams immediately");

		q = await ext.status(QUEUE_KEY);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["ONE-A"], "B did not evict A");

		await sa.dispatch("session_shutdown"); // A releases: B's turn
		await startedB;
		await sb.dispatch("session_shutdown");
	});

	it("joins the run's in-flight acquire instead of double-queuing the session (#55)", async () => {
		// pi does not await agent_start, so a run's first request can land before the grant. It must
		// join the run's acquire; queueing again releases the slot after every stream (KV re-prefill).
		const ext = startExt();
		const sa = ext.startSession("RACE-A");
		const sb = ext.startSession("RACE-B");

		const startedA = sa.dispatch("agent_start"); // in flight, not awaited
		const eA = firstEvent(sa.stream(model));
		const eB = firstEvent(sb.stream(model));
		await startedA;

		let q = await ext.status(QUEUE_KEY);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["RACE-A"], "A holds the slot");
		assert.deepEqual(
			q.waiting.map((w: any) => w.sessionKey),
			["RACE-B"],
			"only B queues: A did not enqueue a second time",
		);

		await eA; // A's request ran under the run's own lease
		assert.equal(await race(eB, 400), "blocked", "B stays blocked while A's run holds the slot");
		q = await ext.status(QUEUE_KEY);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["RACE-A"], "A's stream did not release the run's slot");

		await sa.dispatch("session_shutdown"); // A releases: B's turn
		assert.equal(await race(eB, 3000), "done", "B proceeds once the run releases");
		await sb.dispatch("session_shutdown");
	});

	it("shows a footer indicator while queued and clears it when the slot is granted", async () => {
		const a = startExt();
		const b = startExt();
		const sa = a.startSession("IND-A");
		const sb = b.startSession("IND-B");
		await sa.dispatch("agent_start"); // A holds the slot for its run

		const eB = firstEvent(sb.stream(model)); // B queues for itself
		// The client's first status report fires at 300 ms (lease-client.ts): poll for the indicator with
		// the 1500 ms bound the sleep used; the asserts below are the ones that sleep preceded.
		await pollUntil(() => sb.statuses().some(([, text]) => text), 1500, "waiting for the queue indicator to appear");
		const shown = sb.statuses().filter(([, text]) => text).pop()?.[1] ?? "";
		assert.ok(shown, "the indicator appears while the session waits");
		assert.match(shown, /#1/, "the indicator reports the queue position");
		assert.match(shown, /\d+s/, "the indicator reports the elapsed wait");
		assert.match(shown, /IND-A/, "the indicator names the slot holder");
		await sa.dispatch("session_shutdown"); // A releases: B's turn (a live holder is never preempted)
		assert.equal(await race(eB, 4000), "done");
		const statuses = sb.statuses();
		assert.equal(statuses[statuses.length - 1][1], undefined, "the indicator clears in sync with the grant");

		await sb.dispatch("session_shutdown");
	});

	it("streams immediately when the run already holds the slot (no self-queue)", async () => {
		const ext = startExt();
		const session = ext.startSession("S", { signal: new AbortController().signal });
		await session.dispatch("agent_start"); // acquires the slot for the run

		assert.equal(await race(firstEvent(session.stream(model)), 500), "done", "held request streams without queuing");
		await session.dispatch("session_shutdown");
	});

	it("reports an error event when the stream implementation is missing", async () => {
		const ext = startExt();
		const session = ext.startSession("E");
		await session.dispatch("agent_start"); // holds QUEUE_KEY
		const event = await firstEvent(session.stream({ ...model, api: "no-such-api-xyz" }));
		assert.equal(event.type, "error");
		await session.dispatch("session_shutdown");
	});

	it("releases the lease in finally when a non-held request errors", async () => {
		const a = startExt();
		const sa = a.startSession("R"); // no agent_start → the request queues for itself
		const error = await firstEvent(sa.stream({ ...model, api: "no-such-api-xyz" })); // acquires for R, impl missing
		assert.equal(error.type, "error");

		// The finally released R's lease, so a fresh session can take the slot.
		const b = startExt();
		const sb = b.startSession("T");
		assert.equal(await race(firstEvent(sb.stream(model)), 3000), "done", "R's lease was released on the error path");
	});

	it("beats the lease on every held request, retaining the slot across tool executions", async () => {
		const a = startExt();
		const b = startExt();
		const sa = a.startSession("KEEP");
		const sb = b.startSession("STEAL");
		await sa.dispatch("agent_start"); // A holds the slot for its run

		const eB = firstEvent(sb.stream(model)); // queued behind A

		// A keeps requesting for ~1.2 s while B waits: a live holder is never preempted (#49).
		for (let i = 0; i < 8; i++) {
			await firstEvent(sa.stream(model));
			await sleep(150);
		}
		assert.equal(await race(eB, 700), "blocked", "an active run keeps the slot while a session waits");
		await sa.dispatch("session_shutdown");
		await eB; // B proceeds once A releases
	});

	it("keeps the slot through the settle grace window, then releases it (wake-chain protection)", async () => {
		const a = startExt();
		const b = startExt();
		const sa = a.startSession("GRACE");
		const sb = b.startSession("GRACE-B");
		await sa.dispatch("agent_start");
		await sa.dispatch("agent_settled"); // 3 s grace: a wake chain keeps the slot

		const eB = firstEvent(sb.stream(model));
		assert.equal(await race(eB, 300), "blocked", "the grace window keeps the slot after the run settles");

		// A parent woken by its child starts a new run inside the grace: the slot is kept.
		await sa.dispatch("agent_start");
		assert.equal(await race(eB, 500), "blocked", "a run starting inside the grace keeps the slot");

		// The woken run settles with no further activity: the slot frees once the grace expires.
		await sa.dispatch("agent_settled");
		assert.equal(await race(eB, 4000), "done", "the slot is released once the grace window expires");
		await sa.dispatch("session_shutdown");
	});

	it("releases the old queue's lease when a run switches models inside the settle grace (#81)", async () => {
		// A settle arms the grace on model A's queue. A new run on model B inside that window must
		// release A's lease first: leaving it behind keeps A's beat alive for the process lifetime,
		// blocking every other session behind a holder that is not running.
		const a = startExt();
		const sa = a.startSession("SWITCH-A");
		await sa.dispatch("agent_start"); // holds QUEUE_KEY
		await sa.dispatch("agent_settled"); // grace armed

		sa.model = model2;
		await sa.dispatch("agent_start"); // switch inside the grace → QUEUE_KEY2

		const oldQueue = await a.status(QUEUE_KEY);
		assert.deepEqual(oldQueue.holders, [], "the old queue's lease is released before the new run takes the slot");

		// A second session can take the old queue at once instead of queueing behind a ghost holder.
		const b = startExt();
		const sb = b.startSession("SWITCH-B");
		assert.equal(await race(firstEvent(sb.stream(model)), 3000), "done", "the old endpoint is free for another session");

		await sa.dispatch("session_shutdown");
		await sb.dispatch("session_shutdown");
	});

	it("keeps the slot when a request lands inside the settle grace window (#55)", async () => {
		const a = startExt();
		const b = startExt();
		const sa = a.startSession("GRACE2");
		const sb = b.startSession("GRACE2-B");
		await sa.dispatch("agent_start");
		await sa.dispatch("agent_settled"); // 3 s grace armed

		const eB = firstEvent(sb.stream(model));
		await sleep(2000); // still inside the grace

		// A lone request (compaction/classifier) arrives in the grace: it must not have its slot
		// released mid-stream; the release is re-armed for the quiet period that follows (defect 3).
		await firstEvent(sa.stream(model));
		await sleep(1500); // past the original deadline (~3 s from settle)
		assert.equal(await race(eB, 300), "blocked", "the in-grace request kept the slot past the original grace deadline");

		// No further activity: the re-armed grace releases the slot once the session goes quiet.
		assert.equal(await race(eB, 4000), "done", "the re-armed grace releases the slot once the session goes quiet");
		await sa.dispatch("session_shutdown");
	});

	it("releases the held slot immediately on abort", async () => {
		const ac = new AbortController();
		const a = startExt();
		const b = startExt();
		const sa = a.startSession("ABORT", { signal: ac.signal });
		const sb = b.startSession("ABORT-B");
		await sa.dispatch("agent_start"); // holds the slot

		const eB = firstEvent(sb.stream(model));
		assert.equal(await race(eB, 400), "blocked");
		ac.abort();
		assert.equal(await race(eB, 3000), "done", "Esc releases the held slot at once");
	});

	it("re-queues when the slot is gone mid-run instead of streaming unqueued", async () => {
		const ext = startExt();
		const session = ext.startSession("EVICTED");
		await session.dispatch("agent_start"); // holds the slot
		const raw = await ext.connect();
		raw.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "RAW", role: "parent" }); // queued behind it

		// The lease is gone (a TTL lapse of a starved heartbeat, a disconnect release: the daemon
		// answers a beat ungranted). Here a release on its sessionKey takes the slot for RAW.
		raw.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "EVICTED" });
		assert.deepEqual(await raw.next(3000), { op: "granted", queueKey: QUEUE_KEY, sessionKey: "RAW" }, "the freed slot goes to the waiter");
		assert.deepEqual(await raw.next(3000), { op: "ok" });

		const eA = firstEvent(session.stream(model)); // its beat comes back ungranted: must re-queue
		for (let i = 0; i < 5; i++) {
			raw.send({ op: "renew", queueKey: QUEUE_KEY, sessionKey: "RAW" }); // RAW keeps its slot
			await sleep(150);
		}
		assert.equal(await race(eA, 700), "blocked", "a run that lost its slot re-queues behind the new holder instead of streaming unqueued");

		raw.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "RAW" });
		assert.equal(await race(eA, 3000), "done", "the run that lost its slot proceeds once the slot frees");
		raw.close();
		await session.dispatch("session_shutdown");
	});

	it("prints passthrough for a disabled queue instead of a null slot count", async () => {
		const ext = startExt();
		const session = ext.startSession("QUEUE");
		const raw = await ext.connect();
		raw.send({ op: "acquire", queueKey: "free-api", sessionKey: "FREE", role: "parent" }); // the passthrough queue
		assert.equal((await raw.next(3000)).op, "granted");

		await session.command("queue", "");
		assert.match(session.notifications().map((n) => n.message).join("\n"), /free-api: slot FREE parent \(1\/passthrough\)/, "status must not read as a daemon bug");

		raw.send({ op: "release", queueKey: "free-api", sessionKey: "FREE" });
		raw.close();
		await session.dispatch("session_shutdown");
	});

	it("renders tier and group label for Holders and Waiters in the status line", async () => {
		// The tier a session queues as comes from the env contract: pin it so the rendered line is
		// deterministic wherever the suite runs (a lane running these tests is itself a child).
		const a = startExtension({ config: CONFIG, models: MODELS, env: { PI_QUEUE_ROLE: "parent" } });
		try {
			const session = a.startSession("TIER-PARENT");
			await session.dispatch("agent_start"); // the parent session holds the slot for its run

			const raw = await a.connect();
			raw.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey: "TIER-CHILD", role: "child", group: "scout" });
			await assert.rejects(raw.next(200), /no message from daemon within/, "the child waits behind the parent");

			await session.command("queue", "");
			const line = session.notifications().map((n) => n.message).join("\n");
			assert.match(line, /TIER-PARENT parent/, "a Holder is named with its tier beside its session id");
			assert.match(line, /TIER-CHILD child scout/, "a Waiter is named with its tier and group label");

			raw.send({ op: "release", queueKey: QUEUE_KEY, sessionKey: "TIER-PARENT" });
			raw.close();
			await session.dispatch("session_shutdown");
		} finally {
			a.dispose();
		}
	});
});

describe("config hot-reload (provider set)", () => {
	it("unregisters a provider a config edit disables, so it passes through unqueued", async () => {
		const ext = startExt();
		ext.startSession("RELOAD");
		assert.ok(ext.providers()["stub"], "wrapped before the edit");

		ext.writeConfig({ defaults: { maxStreams: 1, daemonIdleExitMs: 1000000000 }, queues: { stub: { enabled: false }, "free-api": { enabled: false } } });
		// The config watcher debounces 250 ms (extension.ts watchConfig): poll for the unregistration.
		await pollUntil(() => ext.providers()["stub"] === undefined, 3000, "waiting for the config reload to unregister the provider");
		assert.equal(ext.providers()["stub"], undefined, "a disabled provider is unregistered: passthrough, no pi restart");
	});
});

after(() => {
	closeAllClients();
	disposeAllExtensions();
});
