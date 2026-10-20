// Bye on session_shutdown: the extension announces the departure to the daemon, so the daemon
// exits the moment the last session shuts down — while the pi process is still alive. The
// socket-close fallback (#60) only fires when the process dies; a pi that lingers after its last
// session (slow teardown, hung shutdown) must not keep stale daemon code resident, because the
// next pi would then connect to the old daemon instead of autostarting a fresh one.
// Real daemon + stub api + the pi Extension harness (ADR 0006).
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { closeAllClients, disposeAllExtensions, exited, QUEUE_KEY, startExtension } from "./extension-harness.ts";

const STUB_API = "stub-bye-api";

// No daemonIdleExitMs: the production default 0 — exit at once when the last counted client leaves.
const CONFIG = { defaults: { maxStreams: 1 }, queues: { stub: { maxStreams: 1 } } };
const MODELS = { stub: { baseUrl: QUEUE_KEY, api: STUB_API, models: [{ id: "m" }] } };

describe("bye on session_shutdown (#63): the daemon exits while pi is still alive", () => {
	it("the last session's shutdown exits the daemon with the client process still alive", async () => {
		const ext = startExtension({ config: CONFIG, models: MODELS });
		await ext.ready();
		const session = ext.startSession("BYE-A");
		await session.dispatch("agent_start"); // acquire: the connection becomes a counted client

		const q = await ext.status(QUEUE_KEY);
		assert.deepEqual(q.holders.map((h: any) => h.sessionKey), ["BYE-A"], "A holds the Slot before shutdown");

		await session.dispatch("session_shutdown"); // the extension says bye and closes the lease socket
		assert.ok(await exited(ext.daemon!, 2000), "bye exits the daemon while this test process — standing in for pi — is alive");
	});
});

after(() => {
	closeAllClients();
	disposeAllExtensions();
});
