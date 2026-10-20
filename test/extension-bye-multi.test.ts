// Bye only on the last session (#92): one pi process runs several sessions on one lease
// connection, so a non-last session's shutdown must not announce the process's departure.
// Real daemon + stub api + the pi Extension harness (ADR 0006).
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { closeAllClients, disposeAllExtensions, exited, QUEUE_KEY, sleep, startExtension } from "./extension-harness.ts";

const STUB_API = "stub-bye-multi-api";
// No daemonIdleExitMs: the production default 0 — exit at once when the last counted client leaves.
const CONFIG = { defaults: { maxStreams: 1 }, queues: { stub: { maxStreams: 1 } } };
const MODELS = { stub: { baseUrl: QUEUE_KEY, api: STUB_API, models: [{ id: "m" }] } };

describe("bye is the last session's word (#92)", () => {
	it("a non-last session's shutdown keeps the daemon up", async () => {
		const ext = startExtension({ config: CONFIG, models: MODELS });
		await ext.ready();
		const a = ext.startSession("BYE-B1");
		const b = ext.startSession("BYE-B2");
		await a.dispatch("agent_start"); // the connection becomes a counted client

		await a.dispatch("session_shutdown");
		await sleep(300);
		assert.equal(ext.daemon!.exitCode, null, "the daemon stays up while another session remains");

		await b.dispatch("session_shutdown");
		assert.ok(await exited(ext.daemon!, 2000), "the last session's shutdown says bye");
	});
});

after(() => {
	closeAllClients();
	disposeAllExtensions();
});
