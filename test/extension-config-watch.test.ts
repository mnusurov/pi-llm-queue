// The README install path: the config file is created for you on first launch.
// A config created after pi starts must be picked up by the running session (audit finding 4).
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { closeAllClients, disposeAllExtensions, QUEUE_KEY, sleep, startExtension } from "./extension-harness.ts";

const STUB_API = "stub-watch-api";

describe("config created after startup", () => {
	it("wraps the provider once the config file appears, without restarting pi", async () => {
		const ext = startExtension({ config: null, models: { stub: { baseUrl: QUEUE_KEY, api: STUB_API, models: [{ id: "m" }] } }, daemon: false });
		ext.startSession("LATE");
		assert.equal(ext.providers()["stub"], undefined, "no config at startup: nothing is queued");

		const config = { defaults: { maxStreams: 1 }, queues: { stub: { maxStreams: 1 } } };
		ext.writeConfig(config);
		// fs.watch delivery latency varies with system load (macOS FSEvents coalesces), so poll the
		// reload window and re-save while waiting: the wrap must appear, and it must appear without
		// a pi restart.
		let wrapped = false;
		for (let waited = 0; waited < 10000 && !wrapped; waited += 100) {
			await sleep(100);
			if (waited % 500 === 0) ext.writeConfig(config);
			wrapped = !!ext.providers()["stub"];
		}
		assert.ok(wrapped, "the config created after session_start is picked up by the running session");
	});
});

after(() => {
	closeAllClients();
	disposeAllExtensions();
});
