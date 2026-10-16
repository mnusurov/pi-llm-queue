/**
 * Daemon-side config resolution: queue keys are endpoint baseUrls, config keys
 * may name providers (resolved through pi's models.json), and keys are
 * normalised so case and trailing slashes cannot silently fall back to
 * defaults. Socket seam only.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { describe, test } from "node:test";
import { clientTracker, sleep, startTestDaemon } from "./harness.ts";

const GPU = "http://127.0.0.1:8011/v1";
const ZEN = "http://127.0.0.1:8787/v1";

describe("daemon config key resolution", () => {
	const tracker = clientTracker();
	const closeClients = () => tracker.closeAll();
	test("a provider-name config key resolves through models.json to its baseUrl", async () => {
		const daemon = await startTestDaemon({
			config: { defaults: { maxStreams: 1 }, queues: { Zen: { maxStreams: 2 } } },
			modelsJson: { providers: { zen: { baseUrl: ZEN } } },
		});
		try {
			const a = tracker.make(daemon.socketPath);
			const b = tracker.make(daemon.socketPath);
			const grantA = await a.acquire({ queueKey: ZEN, sessionKey: "a", role: "parent" });
			const grantB = await b.acquire({ queueKey: ZEN, sessionKey: "b", role: "parent" });
			assert.equal(grantA.granted, true);
			assert.equal(grantB.granted, true, "provider-name key must raise the slot count, not fall back to defaults");
			a.close();
			b.close();
		} finally {
			closeClients();
			await daemon.stop();
		}
	});

	test("a mixed-case baseUrl config key with a trailing slash still applies", async () => {
		const daemon = await startTestDaemon({
			config: { defaults: { maxStreams: 1 }, queues: { "HTTP://127.0.0.1:8011/V1/": { maxStreams: 2 } } },
		});
		try {
			const a = tracker.make(daemon.socketPath);
			const b = tracker.make(daemon.socketPath);
			const grantA = await a.acquire({ queueKey: GPU, sessionKey: "a", role: "parent" });
			const grantB = await b.acquire({ queueKey: GPU, sessionKey: "b", role: "parent" });
			assert.equal(grantA.granted, true);
			assert.equal(grantB.granted, true, "normalisation must not silently drop the queue entry");
			const status = await a.status();
			assert.equal(status?.find((entry) => entry.key === GPU)?.maxStreams, 2);
			a.close();
			b.close();
		} finally {
			closeClients();
			await daemon.stop();
		}
	});

	test("reload re-reads config without a daemon restart", async () => {
		const daemon = await startTestDaemon({ config: { defaults: { maxStreams: 1 }, queues: { [GPU]: { maxStreams: 1 } } } });
		try {
			const a = tracker.make(daemon.socketPath);
			const b = tracker.make(daemon.socketPath);
			const grantA = await a.acquire({ queueKey: GPU, sessionKey: "a", role: "parent" });
			assert.equal(grantA.granted, true);
			let grantedB: unknown = null;
			const pendingB = b.acquire({ queueKey: GPU, sessionKey: "b", role: "parent" }).then((grant) => {
				grantedB = grant;
				return grant;
			});
			await sleep(150);
			assert.equal(grantedB, null, "single slot queue keeps the second session waiting");

			fs.writeFileSync(daemon.configPath, JSON.stringify({ defaults: { maxStreams: 2 }, queues: { [GPU]: { maxStreams: 2 } } }), "utf8");
			assert.equal(await a.reload(), true, "daemon re-reads config on request");
			const grantB = await pendingB;
			assert.equal((grantB as { granted: boolean }).granted, true, "hot-reloaded maxStreams takes effect");
			a.close();
			b.close();
		} finally {
			closeClients();
			await daemon.stop();
		}
	});
});
