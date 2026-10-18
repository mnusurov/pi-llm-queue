import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "node:test";
import { SCHEMA_VERSION, loadValidConfig, validateConfig } from "../src/config.js";

describe("config schema validation", () => {
	it("accepts a full valid config", () => {
		assert.deepEqual(
			validateConfig({
				schemaVersion: SCHEMA_VERSION,
				defaults: {
					maxStreams: 1,
					policy: "priority-aging",
					leaseTtlMs: 30000,
					agingAfterSlots: 3,
					agingAfterMs: 60000,
					failOpen: true,
					queueTimeoutMs: 120000,
					daemonIdleExitMs: 60000,
				},
				queues: {
					"http://gpu.test/v1": { maxStreams: 1, blockSubagentLaunch: true },
					zen: { maxStreams: 3, policy: "fifo", enabled: true, match: ["zen"] },
					"free-api": { enabled: false },
				},
			}),
			[],
		);
	});

	it("accepts an empty config (all defaults)", () => {
		assert.deepEqual(validateConfig({}), []);
	});

	it("rejects a non-object config with a clear error", () => {
		assert.deepEqual(validateConfig(5), ["config must be a JSON object"]);
		assert.deepEqual(validateConfig([1, 2]), ["config must be a JSON object"]);
	});

	it("rejects a schemaVersion newer than this build supports", () => {
		const errors = validateConfig({ schemaVersion: SCHEMA_VERSION + 1 });
		assert.equal(errors.length, 1);
		assert.match(errors[0], /schemaVersion 2 is newer than this pi-llm-queue build supports \(max 1\) — upgrade pi-llm-queue/);
	});

	it("rejects a non-integer schemaVersion", () => {
		assert.deepEqual(validateConfig({ schemaVersion: "1" }), ['schemaVersion must be a positive integer (got "1")']);
	});

	it("names the path and expected type for every bad field", () => {
		const errors = validateConfig({
			defaults: { maxStreams: "1", agingAfterSlots: -2, failOpen: "yes", leaseTtlMs: 0 },
			queues: { zen: { maxStreams: 0, policy: "lifo", match: "zen", blockSubagentLaunch: 1 } },
		});
		assert.deepEqual(errors, [
			'defaults.maxStreams must be a positive integer (got "1")',
			"defaults.agingAfterSlots must be an integer >= 0 (got -2)",
			'defaults.failOpen must be true or false (got "yes")',
			"defaults.leaseTtlMs must be a number >= 1000 (got 0)",
			"queues.zen.maxStreams must be a positive integer (got 0)",
			'queues.zen.policy must be one of "priority-aging" or "fifo" (got "lifo")',
			'queues.zen.match must be an array of strings (got "zen")',
			"queues.zen.blockSubagentLaunch must be true or false (got 1)",
		]);
	});

	it("rejects a leaseTtlMs below the heartbeat/sweep floor, accepts the floor itself", () => {
		// A TTL under the 1 s client-heartbeat floor steals the slot from a healthy, heartbeating
		// session over and over (the sweep runs every second), i.e. the KV-cache thrash the tool exists to prevent.
		for (const ttl of [30, 999]) assert.deepEqual(validateConfig({ defaults: { leaseTtlMs: ttl } }), [`defaults.leaseTtlMs must be a number >= 1000 (got ${ttl})`]);
		assert.deepEqual(validateConfig({ defaults: { leaseTtlMs: 1000 } }), []);
		assert.deepEqual(validateConfig({ queues: { zen: { leaseTtlMs: 999 } } }), ["queues.zen.leaseTtlMs must be a number >= 1000 (got 999)"]);
	});

	it("flags unknown fields and lists the known ones", () => {
		const errors = validateConfig({ defaults: { maxstream: 1 }, queues: { zen: { maxStreams: 1, matchez: [] } } });
		assert.equal(errors.length, 2);
		assert.match(errors[0], /^defaults\.maxstream is an unknown field \(known: .*maxStreams.*policy.*\)$/);
		assert.match(errors[1], /^queues\.zen\.matchez is an unknown field \(known: .*match.*\)$/);
	});

	// #49 removed the session-switch quanta outright: a config carrying them is rejected and
	// the daemon serves defaults, so no preemption can be re-enabled from config.
	it("rejects the removed session-switch quanta as unknown fields", () => {
		assert.deepEqual(validateConfig({ defaults: { idleQuantumMs: 120000, monopolyRequests: 12, monopolyMs: 180000 } }), [
			"defaults.idleQuantumMs is an unknown field (known: maxStreams, policy, leaseTtlMs, agingAfterSlots, agingAfterMs, blockSubagentLaunch, failOpen, queueTimeoutMs, daemonIdleExitMs)",
			"defaults.monopolyRequests is an unknown field (known: maxStreams, policy, leaseTtlMs, agingAfterSlots, agingAfterMs, blockSubagentLaunch, failOpen, queueTimeoutMs, daemonIdleExitMs)",
			"defaults.monopolyMs is an unknown field (known: maxStreams, policy, leaseTtlMs, agingAfterSlots, agingAfterMs, blockSubagentLaunch, failOpen, queueTimeoutMs, daemonIdleExitMs)",
		]);
		assert.match(validateConfig({ queues: { zen: { idleQuantumMs: 1 } } })[0], /^queues\.zen\.idleQuantumMs is an unknown field/);
	});

	it("flags defaults-only fields set per queue", () => {
		const errors = validateConfig({ queues: { zen: { failOpen: false, queueTimeoutMs: 5, daemonIdleExitMs: 1 } } });
		assert.deepEqual(errors, [
			"queues.zen.failOpen is only valid in defaults (the whole config is rejected and defaults are used)",
			"queues.zen.queueTimeoutMs is only valid in defaults (the whole config is rejected and defaults are used)",
			"queues.zen.daemonIdleExitMs is only valid in defaults (the whole config is rejected and defaults are used)",
		]);
	});

	it("flags a non-object defaults and queue entry", () => {
		assert.deepEqual(validateConfig({ defaults: [] }), ["defaults must be an object"]);
		assert.deepEqual(validateConfig({ queues: { zen: [] } }), ["queues.zen must be an object"]);
	});

	it("flags an unknown top-level field", () => {
		assert.match(validateConfig({ queue: {} })[0], /^queue is an unknown top-level field \(known: schemaVersion, defaults, queues\)$/);
	});

	it("flags queues-only fields set in defaults", () => {
		assert.deepEqual(validateConfig({ defaults: { match: ["zen"], enabled: false } }), [
			"defaults.match is only valid in a queue entry (the whole config is rejected and defaults are used)",
			"defaults.enabled is only valid in a queue entry (the whole config is rejected and defaults are used)",
		]);
	});
});

describe("config file loading", () => {
	const dir = mkdtempSync(join(tmpdir(), "llm-queue-cfg-"));

	it("treats a missing file as all defaults, with no errors", () => {
		assert.deepEqual(loadValidConfig(join(dir, "absent.json")), { config: {}, errors: [], invalidJson: false });
	});

	it("reports invalid JSON as a clear error and flags it for silent reload", () => {
		const path = join(dir, "half-written.json");
		writeFileSync(path, "{ 'defaults': ");
		const { config, errors, invalidJson } = loadValidConfig(path);
		assert.deepEqual(config, {});
		assert.equal(invalidJson, true, "a half-written file must be a silent no-op on hot-reload");
		assert.match(errors[0], /^config is not valid JSON: /);
	});

	it("falls back to defaults on a schema violation, reporting every error", () => {
		const path = join(dir, "bad.json");
		writeFileSync(path, JSON.stringify({ defaults: { maxStreams: 0 } }));
		const { config, errors, invalidJson } = loadValidConfig(path);
		assert.deepEqual(config, {});
		assert.equal(invalidJson, false);
		assert.deepEqual(errors, ["defaults.maxStreams must be a positive integer (got 0)"]);
	});

	it("loads a valid config", () => {
		const path = join(dir, "good.json");
		writeFileSync(path, JSON.stringify({ schemaVersion: SCHEMA_VERSION, defaults: { maxStreams: 2 } }));
		assert.deepEqual(loadValidConfig(path), { config: { schemaVersion: 1, defaults: { maxStreams: 2 } }, errors: [], invalidJson: false });
	});
});
