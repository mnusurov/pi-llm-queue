// The config module at its own seam (ADR 0001): schema validation, config file loading, the
// defaults table, the policy cascade (resolveQueuePolicy) and the keyed beat period. No daemon, no
// pi — the module decides, the test reads decisions.
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "node:test";
import { DEFAULTS, SCHEMA_VERSION, heartbeatMs, keyedBeatMs, loadValidConfig, resolveQueuePolicy, validateConfig } from "../src/config.js";

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
					blockingTools: ["subagent", "bg_wait"],
					daemonIdleExitMs: 60000,
					logFile: "/tmp/queue-events.jsonl",
				},
				queues: {
					"http://gpu.test/v1": { maxStreams: 1, idleYieldMs: 30000 },
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
			queues: { zen: { maxStreams: 0, policy: "lifo", match: "zen", idleYieldMs: -1 } },
		});
		assert.deepEqual(errors, [
			'defaults.maxStreams must be a positive integer (got "1")',
			"defaults.agingAfterSlots must be an integer >= 0 (got -2)",
			'defaults.failOpen must be true or false (got "yes")',
			"defaults.leaseTtlMs must be a number >= 1000 (got 0)",
			"queues.zen.maxStreams must be a positive integer (got 0)",
			'queues.zen.policy must be one of "priority-aging" or "fifo" (got "lifo")',
			'queues.zen.match must be an array of strings (got "zen")',
			"queues.zen.idleYieldMs must be a number >= 0 (got -1)",
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
			"defaults.idleQuantumMs is an unknown field (known: maxStreams, policy, leaseTtlMs, agingAfterSlots, agingAfterMs, idleYieldMs, blockingTools, failOpen, queueTimeoutMs, daemonIdleExitMs, logFile)",
			"defaults.monopolyRequests is an unknown field (known: maxStreams, policy, leaseTtlMs, agingAfterSlots, agingAfterMs, idleYieldMs, blockingTools, failOpen, queueTimeoutMs, daemonIdleExitMs, logFile)",
			"defaults.monopolyMs is an unknown field (known: maxStreams, policy, leaseTtlMs, agingAfterSlots, agingAfterMs, idleYieldMs, blockingTools, failOpen, queueTimeoutMs, daemonIdleExitMs, logFile)",
		]);
		assert.match(validateConfig({ queues: { zen: { idleQuantumMs: 1 } } })[0], /^queues\.zen\.idleQuantumMs is an unknown field/);
	});

	// #57: the blocking-tool set pauses the client's run-scoped keyed beat, so a Holder parked
	// on another agent yields the Slot. Client-side concern: defaults only, the daemon ignores it.
	it("accepts blockingTools in defaults and rejects it per queue", () => {
		assert.deepEqual(validateConfig({ defaults: { blockingTools: ["subagent", "intercom"] } }), []);
		assert.deepEqual(validateConfig({ defaults: { blockingTools: [] } }), []);
		assert.deepEqual(validateConfig({ defaults: { blockingTools: "subagent" } }), ['defaults.blockingTools must be an array of strings (got "subagent")']);
		assert.deepEqual(validateConfig({ defaults: { blockingTools: [1] } }), ["defaults.blockingTools must be an array of strings (got [1])"]);
		assert.deepEqual(validateConfig({ queues: { zen: { blockingTools: ["subagent"] } } }), [
			"queues.zen.blockingTools is only valid in defaults (the whole config is rejected and defaults are used)",
		]);
	});

	it("flags defaults-only fields set per queue", () => {
		const errors = validateConfig({ queues: { zen: { failOpen: false, queueTimeoutMs: 5, daemonIdleExitMs: 1, logFile: "/tmp/queue.jsonl" } } });
		assert.deepEqual(errors, [
			"queues.zen.failOpen is only valid in defaults (the whole config is rejected and defaults are used)",
			"queues.zen.queueTimeoutMs is only valid in defaults (the whole config is rejected and defaults are used)",
			"queues.zen.daemonIdleExitMs is only valid in defaults (the whole config is rejected and defaults are used)",
			"queues.zen.logFile is only valid in defaults (the whole config is rejected and defaults are used)",
		]);
	});

	// #47: the event log is a daemon-wide path, so it is defaults-only and must name a real file.
	it("rejects a logFile that is not a path, accepts a string path", () => {
		assert.deepEqual(validateConfig({ defaults: { logFile: "/tmp/queue-events.jsonl" } }), []);
		assert.deepEqual(validateConfig({ defaults: { logFile: 42 } }), ['defaults.logFile must be a non-empty string path (got 42)']);
		assert.deepEqual(validateConfig({ defaults: { logFile: "" } }), ['defaults.logFile must be a non-empty string path (got "")']);
	});

	// #60: the documented default is 0 (exit the moment the last counted client leaves), so 0 must
	// be expressible; a JSON config cannot carry Infinity, so "never exit" is a large value (the
	// daemon clamps it to setTimeout's 2^31-1 cap — see the daemon-seam test).
	it("accepts daemonIdleExitMs 0 (the #60 default) and a large resident value", () => {
		assert.deepEqual(validateConfig({ defaults: { daemonIdleExitMs: 0 } }), []);
		assert.deepEqual(validateConfig({ defaults: { daemonIdleExitMs: 31536000000 } }), []);
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

// #77: the defaults table and the heartbeat derivation are exports, so the daemon, the extension
// and the test harnesses stop re-typing them.
describe("policy defaults table", () => {
	it("derives the client heartbeat as a third of the lease TTL with a 250 ms floor", () => {
		assert.equal(heartbeatMs(DEFAULTS.leaseTtlMs), 10000);
		// At the leaseTtlMs floor (1000 ms) a 1 s beat would equal the TTL, so a sweep landing between
		// two beats would evict a healthy holder; the floor keeps the period under it.
		assert.equal(heartbeatMs(1000), 333);
		assert.equal(heartbeatMs(750), 250);
	});

	it("bounds a queue's keyed beat by its own TTL and idle-yield window (#81)", () => {
		assert.equal(keyedBeatMs(1000, 30000), 333, "a short per-queue TTL governs the beat");
		assert.equal(keyedBeatMs(30000, 1000), 500, "a short idle window needs a beat at half of it");
		assert.equal(keyedBeatMs(30000, 0), 10000, "disabled idle-yield imposes no bound");
		assert.equal(keyedBeatMs(30000, 30000), 10000, "the TTL beat is already tighter than idle/2");
	});

	// #98: the schema's leaseTtlMs floor (1000) and the beat derivation are one invariant — a beat
	// must leave at least two refreshes inside every TTL the schema accepts, or a sweep landing
	// between two beats evicts a healthy Holder. Before this the invariant was a pair of comments.
	// The test crosses the whole client path: schema-accepted config → resolveQueuePolicy → beat.
	it("leaves at least two beats inside every TTL the schema accepts", () => {
		for (const ttl of [1000, 1001, 1500, 30000, 3600000]) {
			for (const idle of [0, 1, 500, 999, 30000]) {
				assert.deepEqual(validateConfig({ queues: { zen: { leaseTtlMs: ttl, idleYieldMs: idle } } }), [], `ttl ${ttl} idle ${idle} is schema-accepted`);
				// The cascade rungs a schema-accepted value can sit on: queue entry, defaults, DEFAULTS.
				for (const source of [{ leaseTtlMs: ttl, idleYieldMs: idle }, {}]) {
					const policy = resolveQueuePolicy(source, source);
					const beat = keyedBeatMs(policy.leaseTtlMs, policy.idleYieldMs);
					assert.ok(beat * 2 <= policy.leaseTtlMs, `beat ${beat} ms leaves < 2 refreshes inside TTL ${policy.leaseTtlMs} ms (idle ${policy.idleYieldMs})`);
				}
			}
		}
	});
});

// #98: one resolver owns the policy cascade (entry field → defaults field → DEFAULTS), so the
// daemon and the client agree on every Policy default by construction, not by comment.
describe("policy cascade resolver", () => {
	it("resolves every field from DEFAULTS when entry and defaults are empty", () => {
		assert.deepEqual(resolveQueuePolicy({}, {}), {
			maxStreams: 1,
			leaseTtlMs: 30000,
			agingAfterSlots: 3,
			agingAfterMs: 60000,
			policy: "priority-aging",
			idleYieldMs: 30000,
		});
	});

	it("entry fields override defaults, defaults override DEFAULTS", () => {
		const policy = resolveQueuePolicy({ maxStreams: 4, policy: "fifo" }, { maxStreams: 2, leaseTtlMs: 9000, agingAfterSlots: 5 });
		assert.deepEqual(policy, {
			maxStreams: 4,
			leaseTtlMs: 9000,
			agingAfterSlots: 5,
			agingAfterMs: 60000,
			policy: "fifo",
			idleYieldMs: 30000,
		});
	});

	it("enabled: false is Passthrough: maxStreams Infinity, other fields still cascade", () => {
		const policy = resolveQueuePolicy({ enabled: false, leaseTtlMs: 5000 }, { leaseTtlMs: 9000 });
		assert.equal(policy.maxStreams, Infinity);
		assert.equal(policy.leaseTtlMs, 5000);
	});

	it("idleYieldMs 0 passes through, imposing no idle bound on the beat", () => {
		assert.equal(resolveQueuePolicy({ idleYieldMs: 0 }, {}).idleYieldMs, 0);
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
