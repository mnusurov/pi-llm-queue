import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { resolveQueueKey, resolveQueueProviders, resolveRole, resolveBlockSubagentLaunch } from "../src/extension.ts";

describe("provider resolution", () => {
	const providers = {
		strata: { baseUrl: "http://gpu.test/v1", api: "openai-completions" },
		zen: { baseUrl: "http://zen.test/v1", api: "openai-completions" },
	};

	it("wraps a provider named by a queue key and one serving a queued endpoint", () => {
		const queues = { "http://gpu.test/v1": { maxStreams: 1 }, zen: { maxStreams: 3 } };
		assert.deepEqual(resolveQueueProviders(queues, providers).sort(), ["strata", "zen"]);
	});

	it("ignores queue keys naming no provider", () => {
		assert.deepEqual(resolveQueueProviders({ "http://nowhere.test/v1": {} }, providers), []);
	});

	it("skips a queue key whose entry is disabled", () => {
		const queues = { "http://gpu.test/v1": { maxStreams: 1 }, zen: { maxStreams: 3, enabled: false } };
		assert.deepEqual(resolveQueueProviders(queues, providers), ["strata"]);
	});

	it("wraps every provider named by a group's match list", () => {
		const queues = { gpu: { match: ["http://gpu.test/v1", "zen"], maxStreams: 1 } };
		assert.deepEqual(resolveQueueProviders(queues, providers).sort(), ["strata", "zen"]);
	});

	it("skips a disabled group", () => {
		const queues = { gpu: { match: ["strata", "zen"], enabled: false }, "http://gpu.test/v1": { maxStreams: 1 } };
		assert.deepEqual(resolveQueueProviders(queues, providers), ["strata"]);
	});
});

describe("queue key resolution", () => {
	const strata = { provider: "strata", baseUrl: "http://gpu.test/v1" };
	const llama = { provider: "llama", baseUrl: "http://gpu.test/v1" };
	const zen = { provider: "zen", baseUrl: "http://zen.test/v1" };

	it("keys a model by its endpoint when no group names it", () => {
		assert.equal(resolveQueueKey({ "http://gpu.test/v1": { maxStreams: 1 } }, strata), "http://gpu.test/v1");
	});

	it("splits one endpoint per provider with groups", () => {
		const queues = { serial: { match: ["strata"] }, parallel: { match: ["llama"], maxStreams: 4 } };
		assert.equal(resolveQueueKey(queues, strata), "serial");
		assert.equal(resolveQueueKey(queues, llama), "parallel", "two providers on one server get two slot pools");
	});

	it("merges every endpoint a group names into the group queue", () => {
		const queues = { gpu: { match: ["http://gpu.test/v1", "zen"] } };
		assert.equal(resolveQueueKey(queues, strata), "gpu");
		assert.equal(resolveQueueKey(queues, zen), "gpu");
	});

	it("leaves a disabled group's members on their endpoint", () => {
		assert.equal(resolveQueueKey({ gpu: { match: ["strata"], enabled: false } }, strata), "http://gpu.test/v1");
	});

	it("keys a model by a group naming a provider that serves its endpoint", () => {
		// The daemon's endpoint-level rule, now shared: a group naming strata owns every provider
		// on strata's endpoint, so an extension session and a raw client land in one slot pool.
		const providers = { strata: { baseUrl: "http://gpu.test/v1" }, extra: { baseUrl: "http://gpu.test/v1" } };
		assert.equal(resolveQueueKey({ g1: { match: ["strata"] } }, { provider: "extra", baseUrl: "http://gpu.test/v1" }, providers), "g1");
	});
});

describe("blockSubagentLaunch policy", () => {
	const serial = { provider: "s", baseUrl: "http://s.test/v1" };
	const par = { provider: "p", baseUrl: "http://p.test/v1" };
	const queues = { serial: { match: ["s"], maxStreams: 1 }, par: { match: ["p"], maxStreams: 4 } };

	it("defaults to true for a single-slot queue and false for a multi-slot queue", () => {
		assert.equal(resolveBlockSubagentLaunch(queues, {}, serial), true);
		assert.equal(resolveBlockSubagentLaunch(queues, {}, par), false);
	});
	it("treats an omitted maxStreams as 1 (serialised)", () => {
		assert.equal(resolveBlockSubagentLaunch({ d: { match: ["s"] } }, {}, serial), true);
	});
	it("honours a per-queue override in both directions", () => {
		assert.equal(resolveBlockSubagentLaunch({ serial: { match: ["s"], maxStreams: 1, blockSubagentLaunch: false } }, {}, serial), false);
		assert.equal(resolveBlockSubagentLaunch({ par: { match: ["p"], maxStreams: 4, blockSubagentLaunch: true } }, {}, par), true);
	});
	it("falls back to defaults.blockSubagentLaunch when the queue omits it", () => {
		assert.equal(resolveBlockSubagentLaunch(queues, { blockSubagentLaunch: false }, serial), false);
	});
	it("never blocks a disabled (passthrough) queue", () => {
		assert.equal(resolveBlockSubagentLaunch({ serial: { match: ["s"], maxStreams: 1, enabled: false } }, {}, serial), false);
	});

	it("resolves a queue keyed by the provider name, the way the daemon resolves its policy", () => {
		// No group, no baseUrl key: the key is the provider's own name (the README's shape).
		const model = { provider: "stub", baseUrl: "http://127.0.0.1:9999/v1" };
		const providers = { stub: { baseUrl: "http://127.0.0.1:9999/v1" } };
		assert.equal(resolveBlockSubagentLaunch({ stub: { maxStreams: 1 } }, {}, model, providers), true, "a provider-name key serialises: launches must block");
		assert.equal(resolveBlockSubagentLaunch({ stub: { maxStreams: 2 } }, {}, model, providers), false);
		assert.equal(resolveBlockSubagentLaunch({ stub: { maxStreams: 1, enabled: false } }, {}, model, providers), false);
	});
});

describe("role resolution", () => {
	it("explicit PI_QUEUE_ROLE wins over child hints", () => {
		assert.equal(resolveRole({ PI_QUEUE_ROLE: "parent", PI_SUBAGENT_CHILD: "1" }), "parent");
		assert.equal(resolveRole({ PI_QUEUE_ROLE: "child" }), "child");
	});
	it("pi-subagents child hints resolve to child (read as env strings)", () => {
		assert.equal(resolveRole({ PI_SUBAGENT_CHILD: "1" }), "child");
		assert.equal(resolveRole({ PI_SUBAGENT_PARENT_SESSION: "abc" }), "child");
	});
	it("defaults to parent when no role env is set", () => {
		assert.equal(resolveRole({}), "parent");
	});
});
