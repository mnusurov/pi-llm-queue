import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { resolveQueueKey, resolveQueueProviders, resolveRole } from "../src/extension.ts";

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
