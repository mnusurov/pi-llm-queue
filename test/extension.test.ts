import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { resolveQueueProviders, resolveRole } from "../src/extension.ts";

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
