// Role resolution: the neutral env contract read as plain strings — explicit PI_QUEUE_ROLE, then
// the pi-subagents child hints, then parent. No daemon, no pi: the module decides, the test reads
// the decision. The group rule and the provider mapping are tested at their own seam in
// test/addressing.test.ts (#77, ADR 0005).
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { resolveRole } from "../src/extension.ts";

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
