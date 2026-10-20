import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { resolveRole, isLocalEndpoint, buildStarterConfig } from "../src/extension.ts";

// The group rule, the entry-ownership rule and the provider mapping are tested at their own seam
// in test/addressing.test.ts (#77, ADR 0005).

describe("self-hosted endpoint detection", () => {
	it("detects loopback", () => {
		for (const url of ["http://127.0.0.1:8080/v1", "http://localhost:8080/v1", "http://[::1]:8080/v1"]) assert.equal(isLocalEndpoint(url), true, url);
	});
	it("detects RFC1918", () => {
		for (const url of ["http://10.1.2.3:8000/v1", "http://172.16.0.1:8000/v1", "http://172.31.255.254:8000/v1", "http://192.168.1.50:8000/v1"]) assert.equal(isLocalEndpoint(url), true, url);
	});
	it("detects link-local", () => {
		assert.equal(isLocalEndpoint("http://169.254.7.7:8000/v1"), true);
	});
	it("detects local/mDNS domains", () => {
		for (const url of ["http://gpu.local:8000/v1", "http://llama.lan:8000/v1", "http://server.home.arpa:8000/v1"]) assert.equal(isLocalEndpoint(url), true, url);
	});
	it("treats public IPs and other hostnames as cloud", () => {
		for (const url of ["https://api.openai.com/v1", "https://8.8.8.8/v1", "https://172.15.0.1:8000/v1", "https://169.253.1.1/v1", "http://10.0.0.1.nip.io:8080/v1", "http://127.0.0.1.example.com:8080/v1"]) assert.equal(isLocalEndpoint(url), false, url);
	});
	it("treats a missing baseUrl as cloud", () => {
		assert.equal(isLocalEndpoint(undefined), false);
		assert.equal(isLocalEndpoint("not a url"), false);
	});
});

describe("starter config generation", () => {
	const providers = {
		strata: { baseUrl: "http://192.168.1.50:8000/v1" },
		extra: { baseUrl: "http://192.168.1.50:8000/v1" },
		llama: { baseUrl: "http://127.0.0.1:8080/v1" },
		openai: { baseUrl: "https://api.openai.com/v1" },
		native: { baseUrl: "http://gpu.local:8000/v1" },
	};
	const native = (name: string) => name === "native";

	it("enables self-hosted endpoints and passes cloud through", () => {
		const starter = buildStarterConfig(providers, native)!;
		assert.deepEqual(starter.queues["http://192.168.1.50:8000/v1"], { enabled: true });
		assert.deepEqual(starter.queues["https://api.openai.com/v1"], { enabled: false });
	});
	it("keys by endpoint so providers sharing one endpoint share one queue", () => {
		const starter = buildStarterConfig(providers, native)!;
		assert.equal(Object.keys(starter.queues).filter((k) => k === "http://192.168.1.50:8000/v1").length, 1);
	});
	it("excludes endpoints owned by native extensions", () => {
		assert.equal(buildStarterConfig(providers, native)!.queues["http://gpu.local:8000/v1"], undefined);
	});
	it("skips providers with no baseUrl", () => {
		const starter = buildStarterConfig({ strata: { baseUrl: "http://127.0.0.1:1/v1" }, anthropic: { api: "anthropic" } }, () => false)!;
		assert.deepEqual(starter.queues, { "http://127.0.0.1:1/v1": { enabled: true } });
	});
	it("sorts keys deterministically", () => {
		const starter = buildStarterConfig(providers, native)!;
		assert.deepEqual(Object.keys(starter.queues), [...Object.keys(starter.queues)].sort());
	});
	it("carries schemaVersion 1, the parent queue-wait cap as its only defaults field, then queues", () => {
		const starter = buildStarterConfig(providers, native)!;
		assert.equal(starter.schemaVersion, 1);
		assert.deepEqual(Object.keys(starter), ["schemaVersion", "defaults", "queues"]);
		assert.deepEqual(starter.defaults, { queueTimeoutMs: 120000 });
	});
	it("returns nothing to scaffold for an empty catalog, an all-cloud catalog and an all-native catalog", () => {
		assert.equal(buildStarterConfig({}, () => false), null);
		assert.equal(buildStarterConfig({ openai: { baseUrl: "https://api.openai.com/v1" } }, () => false), null);
		assert.equal(buildStarterConfig({ native: { baseUrl: "http://127.0.0.1:8080/v1" } }, () => true), null);
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
