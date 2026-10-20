// The addressing module's own seam: the group rule, the entry-ownership rule and the provider
// mapping, called directly (no daemon, no pi). The rule tests moved here from the config suite and
// the extension suite when the rule gained its own module (#77, ADR 0005).
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "node:test";
import { addressModel, addressEndpoint, queuedProviders, readProviders } from "../src/addressing.js";

const GPU = "http://gpu.test/v1";
const ZEN = "http://zen.test/v1";
const providers = {
	strata: { baseUrl: GPU, api: "openai-completions" },
	llama: { baseUrl: GPU, api: "openai-completions" },
	zen: { baseUrl: ZEN, api: "openai-completions" },
};

describe("addressModel: the extension's form", () => {
	it("keys a model by its endpoint when no group names it", () => {
		assert.deepEqual(addressModel({ provider: "strata", baseUrl: GPU }, { [GPU]: { maxStreams: 1 } }, providers), { queueKey: GPU, entry: { maxStreams: 1 } });
	});

	it("splits one endpoint per provider with groups", () => {
		const queues = { serial: { match: ["strata"] }, parallel: { match: ["llama"], maxStreams: 4 } };
		assert.equal(addressModel({ provider: "strata", baseUrl: GPU }, queues, providers).queueKey, "serial");
		assert.equal(addressModel({ provider: "llama", baseUrl: GPU }, queues, providers).queueKey, "parallel", "two providers on one server get two slot sets");
	});

	it("merges every endpoint a group names into the group queue", () => {
		const queues = { gpu: { match: [GPU, "zen"] } };
		assert.equal(addressModel({ provider: "strata", baseUrl: GPU }, queues, providers).queueKey, "gpu");
		assert.equal(addressModel({ provider: "zen", baseUrl: ZEN }, queues, providers).queueKey, "gpu");
	});

	it("leaves a disabled group's members on their endpoint", () => {
		assert.equal(addressModel({ provider: "strata", baseUrl: GPU }, { gpu: { match: ["strata"], enabled: false } }, providers).queueKey, GPU);
	});

	it("keys a model by a group naming a provider that serves its endpoint", () => {
		// A raw client names only the endpoint, so a group naming one provider on it owns every
		// provider there: an extension session and a raw client land in one slot set.
		assert.equal(addressModel({ provider: "extra", baseUrl: GPU }, { g1: { match: ["strata"] } }, { ...providers, extra: { baseUrl: GPU } }).queueKey, "g1");
	});

	it("returns the raw owning entry, not a resolved policy, and undefined when nothing owns the key", () => {
		const entry = { maxStreams: 3, policy: "fifo" };
		assert.deepEqual(addressModel({ provider: "zen", baseUrl: ZEN }, { zen: entry }, providers).entry, entry, "the entry object itself, defaults left to the caller");
		assert.equal(addressModel({ provider: "strata", baseUrl: GPU }, {}, providers).entry, undefined, "an unclaimed endpoint key has no entry");
	});
});

describe("addressEndpoint: the daemon's form", () => {
	it("resolves a raw endpoint key through the same group rule", () => {
		// The provider-name branch is unreachable here by construction: the endpoint-level group wins.
		const queues = { g1: { match: ["strata"], maxStreams: 1 }, g2: { match: ["llama"], maxStreams: 1 } };
		assert.equal(addressEndpoint(GPU, queues, providers).queueKey, "g1", "the endpoint's first matching group owns the raw client's slot");
	});

	it("lets a baseUrl key win over a provider-name key naming the same endpoint", () => {
		const queues = { zen: { maxStreams: 1 }, [ZEN]: { maxStreams: 2 } };
		assert.deepEqual(addressEndpoint(ZEN, queues, providers), { queueKey: ZEN, entry: { maxStreams: 2 } });
	});

	it("takes the group's own entry once the group owns the endpoint", () => {
		const queues = { g1: { match: ["zen"], maxStreams: 5 }, zen: { maxStreams: 1 } };
		assert.deepEqual(addressEndpoint(ZEN, queues, providers), { queueKey: "g1", entry: { match: ["zen"], maxStreams: 5 } });
	});

	it("takes a provider-name entry's policy for its endpoint when no key names the endpoint", () => {
		assert.deepEqual(addressEndpoint(ZEN, { zen: { maxStreams: 1 } }, providers), { queueKey: ZEN, entry: { maxStreams: 1 } });
	});

	it("returns an empty key space with no config", () => {
		assert.deepEqual(addressEndpoint(GPU, {}, providers), { queueKey: GPU, entry: undefined });
	});
});

describe("queuedProviders: the reverse mapping", () => {
	it("wraps a provider named by a queue key and one serving a queued endpoint", () => {
		assert.deepEqual(queuedProviders({ [GPU]: { maxStreams: 1 }, zen: { maxStreams: 3 } }, providers).sort(), ["llama", "strata", "zen"]);
	});

	it("ignores queue keys naming no provider", () => {
		assert.deepEqual(queuedProviders({ "http://nowhere.test/v1": {} }, providers), []);
	});

	it("skips a queue key whose entry is disabled", () => {
		assert.deepEqual(queuedProviders({ [GPU]: { maxStreams: 1 }, zen: { maxStreams: 3, enabled: false } }, providers).sort(), ["llama", "strata"]);
	});

	it("wraps every provider named by a group's match list", () => {
		assert.deepEqual(queuedProviders({ gpu: { match: [GPU, "zen"], maxStreams: 1 } }, providers).sort(), ["llama", "strata", "zen"]);
	});

	it("skips a disabled group", () => {
		assert.deepEqual(queuedProviders({ gpu: { match: ["strata", "zen"], enabled: false }, [GPU]: { maxStreams: 1 } }, providers).sort(), ["llama", "strata"]);
	});
});

describe("readProviders", () => {
	const dir = mkdtempSync(join(tmpdir(), "llm-queue-providers-"));

	it("unwraps the .providers catalog", () => {
		const path = join(dir, "models.json");
		writeFileSync(path, JSON.stringify({ providers }));
		assert.deepEqual(readProviders(path), providers);
	});

	it("treats a missing, empty or unparsable file as an empty catalog", () => {
		assert.deepEqual(readProviders(join(dir, "absent.json")), {});
		const bad = join(dir, "half-written.json");
		writeFileSync(bad, '{ "providers": ');
		assert.deepEqual(readProviders(bad), {});
		const noKey = join(dir, "no-providers.json");
		writeFileSync(noKey, "{}");
		assert.deepEqual(readProviders(noKey), {});
	});
});
