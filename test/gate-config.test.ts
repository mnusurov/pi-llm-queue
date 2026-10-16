/**
 * Thin seam tests for the queue policy helpers and the lease gate. Stub
 * client only — no pi session, no model endpoint.
 */
import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { normalizeBaseUrl, resolveConfig, resolveQueueKey, resolveQueueSettings } from "../src/lib/config.ts";
import { acquireForRequest, releaseLease, resolveGroup, resolveRole, type SessionLeaseState } from "../src/lib/gate.ts";
import type { LeaseGrant, LeaseClient } from "../src/lib/lease-client.ts";

const GPU = "http://192.168.1.100:8001/v1";

describe("queue key resolution", () => {
	test("baseUrl keys are normalised so providers on one endpoint merge", () => {
		assert.equal(normalizeBaseUrl("HTTP://192.168.1.100:8001/V1/"), "http://192.168.1.100:8001/v1");
		const config = resolveConfig({ queues: { [GPU]: { maxStreams: 1 } } });
		assert.equal(resolveQueueKey(config, "strata", GPU), GPU);
		assert.equal(resolveQueueKey(config, "llamacpp", "http://192.168.1.100:8001/v1/"), GPU);
	});

	test("provider-name keys resolve to that provider's baseUrl", () => {
		const config = resolveConfig({ queues: { zen: { maxStreams: 3 } } });
		assert.equal(resolveQueueKey(config, "zen", "http://192.168.1.100:8787/v1"), "http://192.168.1.100:8787/v1");
		assert.equal(resolveQueueSettings(config, "zen", "http://192.168.1.100:8787/v1")?.maxStreams, 3);
	});

	test("enabled false is passthrough", () => {
		const config = resolveConfig({ queues: { zen: { enabled: false } } });
		assert.equal(resolveQueueKey(config, "zen", "http://192.168.1.100:8787/v1"), null);
		assert.equal(resolveQueueSettings(config, "zen", "http://192.168.1.100:8787/v1"), null);
	});

	test("baseUrl entries win over provider-name entries on collision", () => {
		const config = resolveConfig({
			defaults: { maxStreams: 4 },
			queues: { strata: { maxStreams: 2 }, [GPU]: { maxStreams: 1 } },
		});
		assert.equal(resolveQueueSettings(config, "strata", GPU)?.maxStreams, 1);
	});

	test("unconfigured providers are queued with defaults", () => {
		const config = resolveConfig({ defaults: { maxStreams: 2 } });
		assert.equal(resolveQueueSettings(config, "other", "http://example.test/v1")?.maxStreams, 2);
	});

	test("blockSubagentLaunch defaults to true only for single-slot queues", () => {
		const config = resolveConfig({ queues: { [GPU]: { maxStreams: 1 }, zen: { maxStreams: 3 } } });
		assert.equal(resolveQueueSettings(config, "strata", GPU)?.blockSubagentLaunch, true);
		assert.equal(resolveQueueSettings(config, "zen", "http://192.168.1.100:8787/v1")?.blockSubagentLaunch, false);
		const explicit = resolveConfig({ queues: { [GPU]: { maxStreams: 1, blockSubagentLaunch: false } } });
		assert.equal(resolveQueueSettings(explicit, "strata", GPU)?.blockSubagentLaunch, false);
	});
});

describe("role contract", () => {
	test("PI_QUEUE_ROLE wins over the pi-subagents hint", () => {
		assert.equal(resolveRole({ PI_QUEUE_ROLE: "parent", PI_SUBAGENT_CHILD: "1" }), "parent");
		assert.equal(resolveRole({ PI_QUEUE_ROLE: "child" }), "child");
	});
	test("PI_SUBAGENT_CHILD is read as a plain env string", () => {
		assert.equal(resolveRole({ PI_SUBAGENT_CHILD: "1" }), "child");
		assert.equal(resolveRole({ PI_SUBAGENT_CHILD: "" }), "parent");
		assert.equal(resolveRole({}), "parent");
	});
	test("PI_QUEUE_GROUP is optional", () => {
		assert.equal(resolveGroup({ PI_QUEUE_GROUP: "batch-7" }), "batch-7");
		assert.equal(resolveGroup({}), undefined);
	});
});

/** Stub lease client: records calls, grants on a schedule. */
function stubClient(behaviour: { grants: LeaseGrant[]; renewResult?: boolean }): {
	client: LeaseClient;
	calls: { acquire: number; renew: number; release: string[] };
} {
	const calls = { acquire: 0, renew: 0, release: [] as string[] };
	const grants = [...behaviour.grants];
	const client = {
		acquire: async () => {
			calls.acquire += 1;
			return grants.shift() ?? { granted: true, leaseId: `lease-x${calls.acquire}` };
		},
		renew: async () => {
			calls.renew += 1;
			return behaviour.renewResult ?? true;
		},
		release: async (leaseId: string) => {
			calls.release.push(leaseId);
		},
	} as unknown as LeaseClient;
	return { client, calls };
}

describe("lease gate", () => {
	const settings = { ...resolveConfig({}).defaults, queueTimeoutMs: null };

	test("a session that already holds the slot renews instead of re-queuing", async () => {
		const { client, calls } = stubClient({ grants: [] });
		const state: SessionLeaseState = { sessionKey: "s", role: "parent", leaseId: "lease-1", queueKey: GPU };
		const grant = await acquireForRequest({ client, state, settings, queueKey: GPU });
		assert.equal(grant.granted, true);
		assert.equal(grant.leaseId, "lease-1");
		assert.equal(calls.acquire, 0, "no re-queue while the lease is held");
		assert.equal(calls.renew, 1);
	});

	test("a lost lease falls back to a fresh acquire", async () => {
		const { client, calls } = stubClient({ grants: [{ granted: true, leaseId: "lease-2" }], renewResult: false });
		const state: SessionLeaseState = { sessionKey: "s", role: "child", leaseId: "lease-1", queueKey: GPU };
		const grant = await acquireForRequest({ client, state, settings, queueKey: GPU });
		assert.equal(grant.leaseId, "lease-2");
		assert.equal(calls.acquire, 1);
		assert.equal(state.leaseId, "lease-2");
	});

	test("fail-open grants pass through and are warned on timeout", async () => {
		const { client } = stubClient({ grants: [{ granted: true, failOpen: true, timedOut: true }] });
		const warnings: string[] = [];
		const state: SessionLeaseState = { sessionKey: "s", role: "parent" };
		const grant = await acquireForRequest({ client, state, settings: { ...settings, queueTimeoutMs: 500 }, queueKey: GPU, onWarn: (m) => warnings.push(m) });
		assert.equal(grant.granted, true);
		assert.equal(warnings.length, 1, "timeout degrades with a visible warning");
	});

	test("release clears the lease and tells the daemon", async () => {
		const { client, calls } = stubClient({ grants: [] });
		const state: SessionLeaseState = { sessionKey: "s", role: "parent", leaseId: "lease-9", queueKey: GPU };
		await releaseLease({ client, state });
		assert.deepEqual(calls.release, ["lease-9"]);
		assert.equal(state.leaseId, undefined);
		assert.equal(state.queueKey, undefined);
	});

	test("requests from two sessions on one queue are serialised by the daemon", async () => {
		// The daemon is the scheduler; the gate only asks for a slot. Order is
		// asserted end to end in test/serialisation.test.ts.
		const order: string[] = [];
		const { client } = stubClient({ grants: [{ granted: true, leaseId: "l-a" }, { granted: true, leaseId: "l-b" }] });
		for (const sessionKey of ["a", "b"]) {
			const state: SessionLeaseState = { sessionKey, role: "parent" };
			const grant = await acquireForRequest({ client, state, settings, queueKey: GPU });
			order.push(`${sessionKey}:${grant.leaseId}`);
		}
		assert.deepEqual(order, ["a:l-a", "b:l-b"]);
	});
});
