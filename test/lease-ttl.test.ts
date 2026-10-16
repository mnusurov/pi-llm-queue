/**
 * Lease TTL: a crashed session that stops renewing is reclaimed by the daemon
 * and the slot moves to the next waiter. Socket seam only.
 */
import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { client, sleep, startTestDaemon, type TestDaemon } from "./harness.ts";

const ENDPOINT = "http://127.0.0.1:8002/v1";

describe("lease TTL reclaim", () => {
	let daemon: TestDaemon;

	before(async () => {
		daemon = await startTestDaemon({ config: { defaults: { maxStreams: 1, leaseTtlMs: 1500 }, queues: { [ENDPOINT]: { leaseTtlMs: 1500 } } } });
	});
	after(async () => {
		await daemon.stop();
	});

	test("an un-renewed lease expires and the next waiter is granted", async () => {
		const holder = client(daemon.socketPath);
		const waiter = client(daemon.socketPath);
		// Simulate a session that acquired a slot and died: no renew, no release.
		const grant = await holder.acquire({ queueKey: ENDPOINT, sessionKey: "dead-session", role: "parent" });
		assert.equal(grant.granted, true);

		let granted: Awaited<ReturnType<typeof waiter.acquire>> | null = null;
		const pending = waiter.acquire({ queueKey: ENDPOINT, sessionKey: "waiting-session", role: "child" }).then((value) => {
			granted = value;
			return value;
		});

		await sleep(500);
		assert.equal(granted, null, "slot stays with the expired-but-not-yet-swept holder");

		const result = await pending;
		assert.equal(result.granted, true, "TTL sweep reclaims the slot within a few seconds");
		assert.ok(result.leaseId);

		const status = await waiter.status();
		const queue = status?.find((entry) => entry.key === ENDPOINT);
		assert.equal(queue?.holders[0]?.sessionKey, "waiting-session");
		holder.close();
		waiter.close();
	});

	test("renewing a lease keeps the slot alive past the TTL", async () => {
		const a = client(daemon.socketPath);
		const grant = await a.acquire({ queueKey: ENDPOINT, sessionKey: "alive-session", role: "parent" });
		assert.equal(grant.granted, true);
		const start = Date.now();
		while (Date.now() - start < 2600) {
			await sleep(400);
			assert.equal(await a.renew(grant.leaseId!), true, "renew extends the lease");
		}
		const status = await a.status();
		const queue = status?.find((entry) => entry.key === ENDPOINT);
		assert.equal(queue?.holders.some((holder) => holder.sessionKey === "alive-session"), true, "renewing session keeps its slot");
		await a.release(grant.leaseId!);
		a.close();
	});
});
