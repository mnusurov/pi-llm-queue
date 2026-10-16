/**
 * Tracer bullet: two sessions on one endpoint are strictly serialised.
 * Driven entirely through the daemon socket seam.
 */
import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { client, sleep, startTestDaemon, type TestDaemon } from "./harness.ts";

const ENDPOINT = "http://127.0.0.1:8001/v1";

describe("serialisation of two sessions (maxStreams 1)", () => {
	let daemon: TestDaemon;

	before(async () => {
		daemon = await startTestDaemon({ config: { defaults: { maxStreams: 1 }, queues: { [ENDPOINT]: { maxStreams: 1 } } } });
	});
	after(async () => {
		await daemon.stop();
	});

	test("second session waits while the first holds the slot, then takes over", async () => {
		const a = client(daemon.socketPath);
		const b = client(daemon.socketPath);
		const grantOrder: string[] = [];

		const grantA = await a.acquire({ queueKey: ENDPOINT, sessionKey: "session-a", role: "parent" });
		assert.equal(grantA.granted, true);
		assert.ok(grantA.leaseId, "first session gets a lease");
		grantOrder.push("a");

		let grantB: Awaited<ReturnType<typeof b.acquire>> | null = null;
		const pendingB = b.acquire({ queueKey: ENDPOINT, sessionKey: "session-b", role: "parent" }).then((grant) => {
			grantOrder.push("b");
			grantB = grant;
			return grant;
		});

		await sleep(300);
		assert.equal(grantB, null, "second session must not be granted while the first holds the slot");

		const statusDuring = await a.status();
		const queue = statusDuring?.find((entry) => entry.key === ENDPOINT);
		assert.equal(queue?.holders.length, 1, "exactly one slot holder");
		assert.equal(queue?.holders[0]?.sessionKey, "session-a");
		assert.equal(queue?.waiting.parent, 1, "second session is queued");

		await a.release(grantA.leaseId!);
		const grant = await pendingB;
		assert.equal(grant.granted, true);
		assert.ok(grant.leaseId);
		assert.deepEqual(grantOrder, ["a", "b"], "grants happen in arrival order");

		const statusAfter = await b.status();
		const queueAfter = statusAfter?.find((entry) => entry.key === ENDPOINT);
		assert.equal(queueAfter?.holders[0]?.sessionKey, "session-b");
		assert.equal(queueAfter?.switches, 1, "one deliberate session switch");
		assert.equal(queueAfter?.grants, 2);

		a.close();
		b.close();
	});

	test("a session keeps the slot across consecutive requests (turn-based affinity)", async () => {
		const a = client(daemon.socketPath);
		const b = client(daemon.socketPath);

		const first = await a.acquire({ queueKey: ENDPOINT, sessionKey: "session-a2", role: "parent" });
		assert.equal(first.granted, true);
		const second = await a.acquire({ queueKey: ENDPOINT, sessionKey: "session-a2", role: "parent" });
		assert.equal(second.granted, true, "same session reuses its slot for the next request");
		assert.equal(second.leaseId, first.leaseId, "affinity keeps one lease per session run");

		let grantB: unknown = null;
		const pendingB = b.acquire({ queueKey: ENDPOINT, sessionKey: "session-b2", role: "parent" }).then((grant) => {
			grantB = grant;
			return grant;
		});
		await sleep(200);
		assert.equal(grantB, null, "second session stays queued while the first holds the slot");

		await a.release(first.leaseId!);
		const grant = await pendingB;
		assert.equal((grant as { granted: boolean }).granted, true);
		a.close();
		b.close();
	});

	test("maxStreams 2 grants two sessions at once and queues the third", async () => {
		const wide = await startTestDaemon({ config: { defaults: { maxStreams: 2 }, queues: { [ENDPOINT]: { maxStreams: 2 } } } });
		try {
			const a = client(wide.socketPath);
			const b = client(wide.socketPath);
			const c = client(wide.socketPath);
			const grantA = await a.acquire({ queueKey: ENDPOINT, sessionKey: "a", role: "parent" });
			const grantB = await b.acquire({ queueKey: ENDPOINT, sessionKey: "b", role: "parent" });
			assert.equal(grantA.granted, true);
			assert.equal(grantB.granted, true);
			let grantC: unknown = null;
			const pendingC = c.acquire({ queueKey: ENDPOINT, sessionKey: "c", role: "child" }).then((grant) => {
				grantC = grant;
				return grant;
			});
			await sleep(200);
			assert.equal(grantC, null, "third session waits for a free slot");
			await a.release(grantA.leaseId!);
			const grant = await pendingC;
			assert.equal((grant as { granted: boolean }).granted, true);
			a.close();
			b.close();
			c.close();
		} finally {
			await wide.stop();
		}
	});

	test("a disconnected session's lease is reclaimed and handed to the next waiter", async () => {
		const a = client(daemon.socketPath);
		const b = client(daemon.socketPath);
		const grantA = await a.acquire({ queueKey: ENDPOINT, sessionKey: "session-crashed", role: "parent" });
		assert.equal(grantA.granted, true);
		const pendingB = b.acquire({ queueKey: ENDPOINT, sessionKey: "session-survivor", role: "child" });
		await sleep(100);
		a.close();
		const grantB = await pendingB;
		assert.equal(grantB.granted, true, "slot is released when the holder's connection dies");
		a.close();
		b.close();
	});
});
