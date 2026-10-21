// Log-file seam (#47): with `logFile` set, the daemon appends JSON-lines events so a finished run
// is readable from one file. The seam is the daemon's own file output: script a scenario through
// the socket protocol, then read the file back. No daemon internals, no new protocol op.
import assert from "node:assert/strict";
import { mkdtempSync, existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { after, describe, it } from "node:test";
import { closeAllClients, connectClient, leaseTerms, sleep, startDaemon, stopAllDaemons, QUEUE_KEY, type Client, type Daemon } from "./harness.ts";

const dir = mkdtempSync(join(tmpdir(), "llm-queue-log-"));
const logPath = () => join(dir, `events-${Math.random().toString(36).slice(2)}.jsonl`);

// appendFileSync is synchronous, so every event a client's reply caused is on disk by the time
// that reply arrives: reading after the reply is the whole synchronisation this seam needs.
const readEvents = (path: string) =>
	readFileSync(path, "utf8")
		.split("\n")
		.filter((line) => line.trim())
		.map((line) => JSON.parse(line));

// The log file is created by the daemon's first write, so a count before the first event is zero.
const eventCount = (path: string) => (existsSync(path) ? readEvents(path).length : 0);

// ts is a wall-clock timestamp, so it cannot be a literal in an expected sequence; assert it
// separately ("carries a timestamp") and compare the rest of the line exactly.
const eventLines = (path: string) => readEvents(path).map((line) => ({ event: line.event, queueKey: line.queueKey, sessionKey: line.sessionKey, role: line.role, reason: line.reason }));

describe("daemon event log", () => {
	const daemons: Daemon[] = [];
	const clients: Client[] = [];

	// sweepMs 50: the sweep is the clock the TTL and idle-yield events ride on, so a short tick
	// keeps a sweep-bound test in milliseconds.
	const start = (defaults: Record<string, unknown>, sweepMs = 50) => {
		const d = startDaemon({ defaults: { maxStreams: 1, ...defaults }, queues: {} }, undefined, undefined, sweepMs);
		daemons.push(d);
		return d;
	};
	const open = async (d: Daemon) => {
		const c = await connectClient(d.socketPath);
		clients.push(c);
		return c;
	};
	const acquire = (c: Client, sessionKey: string, role = "parent", activity = true) => c.send({ op: "acquire", queueKey: QUEUE_KEY, sessionKey, role, activity });
	const release = (c: Client, sessionKey: string) => c.send({ op: "release", queueKey: QUEUE_KEY, sessionKey });
	const beat = (c: Client, sessionKey: string) => c.send({ op: "renew", queueKey: QUEUE_KEY, sessionKey });
	const granted = (sessionKey: string, terms = leaseTerms()) => ({ op: "granted", queueKey: QUEUE_KEY, sessionKey, ...terms });

	// The session is a Waiter: asked of the daemon's own status view, polled with a deadline.
	// Presence is polled, never slept for — and it must be a Waiter, or the handover that follows
	// is a direct grant and never writes a switch line.
	const waiting = async (c: Client, sessionKey: string) => {
		for (let attempt = 0; attempt < 100; attempt++) {
			c.send({ op: "status" });
			const reply = await c.next(3000);
			const queue = reply.queues?.find((q: { queueKey: string }) => q.queueKey === QUEUE_KEY);
			if (queue?.waiting?.some((w: { sessionKey: string }) => w.sessionKey === sessionKey)) return;
			await sleep(5);
		}
		throw new Error(`${sessionKey} never queued`);
	};

	after(() => {
		for (const c of clients) c.close();
		for (const d of daemons) d.stop();
		closeAllClients();
		stopAllDaemons();
	});

	it("logs grant, queued, release, grant, switch for a two-client handover", async () => {
		const file = logPath();
		const d = start({ logFile: file });
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B", "child");
		await assert.rejects(b.next(200), /no message from daemon within/);
		release(a, "A");
		assert.deepEqual(await b.next(3000), granted("B"));

		assert.deepEqual(eventLines(file), [
			{ event: "grant", queueKey: QUEUE_KEY, sessionKey: "A", role: "parent", reason: undefined },
			{ event: "queued", queueKey: QUEUE_KEY, sessionKey: "B", role: "child", reason: undefined },
			{ event: "release", queueKey: QUEUE_KEY, sessionKey: "A", role: "parent", reason: undefined },
			{ event: "grant", queueKey: QUEUE_KEY, sessionKey: "B", role: "child", reason: undefined },
			{ event: "switch", queueKey: QUEUE_KEY, sessionKey: "B", role: "child", reason: undefined },
		]);
	});

	// The log must not be behind the reply. The sink promises that every event a reply caused is on
	// disk before that reply goes out, so a reader that opens the file the moment the reply lands
	// sees the whole story. The promise is the decisions module's now (ADR 0010): the batch order —
	// every line before any reply — is asserted there, deterministically and without a socket. What
	// the socket adds is that the lines are on disk at all, so one handover is enough here.
	it("the whole handover is on disk when the reply lands", async () => {
		const file = logPath();
		const d = start({ logFile: file });
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B", "child");
		await waiting(b, "B");
		release(a, "A");
		assert.deepEqual(await b.next(3000), granted("B"));

		// The file is read the moment the reply lands — no round trip in between, because the
		// window between the daemon's reply and its log write is what this pins.
		// grant A, queued B, release A, grant B, switch B: five events, all of them on disk
		// before the reply that caused them went out.
		assert.equal(eventCount(file), 5, "the log must hold every event of the batch when the reply lands");
	});

	// The event log's vocabulary — the field names, the `role` the wire calls the tier, the
	// timestamp — is the decisions module's, and it is asserted in that module's suite
	// (test/decisions.test.ts). What is left here is the log's own behaviour: the file, the
	// events, and the readers that come back for them.

	it("a reader can compute a session's wait time from the file alone", async () => {
		const file = logPath();
		const d = start({ logFile: file });
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B", "child");
		await assert.rejects(b.next(200), /no message from daemon within/);
		await sleep(250); // B stays queued: the wait a post-mortem wants to read
		release(a, "A");
		assert.deepEqual(await b.next(3000), granted("B"));

		const lines = readEvents(file);
		const queued = lines.find((line) => line.event === "queued" && line.sessionKey === "B");
		const grant = lines.find((line) => line.event === "grant" && line.sessionKey === "B");
		assert.ok(queued && grant, "queued and grant for the same session key are the pair a reader needs");
		const waited = Date.parse(grant.ts) - Date.parse(queued.ts);
		assert.ok(waited >= 200, `queued→grant gap must reflect the real wait, got ${waited}ms`);
	});

	// After ADR 0002 the TTL is the only eviction: a client that stopped heartbeating loses its
	// slot, and the log must name that cause, because it is the one a post-mortem cannot infer.
	it("logs a TTL reclaim as an evict carrying reason ttl", async () => {
		const file = logPath();
		const d = start({ logFile: file, leaseTtlMs: 1000 });
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A", leaseTerms(1000)));
		acquire(b, "B", "child");
		await assert.rejects(b.next(200), /no message from daemon within/);

		assert.deepEqual(await b.next(3000), granted("B", leaseTerms(1000)), "a dead client's slot is reclaimed and handed on");
		a.send({ op: "status" });
		// A is gone from the holder list: the evict leaves no ghost holder for /queue to report.
		assert.deepEqual((await a.next(3000)).queues[0].holders.map((h: any) => h.sessionKey), ["B"], "the evict leaves no ghost holder in status");

		assert.deepEqual(eventLines(file), [
			{ event: "grant", queueKey: QUEUE_KEY, sessionKey: "A", role: "parent", reason: undefined },
			{ event: "queued", queueKey: QUEUE_KEY, sessionKey: "B", role: "child", reason: undefined },
			{ event: "evict", queueKey: QUEUE_KEY, sessionKey: "A", role: "parent", reason: "ttl" },
			{ event: "grant", queueKey: QUEUE_KEY, sessionKey: "B", role: "child", reason: undefined },
			{ event: "switch", queueKey: QUEUE_KEY, sessionKey: "B", role: "child", reason: undefined },
		]);
	});

	// ADR 0003: an idle-yield is a release, not an eviction — the log tells the two apart by reason.
	it("logs an idle yield as a release carrying reason idle", async () => {
		const file = logPath();
		const d = start({ logFile: file, idleYieldMs: 100 });
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A", leaseTerms(30000, 100)));
		// B queues, then the sweep yields A (silent past idleYieldMs) and grants B: the yield is
		// the sweep's clock, so B's grant is the assertion that waits for it.
		acquire(b, "B", "child");
		assert.deepEqual(await b.next(2000), granted("B", leaseTerms(30000, 100)), "idle holder yields, waiter granted");

		assert.deepEqual(eventLines(file), [
			{ event: "grant", queueKey: QUEUE_KEY, sessionKey: "A", role: "parent", reason: undefined },
			{ event: "queued", queueKey: QUEUE_KEY, sessionKey: "B", role: "child", reason: undefined },
			{ event: "release", queueKey: QUEUE_KEY, sessionKey: "A", role: "parent", reason: "idle" },
			{ event: "grant", queueKey: QUEUE_KEY, sessionKey: "B", role: "child", reason: undefined },
			{ event: "switch", queueKey: QUEUE_KEY, sessionKey: "B", role: "child", reason: undefined },
		]);
	});

	// Observability must never become an availability risk: the daemon is spawned detached, so a
	// log write that throws (missing directory, full disk) has to leave it serving.
	it("serves requests when the log path is unwritable", async () => {
		const file = join(dir, "no-such-dir", "nested", "events.jsonl");
		const d = start({ logFile: file });
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"), "a failed log write must never reject an acquire");
		acquire(b, "B", "child");
		await assert.rejects(b.next(200), /no message from daemon within/);
		release(a, "A");
		assert.deepEqual(await b.next(3000), granted("B"), "the queue keeps handing over with an unwritable log");
		b.send({ op: "status" });
		assert.equal((await b.next(3000)).op, "status", "the daemon is still serving after the failed writes");
	});

	// The Grace window (GLOSSARY): a Parent woken by its Child keeps the Slot, so the handback
	// costs no switch. A re-acquire of a held slot is a TTL refresh, so the log stays switch-free.
	it("keeps the Parent-Child handback grace switch-free in the log", async () => {
		const file = logPath();
		const d = start({ logFile: file });
		const a = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		for (let i = 0; i < 3; i++) {
			await sleep(100); // inside the grace window
			beat(a, "A");
			assert.deepEqual(await a.next(3000), { op: "ok", queueKey: QUEUE_KEY, sessionKey: "A", granted: true }, "the grace beats keep the slot");
			acquire(a, "A"); // the woken run's acquire: the slot is kept, not re-granted
			assert.deepEqual(await a.next(3000), granted("A"));
		}

		assert.deepEqual(readEvents(file).map((line) => line.event), ["grant"], "the grace window logs one grant and no switch");
	});

	// The docs claim the path is read per event, so a hot-reload can turn the log on, off, or move
	// it. Seam: write the config file, drive the protocol, read the log files — no daemon internals.
	it("follows logFile across a hot-reload: on, moved, then off", async () => {
		const first = logPath();
		const second = logPath();
		const d = start({ logFile: first });
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B", "child");
		await assert.rejects(b.next(200), /no message from daemon within/, "B queues behind A");
		assert.deepEqual(eventLines(first).map((line) => line.event), ["grant", "queued"], "the configured path receives the events of the running queue");

		writeFileSync(d.configPath, JSON.stringify({ defaults: { maxStreams: 1, logFile: second }, queues: {} }));
		await sleep(600); // reload debounce
		release(a, "A");
		assert.deepEqual(await b.next(3000), granted("B"), "the queued child is granted after the reload");
		assert.deepEqual(eventLines(second).map((line) => line.event), ["release", "grant", "switch"], "after the reload the events land in the moved file");
		assert.deepEqual(eventLines(first).map((line) => line.event), ["grant", "queued"], "the old file stops receiving events: a move, not a copy");

		const c = await open(d);
		const written = readdirSync(dir).filter((name) => name.endsWith(".jsonl")).sort();
		writeFileSync(d.configPath, JSON.stringify({ defaults: { maxStreams: 1 }, queues: {} }));
		await sleep(600);
		release(b, "B");
		acquire(c, "C");
		assert.deepEqual(await c.next(3000), granted("C"), "the queue keeps serving with the log off");
		assert.deepEqual(eventLines(second).map((line) => line.event), ["release", "grant", "switch"], "with logFile unset nothing more is written: off, not silently moved");
		assert.deepEqual(readdirSync(dir).filter((name) => name.endsWith(".jsonl")).sort(), written, "turning the log off creates no new file");
	});

	it("writes no log file when logFile is unset, and serves exactly as before", async () => {
		const d = start({});
		const a = await open(d);
		const b = await open(d);

		acquire(a, "A");
		assert.deepEqual(await a.next(3000), granted("A"));
		acquire(b, "B", "child");
		await assert.rejects(b.next(200), /no message from daemon within/);
		release(a, "A");
		assert.deepEqual(await b.next(3000), granted("B"));

		// The daemon's own directory (socket + config live there): with no logFile configured it
		// creates no log file, so an install that never asked for one pays nothing for the feature.
		assert.deepEqual(readdirSync(dirname(d.configPath)).filter((name) => name.endsWith(".jsonl")), [], "no logFile, no file: behaviour identical to today");
	});
});
