#!/usr/bin/env node
// pi-llm-queue daemon: serialises model access per endpoint over a unix socket.
// Protocol: newline-delimited JSON. See docs/daemon-protocol.md.
import { createConnection, createServer } from "node:net";
import { appendFileSync, existsSync, readFileSync, unlinkSync, watch } from "node:fs";
import { basename, dirname } from "node:path";
import { loadValidConfig, groupKeyFor, queueEntryFor } from "./config.js";

const args = process.argv.slice(2);
const arg = (name) => {
	const at = args.indexOf(name);
	return at < 0 ? undefined : args[at + 1];
};
const socketPath = arg("--socket");
const configPath = arg("--config");
const modelsJsonPath = arg("--models-json");
// Sweep interval: 1 s in production, a short tick in tests so a TTL-bound test costs milliseconds.
const sweepMs = Number(arg("--sweep-ms") ?? 1000);
if (!socketPath) {
	console.error("usage: daemon.js --socket <path> [--config <path>] [--models-json <path>] [--sweep-ms <ms>]");
	process.exit(1);
}

// A bad config must not kill the daemon: print clear errors and serve with defaults.
// A missing file is all defaults — the documented default install, no error.
const loaded = loadValidConfig(configPath ?? "");
for (const e of loaded.errors) console.error(`pi-llm-queue daemon: config: ${e}`);
let config = loaded.config;
let defaults = config.defaults ?? {};
// models.json is optional for pi: a missing catalog is an empty one, never a startup crash (the
// extension spawns the daemon detached, so a crash would be silent). A present-but-unreadable
// catalog is reported: provider-name queue keys and group members would silently resolve to nothing.
let modelsJson = {};
if (modelsJsonPath && existsSync(modelsJsonPath)) {
	try {
		modelsJson = JSON.parse(readFileSync(modelsJsonPath, "utf8")).providers ?? {};
	} catch (error) {
		console.error(`pi-llm-queue daemon: models.json unreadable (${error.message}): provider-name queue keys resolve to nothing`);
	}
}

function send(conn, msg) {
	conn.write(`${JSON.stringify(msg)}\n`);
}

// Event log (#47): with `logFile` set, the daemon appends one JSON line per queue event, so a
// finished run can be read back from one file — the extension spawns the daemon detached and its
// stderr is discarded. The path is read per event, so a hot-reload can turn the log on, off, or
// move it. The queue key logged is the queue that owns the slot: a group's name when the client
// addressed it by a member key.
function logEvent(event, queue, sessionKey, role, reason) {
	const path = defaults.logFile;
	if (!path) return;
	const line = { event, queueKey: queue.queueKey, sessionKey, role, ts: new Date().toISOString() };
	if (reason) line.reason = reason;
	// Best effort, swallowed: a bad path, a full disk or a log rotation race must never take the
	// daemon down (it is spawned detached, so a crash is silent) nor reject the request that logged.
	try {
		appendFileSync(path, `${JSON.stringify(line)}\n`, "utf8");
	} catch {}
}

const queues = new Map();

// Idle-exit: the daemon self-terminates `daemonIdleExitMs` after the last client disconnects,
// so no orphan lingers. A client that reconnects within the grace cancels the exit, so quick
// relaunches (a session restarting) keep the warm daemon. `Infinity` disables idle-exit.
let clientCount = 0;
let idleTimer = null;
function scheduleIdleExit() {
	const ms = defaults.daemonIdleExitMs ?? 60000;
	if (!isFinite(ms)) return;
	clearTimeout(idleTimer);
	idleTimer = setTimeout(() => {
		try {
			unlinkSync(socketPath);
		} catch {}
		process.exit(0);
	}, ms);
	idleTimer.unref();
}

// Queue keys are endpoint baseUrls. A provider-name key applies to that provider's
// baseUrl; a baseUrl key wins when both name the same endpoint. Resolution lives in
// src/config.js so the extension and the daemon share one rule.
function policyFor(key) {
	return queueEntryFor(config.queues ?? {}, modelsJson, { baseUrl: key }) ?? {};
}

// Explicit resource groups: queues.<name>.match lists baseUrls and/or provider names that
// share one queue, overriding baseUrl keying so endpoints can be merged or re-slotted.
function groupFor(key) {
	return groupKeyFor(config.queues ?? {}, modelsJson, { baseUrl: key });
}

// Queue policy, re-applied on hot-reload. `enabled: false` is passthrough: unlimited slots,
// so every session is granted immediately (status reports maxStreams null).
function applyPolicy(queue, policy) {
	const pick = (field, fallback) => policy[field] ?? defaults[field] ?? fallback;
	queue.maxStreams = policy.enabled === false ? Infinity : pick("maxStreams", 1);
	// A lease dies only without heartbeats (TTL): a live holder is never preempted (#49), so the
	// slot moves on voluntary release, abort, disconnect, or TTL expiry alone. "fifo" opts a
	// queue out of parent/child tiering and aging (external multi-slot queues).
	queue.ttlMs = pick("leaseTtlMs", 30000);
	// Aging: anti-starvation for background jobs — a child is boosted ahead of parents after
	// this many consecutive parent grants, or after waiting this long.
	queue.agingAfterSlots = pick("agingAfterSlots", 3);
	queue.agingAfterMs = pick("agingAfterMs", 60000);
	queue.policy = pick("policy", "priority-aging");
	queue.idleYieldMs = pick("idleYieldMs", 30000);
	return queue;
}

function newQueue(queueKey) {
	return applyPolicy({ queueKey, parentGrantStreak: 0, switches: 0, holders: [], waiting: [] }, policyFor(queueKey));
}

function queueFor(key) {
	const queueKey = groupFor(key);
	if (!queues.has(queueKey)) queues.set(queueKey, newQueue(queueKey));
	return queues.get(queueKey);
}

// A lease is the holder's record: its TTL deadline, plus the tier it was acquired with and the
// group label it came in with, so `status` can report a Holder by tier and label and not only by
// session id. The same object lives in conn.leases, so a client heartbeat refreshes it in place.
function newLease(queue, sessionKey, conn, activity, role, group) {
	return { queueKey: queue.queueKey, sessionKey, conn, role, group, expires: Date.now() + queue.ttlMs, activity: !!activity, lastActivityAt: Date.now() };
}

function leaseOf(queue, sessionKey) {
	return queue.holders.find((l) => l.sessionKey === sessionKey);
}

// Pick the next waiter: an aged child (longest-waiting) jumps the queue; otherwise the
// earliest parent, then the earliest child — FIFO within each tier (parent > child).
function pickNext(queue) {
	if (queue.policy === "fifo") return queue.waiting[0];
	const now = Date.now();
	let aged = null;
	for (const w of queue.waiting) {
		if (w.role !== "child") continue;
		if (now - w.enqueuedAt >= queue.agingAfterMs || queue.parentGrantStreak >= queue.agingAfterSlots) {
			if (!aged || w.enqueuedAt < aged.enqueuedAt) aged = w;
		}
	}
	if (aged) return aged;
	return queue.waiting.find((w) => w.role === "parent") ?? queue.waiting[0];
}

function grantNext(queue) {
	while (queue.waiting.length && queue.holders.length < queue.maxStreams) {
		const waiter = pickNext(queue);
		remove(queue.waiting, waiter);
		queue.parentGrantStreak = waiter.role === "parent" ? queue.parentGrantStreak + 1 : 0;
		const lease = newLease(queue, waiter.sessionKey, waiter.conn, waiter.activity, waiter.role, waiter.group);
		queue.holders.push(lease);
		queue.switches++; // handing the slot to a new session is the switch that re-prefills the KV cache
		logEvent("grant", queue, waiter.sessionKey, waiter.role);
		logEvent("switch", queue, waiter.sessionKey, waiter.role);
		remove(waiter.conn.queued, waiter);
		waiter.conn.leases.push(lease);
		send(waiter.conn, { op: "granted", queueKey: waiter.requestedKey, sessionKey: waiter.sessionKey });
	}
}

function remove(list, item) {
	const at = list.indexOf(item);
	if (at >= 0) list.splice(at, 1);
}

function release(queue, sessionKey, reason) {
	const lease = leaseOf(queue, sessionKey);
	if (lease) {
		remove(queue.holders, lease);
		remove(lease.conn.leases, lease);
		// The log names the cause: a TTL reclaim is an evict — the only eviction reason after ADR
		// 0002 — a yield is a release with reason "idle", a voluntary return a bare release.
		logEvent(reason === "ttl" ? "evict" : "release", queue, sessionKey, lease.role, reason);
	}
	grantNext(queue);
}

// Evict holders whose lease expired: a crashed session stops heartbeating and loses its slot.
// A live holder is never preempted (#49) — every switch re-prefills the KV cache, and a
// child-to-child preemption benefits nobody. The sweep is unconditional so `/queue` never
// reports a ghost holder, whether or not anyone is waiting.
function sweep() {
	const now = Date.now();
	for (const queue of queues.values()) {
		for (const lease of [...queue.holders]) if (now > lease.expires) release(queue, lease.sessionKey, "ttl");
		// Idle-yield: an activity-flagged holder silent past idleYieldMs with waiters present yields
		// the slot. A lone holder is never disturbed; unflagged (old-client) leases are never yielded.
		if (queue.idleYieldMs > 0 && queue.waiting.length > 0) {
			for (const lease of [...queue.holders]) {
				if (lease.activity && now - lease.lastActivityAt > queue.idleYieldMs) release(queue, lease.sessionKey, "idle");
			}
		}
	}
}
setInterval(sweep, sweepMs).unref();

function handle(msg, conn) {
	// A keyless acquire would create an unnamed queue that silently holds a slot.
	if ((msg.op === "acquire" || msg.op === "release" || msg.op === "cancel") && !msg.queueKey) {
		send(conn, { op: "error", reason: `${msg.op} requires queueKey` });
		return;
	}
	// Every keyed op is resolved through groups first: a group's members all address one queue.
	// Replies always echo the key the client asked with — clients match replies on their own key.
	const requestedKey = msg.queueKey;
	if (msg.queueKey) msg.queueKey = groupFor(msg.queueKey);
	switch (msg.op) {
		case "acquire": {
			const queue = queueFor(msg.queueKey);
			const held = leaseOf(queue, msg.sessionKey);
			if (held) {
				held.expires = Date.now() + queue.ttlMs; // re-acquiring a held slot refreshes its TTL
				send(conn, { op: "granted", queueKey: requestedKey, sessionKey: msg.sessionKey });
				return;
			}
			if (queue.holders.length < queue.maxStreams) {
				const lease = newLease(queue, msg.sessionKey, conn, msg.activity, msg.role ?? "parent", msg.group);
				queue.holders.push(lease);
				conn.leases.push(lease);
				// A direct grant counts toward the parent-grant streak too: a parent that never
				// queues must not starve a queued child past agingAfterSlots.
				queue.parentGrantStreak = (msg.role ?? "parent") === "parent" ? queue.parentGrantStreak + 1 : 0;
				logEvent("grant", queue, msg.sessionKey, msg.role ?? "parent");
				send(conn, { op: "granted", queueKey: requestedKey, sessionKey: msg.sessionKey });
				return;
			}
			const waiter = { sessionKey: msg.sessionKey, role: msg.role ?? "parent", group: msg.group, requestedKey, conn, queue, enqueuedAt: Date.now(), activity: !!msg.activity };
			queue.waiting.push(waiter);
			conn.queued.push(waiter);
			logEvent("queued", queue, msg.sessionKey, msg.role ?? "parent");
			return;
		}
		case "release":
			if (queues.has(msg.queueKey)) release(queues.get(msg.queueKey), msg.sessionKey);
			send(conn, { op: "ok" });
			return;
		case "cancel": {
			// Drop a queued request the client aborted; if it was already granted, release the slot.
			const queue = queues.get(msg.queueKey);
			if (queue) {
				const w = queue.waiting.find((x) => x.sessionKey === msg.sessionKey && x.conn === conn);
				if (w) {
					remove(queue.waiting, w);
					remove(conn.queued, w);
				}
				if (leaseOf(queue, msg.sessionKey)) release(queue, msg.sessionKey);
			}
			return;
		}
		// Bare renew: heartbeat, keeps every lease of this connection alive.
		// Keyed renew: an activity beat — the holder is using the slot, so its TTL refreshes. The
		// reply is authoritative: granted:false tells the client the slot is gone (TTL expiry, a
		// release, a disconnect) and it must re-queue before it streams.
		case "renew": {
			if (msg.sessionKey && msg.queueKey) {
				const queue = queueFor(msg.queueKey);
				const lease = conn.leases.find((l) => l.sessionKey === msg.sessionKey && l.queueKey === msg.queueKey);
				const granted = !!lease && leaseOf(queue, msg.sessionKey) === lease;
				if (granted) { lease.expires = Date.now() + queue.ttlMs; lease.lastActivityAt = Date.now(); }
				send(conn, { op: "ok", queueKey: requestedKey, sessionKey: msg.sessionKey, granted });
				return;
			}
			for (const lease of [...conn.leases]) lease.expires = Date.now() + queueFor(lease.queueKey).ttlMs;
			send(conn, { op: "ok" });
			return;
		}
		case "role": {
			// /queue promote|demote: re-tier a live queued waiter in place (no reply, like cancel).
			for (const queue of queues.values()) {
				const w = queue.waiting.find((x) => x.sessionKey === msg.sessionKey && x.conn === conn);
				if (w) {
					w.role = msg.role === "child" ? "child" : "parent";
					break;
				}
			}
			return;
		}
		case "status":
			send(conn, {
				op: "status",
				queues: [...queues.values()].map((q) => ({
					queueKey: q.queueKey,
					maxStreams: q.maxStreams,
					switches: q.switches,
					// Holders are reported like Waiters: a raw client reads tier and group from the
					// reply, so it needs no extension-side parsing to build a dashboard.
					holders: q.holders.map((l) => ({ sessionKey: l.sessionKey, role: l.role, group: l.group })),
					waiting: q.waiting.map((w) => ({ sessionKey: w.sessionKey, role: w.role, group: w.group })),
				})),
			});
			return;
		default:
			send(conn, { op: "error", reason: `unknown op: ${msg.op}` });
	}
}

const server = createServer((conn) => {
	conn.leases = [];
	conn.queued = [];
	clientCount++;
	if (idleTimer) {
		clearTimeout(idleTimer);
		idleTimer = null;
	}
	let buffer = "";
	// Streaming decoder: a chunk that splits a multi-byte character must not corrupt the key.
	const decoder = new TextDecoder("utf8");
	conn.on("data", (chunk) => {
		buffer += decoder.decode(chunk, { stream: true });
		// A peer that never sends a newline must not grow the daemon's memory without bound.
		if (buffer.length > 1e5) {
			conn.destroy();
			return;
		}
		const lines = buffer.split("\n");
		buffer = lines.pop();
		for (const line of lines) {
			if (!line.trim()) continue;
			let msg;
			try {
				msg = JSON.parse(line);
			} catch {
				send(conn, { op: "error", reason: "invalid json" });
				continue;
			}
			handle(msg, conn);
		}
	});
	conn.on("close", () => cleanup(conn));
	conn.on("error", () => cleanup(conn));
});

// A disconnect is an abort: drop the queued request and release any lease it held.
// Iterate a copy: release() splices conn.leases, and a client can hold several leases.
function cleanup(conn) {
	if (conn.cleaned) return; // close and error both fire cleanup: count the disconnect once
	conn.cleaned = true;
	for (const lease of [...(conn.leases ?? [])]) release(queueFor(lease.queueKey), lease.sessionKey);
	for (const w of [...(conn.queued ?? [])]) remove(w.queue.waiting, w);
	if (--clientCount <= 0) scheduleIdleExit();
}

// Two sessions can autostart at once: the loser finds the socket taken and stands down.
server.on("error", (error) => {
	if (error.code === "EADDRINUSE") process.exit(0);
	throw error;
});

// A crashed daemon leaves a dead socket file behind; reclaim it. If a live daemon
// answers it, stand down and let that one serve the clients.
function start() {
	if (!existsSync(socketPath)) {
		server.listen(socketPath);
		return;
	}
	const probe = createConnection(socketPath);
	probe.on("connect", () => {
		probe.destroy();
		process.exit(0);
	});
	probe.on("error", () => {
		probe.destroy();
		// Reclaim the stale socket. If a concurrent daemon won the race, its unlink/listen
		// wins: our unlink throws ENOENT (exit 0) or our listen hits EADDRINUSE (server
		// "error" handler exits 0). Never guard with existsSync — that deletes the winner's
		// live socket and split-brains the daemon.
		try {
			unlinkSync(socketPath);
		} catch {
			process.exit(0);
		}
		server.listen(socketPath);
	});
}

process.on("SIGTERM", () => {
	try {
		unlinkSync(socketPath);
	} catch {}
	process.exit(0);
});

// Rebuild queues from the reloaded config, carrying holders and waiters with them: adding a
// group at runtime must not leave the endpoint's old slot pool running beside the group's.
// shortcut: deleting a group leaves its live queue keyed by the old group name until restart.
function rebuildQueues() {
	const rebuilt = new Map();
	for (const [key, queue] of queues) {
		const target = groupFor(key);
		let moved = rebuilt.get(target);
		if (!moved) {
			moved = newQueue(target);
			rebuilt.set(target, moved);
		}
		moved.switches += queue.switches; // a rebuild (group merge) carries the switch counts with it
		for (const lease of queue.holders) {
			lease.queueKey = target;
			moved.holders.push(lease);
		}
		for (const waiter of queue.waiting) {
			waiter.queue = moved;
			moved.waiting.push(waiter);
		}
	}
	queues.clear();
	for (const [key, queue] of rebuilt) queues.set(key, queue);
	for (const queue of queues.values()) grantNext(queue);
}

// Hot-reload: a config edit re-applies queue policy to live queues and grants whoever the
// new capacity frees — no daemon restart, no pi restart. A half-written file keeps the last
// good config. Watch the directory, not the file: an atomic-save editor replaces the file and
// a file watcher goes dead.
if (configPath) {
	let debounce = null;
	const watcher = watch(dirname(configPath), (_event, file) => {
		if (file && file !== basename(configPath)) return;
		clearTimeout(debounce);
		debounce = setTimeout(() => {
			if (!existsSync(configPath)) return; // config deleted: keep the last good config
			const next = loadValidConfig(configPath);
			if (next.errors.length) {
				// A half-written file (invalid JSON) is a silent no-op: the last good config is kept.
				if (!next.invalidJson) for (const e of next.errors) console.error(`pi-llm-queue daemon: config reload rejected: ${e}`);
				return;
			}
			config = next.config;
			defaults = config.defaults ?? {};
			rebuildQueues();
		}, 250);
		debounce.unref();
	});
	watcher.on("error", () => {});
	watcher.unref();
}

start();
// Arm idle-exit at startup too: a daemon spawned by ensureDaemon whose client never reaches it
// (connect fails, fail-open) must not hold the socket forever with zero clients.
scheduleIdleExit();
