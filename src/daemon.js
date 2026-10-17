#!/usr/bin/env node
// pi-llm-queue daemon: serialises model access per endpoint over a unix socket.
// Protocol: newline-delimited JSON. See docs/daemon-protocol.md.
import { createConnection, createServer } from "node:net";
import { existsSync, readFileSync, unlinkSync, watch } from "node:fs";
import { basename, dirname } from "node:path";

const args = process.argv.slice(2);
const arg = (name) => {
	const at = args.indexOf(name);
	return at < 0 ? undefined : args[at + 1];
};
const socketPath = arg("--socket");
const configPath = arg("--config");
const modelsJsonPath = arg("--models-json");
if (!socketPath) {
	console.error("usage: daemon.js --socket <path> [--config <path>] [--models-json <path>]");
	process.exit(1);
}

let config = configPath ? JSON.parse(readFileSync(configPath, "utf8")) : {};
let defaults = config.defaults ?? {};
const modelsJson = modelsJsonPath ? JSON.parse(readFileSync(modelsJsonPath, "utf8")).providers ?? {} : {};

function send(conn, msg) {
	conn.write(`${JSON.stringify(msg)}\n`);
}

const queues = new Map();

// Queue keys are endpoint baseUrls. A provider-name key applies to that provider's
// baseUrl; a baseUrl key wins when both name the same endpoint.
function policyFor(key) {
	const queues_ = config.queues ?? {};
	if (queues_[key]) return queues_[key];
	for (const [name, entry] of Object.entries(queues_)) {
		if (!name.includes("://") && modelsJson[name]?.baseUrl === key) return entry;
	}
	return {};
}

// Explicit resource groups: queues.<name>.match lists baseUrls and/or provider names that
// share one queue, overriding baseUrl keying so endpoints can be merged or re-slotted.
// A disabled group is passthrough, so it must not re-key its members.
function groupFor(key) {
	for (const [name, entry] of Object.entries(config.queues ?? {})) {
		const match = entry?.enabled === false ? undefined : entry?.match;
		if (Array.isArray(match) && match.some((m) => m === key || modelsJson[m]?.baseUrl === key)) return name;
	}
	return key;
}

// Queue policy, re-applied on hot-reload. `enabled: false` is passthrough: unlimited slots,
// so every session is granted immediately (status reports maxStreams null).
function applyPolicy(queue, policy) {
	const pick = (field, fallback) => policy[field] ?? defaults[field] ?? fallback;
	queue.maxStreams = policy.enabled === false ? Infinity : pick("maxStreams", 1);
	// Session-switch knobs: a lease dies without heartbeats; a holder yields to a waiter when
	// it goes idle, or when its monopoly quantum (N requests / M s) runs out. "fifo" opts a
	// queue out of parent/child tiering and aging (external multi-slot queues).
	queue.ttlMs = pick("leaseTtlMs", 30000);
	queue.idleMs = pick("idleQuantumMs", 120000);
	queue.monopolyRequests = pick("monopolyRequests", 12);
	queue.monopolyMs = pick("monopolyMs", 180000);
	// Aging: anti-starvation for background jobs — a child is boosted ahead of parents after
	// this many consecutive parent grants, or after waiting this long.
	queue.agingAfterSlots = pick("agingAfterSlots", 3);
	queue.agingAfterMs = pick("agingAfterMs", 60000);
	queue.policy = pick("policy", "priority-aging");
	return queue;
}

function queueFor(key) {
	const queueKey = groupFor(key);
	if (!queues.has(queueKey)) queues.set(queueKey, applyPolicy({ queueKey, parentGrantStreak: 0, holders: [], waiting: [] }, policyFor(queueKey)));
	return queues.get(queueKey);
}

// A lease is the holder's record: TTL deadline, last model request, grant time, request count.
// The same object lives in conn.leases, so a client heartbeat refreshes it in place.
function newLease(queue, sessionKey, conn) {
	const now = Date.now();
	return { queueKey: queue.queueKey, sessionKey, conn, expires: now + queue.ttlMs, activeAt: now, grantedAt: now, requests: 0 };
}

function leaseOf(queue, sessionKey) {
	return queue.holders.find((l) => l.sessionKey === sessionKey);
}

// A model request by the holder: restarts its idle quantum and counts toward its monopoly.
function beat(lease, queue) {
	const now = Date.now();
	lease.expires = now + queue.ttlMs;
	lease.activeAt = now;
	lease.requests++;
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
		const lease = newLease(queue, waiter.sessionKey, waiter.conn);
		queue.holders.push(lease);
		remove(waiter.conn.queued, waiter);
		waiter.conn.leases.push(lease);
		send(waiter.conn, { op: "granted", queueKey: waiter.requestedKey, sessionKey: waiter.sessionKey });
	}
}

function remove(list, item) {
	const at = list.indexOf(item);
	if (at >= 0) list.splice(at, 1);
}

function release(queue, sessionKey) {
	const lease = leaseOf(queue, sessionKey);
	if (lease) {
		remove(queue.holders, lease);
		remove(lease.conn.leases, lease);
	}
	grantNext(queue);
}

// Evict holders whose lease expired (crashed session) or whose switch quantum ran out,
// and only when someone is waiting: an unmonopolised idle slot costs nobody anything.
function sweep() {
	const now = Date.now();
	for (const queue of queues.values()) {
		if (!queue.waiting.length) continue;
		for (const lease of [...queue.holders]) {
			if (now > lease.expires || now - lease.activeAt > queue.idleMs || lease.requests >= queue.monopolyRequests || now - lease.grantedAt > queue.monopolyMs)
				release(queue, lease.sessionKey);
		}
	}
}
setInterval(sweep, 1000).unref();

function handle(msg, conn) {
	// Every keyed op is resolved through groups first: a group's members all address one queue.
	// Replies always echo the key the client asked with — clients match replies on their own key.
	const requestedKey = msg.queueKey;
	if (msg.queueKey) msg.queueKey = groupFor(msg.queueKey);
	switch (msg.op) {
		case "acquire": {
			const queue = queueFor(msg.queueKey);
			const held = leaseOf(queue, msg.sessionKey);
			if (held) {
				beat(held, queue); // re-acquiring a held slot is a model request
				send(conn, { op: "granted", queueKey: requestedKey, sessionKey: msg.sessionKey });
				return;
			}
			if (queue.holders.length < queue.maxStreams) {
				const lease = newLease(queue, msg.sessionKey, conn);
				queue.holders.push(lease);
				conn.leases.push(lease);
				// A direct grant counts toward the parent-grant streak too: a parent that never
				// queues must not starve a queued child past agingAfterSlots.
				queue.parentGrantStreak = (msg.role ?? "parent") === "parent" ? queue.parentGrantStreak + 1 : 0;
				send(conn, { op: "granted", queueKey: requestedKey, sessionKey: msg.sessionKey });
				return;
			}
			const waiter = { sessionKey: msg.sessionKey, role: msg.role ?? "parent", group: msg.group, requestedKey, conn, queue, enqueuedAt: Date.now() };
			queue.waiting.push(waiter);
			conn.queued.push(waiter);
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
		// Keyed renew: an activity beat — the holder is using the slot, its switch quanta
		// restart. The reply is authoritative: granted:false tells the client the slot was
		// switched away (idle/monopoly quantum) and it must re-queue before it streams.
		case "renew": {
			if (msg.sessionKey && msg.queueKey) {
				const queue = queueFor(msg.queueKey);
				const lease = conn.leases.find((l) => l.sessionKey === msg.sessionKey && l.queueKey === msg.queueKey);
				const granted = !!lease && leaseOf(queue, msg.sessionKey) === lease;
				if (granted) beat(lease, queue);
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
					holders: q.holders.map((l) => l.sessionKey),
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
	let buffer = "";
	conn.on("data", (chunk) => {
		buffer += chunk.toString();
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
	for (const lease of [...(conn.leases ?? [])]) release(queueFor(lease.queueKey), lease.sessionKey);
	for (const w of [...(conn.queued ?? [])]) remove(w.queue.waiting, w);
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
			moved = applyPolicy({ queueKey: target, parentGrantStreak: 0, holders: [], waiting: [] }, policyFor(target));
			rebuilt.set(target, moved);
		}
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
			try {
				config = JSON.parse(readFileSync(configPath, "utf8"));
				defaults = config.defaults ?? {};
				rebuildQueues();
			} catch {}
		}, 250);
		debounce.unref();
	});
	watcher.on("error", () => {});
	watcher.unref();
}

start();
