#!/usr/bin/env node
// pi-llm-queue daemon: serialises model access per endpoint over a unix socket.
// Protocol: newline-delimited JSON. See docs/daemon-protocol.md.
import { createConnection, createServer } from "node:net";
import { existsSync, readFileSync, unlinkSync } from "node:fs";

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

const config = configPath ? JSON.parse(readFileSync(configPath, "utf8")) : {};
const defaults = config.defaults ?? {};
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

function queueFor(key) {
	if (!queues.has(key)) {
		const policy = policyFor(key);
		queues.set(key, {
			queueKey: key,
			maxStreams: policy.maxStreams ?? defaults.maxStreams ?? 1,
			// Session-switch knobs: a lease dies without heartbeats; a holder yields to a waiter
			// when it goes idle, or when its monopoly quantum (N requests / M s) runs out.
			ttlMs: policy.leaseTtlMs ?? defaults.leaseTtlMs ?? 30000,
			idleMs: policy.idleQuantumMs ?? defaults.idleQuantumMs ?? 120000,
			monopolyRequests: policy.monopolyRequests ?? defaults.monopolyRequests ?? 12,
			monopolyMs: policy.monopolyMs ?? defaults.monopolyMs ?? 180000,
			holders: [],
			waiting: [],
		});
	}
	return queues.get(key);
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

function grantNext(queue) {
	while (queue.waiting.length && queue.holders.length < queue.maxStreams) {
		const waiter = queue.waiting.shift();
		const lease = newLease(queue, waiter.sessionKey, waiter.conn);
		queue.holders.push(lease);
		remove(waiter.conn.queued, waiter);
		waiter.conn.leases.push(lease);
		send(waiter.conn, { op: "granted", queueKey: queue.queueKey, sessionKey: waiter.sessionKey });
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
	switch (msg.op) {
		case "acquire": {
			const queue = queueFor(msg.queueKey);
			const held = leaseOf(queue, msg.sessionKey);
			if (held) {
				beat(held, queue); // re-acquiring a held slot is a model request
				send(conn, { op: "granted", queueKey: queue.queueKey, sessionKey: msg.sessionKey });
				return;
			}
			if (queue.holders.length < queue.maxStreams) {
				const lease = newLease(queue, msg.sessionKey, conn);
				queue.holders.push(lease);
				conn.leases.push(lease);
				send(conn, { op: "granted", queueKey: queue.queueKey, sessionKey: msg.sessionKey });
				return;
			}
			const waiter = { sessionKey: msg.sessionKey, role: msg.role ?? "parent", conn, queue };
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
		// Heartbeat (no keys): keep every lease of this connection alive.
		// Keyed renew: an activity beat — the holder is using the slot, its quanta restart.
		case "renew": {
			for (const lease of conn.leases) {
				if (msg.sessionKey && lease.sessionKey === msg.sessionKey && lease.queueKey === msg.queueKey) beat(lease, queueFor(lease.queueKey));
				else lease.expires = Date.now() + queueFor(lease.queueKey).ttlMs;
			}
			send(conn, { op: "ok" });
			return;
		}
		case "status":
			send(conn, {
				op: "status",
				queues: [...queues.values()].map((q) => ({
					queueKey: q.queueKey,
					maxStreams: q.maxStreams,
					holders: q.holders.map((l) => l.sessionKey),
					waiting: q.waiting.map((w) => ({ sessionKey: w.sessionKey, role: w.role })),
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
function cleanup(conn) {
	for (const lease of conn.leases ?? []) release(queueFor(lease.queueKey), lease.sessionKey);
	for (const w of conn.queued ?? []) remove(w.queue.waiting, w);
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

start();
