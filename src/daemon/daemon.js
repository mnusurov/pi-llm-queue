#!/usr/bin/env node
/**
 * pi-llm-queue daemon — plain Node, zero dependencies.
 *
 * Serialises model access per queue (keyed by endpoint baseUrl) over a unix
 * socket using newline-delimited JSON. See docs/protocol.md.
 *
 * Tracer-bullet scope: single queue per key, `maxStreams` slots, FIFO grants
 * with parent tier ahead of child tier, lease TTL reclaim, disconnect
 * cleanup, session-switch counting.
 */
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";

const SOCKET_PATH = process.env.PI_QUEUE_SOCKET || path.join(os.homedir(), ".pi", "agent", "llm-queue.sock");
const LOCK_PATH = process.env.PI_QUEUE_LOCK || path.join(os.homedir(), ".pi", "agent", "llm-queue.lock");
const CONFIG_PATH = process.env.PI_QUEUE_CONFIG || path.join(os.homedir(), ".pi", "agent", "llm-queue.json");

const DEFAULTS = {
	maxStreams: 1,
	leaseTtlMs: 30000,
	policy: "priority-aging",
	agingAfterSlots: 3,
	agingAfterMs: 60000,
};

const startedAt = Date.now();
let config = loadConfig();
let leaseSeq = 0;
let connSeq = 0;

/** @typedef {{type:string, token?:string, [k:string]:any}} Msg */

function loadConfig() {
	try {
		const raw = JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8"));
		return {
			defaults: { ...DEFAULTS, ...(raw.defaults || {}) },
			queues: raw.queues && typeof raw.queues === "object" ? raw.queues : {},
		};
	} catch {
		return { defaults: { ...DEFAULTS }, queues: {} };
	}
}

function normalizeKey(key) {
	return typeof key === "string" ? key.trim().replace(/\/+$/, "") : "";
}

function queueConfigFor(key) {
	const entry = config.queues[key] || {};
	const num = (value, fallback) => {
		const parsed = Number(value);
		return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
	};
	const maxStreams = Math.max(1, Math.floor(num(entry.maxStreams, config.defaults.maxStreams)));
	return {
		maxStreams,
		leaseTtlMs: Math.floor(num(entry.leaseTtlMs, config.defaults.leaseTtlMs)),
		policy: entry.policy || config.defaults.policy,
		agingAfterSlots: Math.floor(num(entry.agingAfterSlots, config.defaults.agingAfterSlots)),
		agingAfterMs: Math.floor(num(entry.agingAfterMs, config.defaults.agingAfterMs)),
	};
}

/** @type {Map<string, {key:string, holders:Map<string,any>, waiters:any[], switches:number, grants:number, lastGrantedSession:string|null}>} */
const queues = new Map();

function getQueue(key) {
	let queue = queues.get(key);
	if (!queue) {
		queue = { key, holders: new Map(), waiters: [], switches: 0, grants: 0, lastGrantedSession: null };
		queues.set(key, queue);
	}
	return queue;
}

function holderList(queue) {
	return [...queue.holders.values()].map((holder) => ({
		sessionKey: holder.sessionKey,
		role: holder.role,
		leaseId: holder.leaseId,
		acquiredAt: holder.acquiredAt,
		expiresAt: holder.expiresAt,
	}));
}

function grantNext(queue) {
	const cfg = queueConfigFor(queue.key);
	while (queue.holders.size < cfg.maxStreams && queue.waiters.length > 0) {
		// Parent tier ahead of child tier; FIFO within a tier.
		let index = queue.waiters.findIndex((waiter) => waiter.role !== "child");
		if (index < 0) index = 0;
		const [waiter] = queue.waiters.splice(index, 1);
		const now = Date.now();
		const leaseId = `lease-${++leaseSeq}`;
		queue.holders.set(leaseId, {
			leaseId,
			queueKey: queue.key,
			sessionKey: waiter.sessionKey,
			role: waiter.role,
			connId: waiter.connId,
			acquiredAt: now,
			expiresAt: now + cfg.leaseTtlMs,
		});
		queue.grants += 1;
		if (queue.lastGrantedSession && queue.lastGrantedSession !== waiter.sessionKey) queue.switches += 1;
		queue.lastGrantedSession = waiter.sessionKey;
		const conn = conns.get(waiter.connId);
		if (!conn) continue;
		conn.tokens.set(waiter.token, { queueKey: queue.key, leaseId });
		send(conn, {
			type: "resp",
			token: waiter.token,
			ok: true,
			state: "granted",
			leaseId,
			queueKey: queue.key,
			acquiredAt: now,
			expiresAt: now + cfg.leaseTtlMs,
		});
	}
}

function releaseLease(queue, leaseId, reason) {
	const holder = queue.holders.get(leaseId);
	if (!holder) return false;
	queue.holders.delete(leaseId);
	const conn = conns.get(holder.connId);
	if (conn) send(conn, { type: "lease_expired", leaseId, queueKey: queue.key, reason });
	grantNext(queue);
	return true;
}

/** @type {Map<number, {id:number, sock:net.Socket, buf:string, tokens:Map<string,any>}>} */
const conns = new Map();

function send(conn, msg) {
	if (conn.sock.destroyed) return;
	conn.sock.write(`${JSON.stringify(msg)}\n`);
}

function statusPayload() {
	return [...queues.values()].map((queue) => {
		const cfg = queueConfigFor(queue.key);
		return {
			key: queue.key,
			maxStreams: cfg.maxStreams,
			holders: holderList(queue),
			waiting: {
				parent: queue.waiters.filter((w) => w.role !== "child").length,
				child: queue.waiters.filter((w) => w.role === "child").length,
			},
			waiters: queue.waiters.map((w) => ({
				sessionKey: w.sessionKey,
				role: w.role,
				waitedMs: Date.now() - w.enqueuedAt,
			})),
			switches: queue.switches,
			grants: queue.grants,
		};
	});
}

function handle(conn, msg) {
	switch (msg.type) {
		case "ping":
			return send(conn, { type: "resp", token: msg.token, ok: true, state: "pong", pid: process.pid, uptimeMs: Date.now() - startedAt });
		case "acquire": {
			const key = normalizeKey(msg.queueKey);
			if (!key) return send(conn, { type: "resp", token: msg.token, ok: false, error: "queueKey is required" });
			const queue = getQueue(key);
			const sessionKey = typeof msg.sessionKey === "string" && msg.sessionKey ? msg.sessionKey : `conn-${conn.id}`;
			const role = msg.role === "child" ? "child" : "parent";
			const cfg = queueConfigFor(key);
			for (const holder of queue.holders.values()) {
				if (holder.connId === conn.id && holder.sessionKey === sessionKey) {
					holder.expiresAt = Date.now() + cfg.leaseTtlMs;
					conn.tokens.set(msg.token, { queueKey: key, leaseId: holder.leaseId });
					return send(conn, {
						type: "resp",
						token: msg.token,
						ok: true,
						state: "granted",
						leaseId: holder.leaseId,
						queueKey: key,
						acquiredAt: holder.acquiredAt,
						expiresAt: holder.expiresAt,
						already: true,
					});
				}
			}
			const pending = queue.waiters.find((w) => w.connId === conn.id && w.sessionKey === sessionKey);
			if (pending) {
				conn.tokens.set(msg.token, { queueKey: key, pending: true });
				return send(conn, {
					type: "resp",
					token: msg.token,
					ok: true,
					state: "queued",
					position: queue.waiters.indexOf(pending),
					holders: holderList(queue),
				});
			}
			const waiter = { token: msg.token, connId: conn.id, sessionKey, role, group: msg.group, enqueuedAt: Date.now() };
			queue.waiters.push(waiter);
			conn.tokens.set(msg.token, { queueKey: key, pending: true });
			send(conn, { type: "resp", token: msg.token, ok: true, state: "queued", position: queue.waiters.length - 1, holders: holderList(queue) });
			return grantNext(queue);
		}
		case "cancel": {
			for (const queue of queues.values()) {
				const index = queue.waiters.findIndex((w) => w.connId === conn.id && w.token === msg.token);
				if (index >= 0) queue.waiters.splice(index, 1);
			}
			conn.tokens.delete(msg.token);
			return send(conn, { type: "resp", token: msg.token, ok: true, state: "cancelled" });
		}
		case "renew": {
			for (const queue of queues.values()) {
				const holder = queue.holders.get(msg.leaseId);
				if (!holder) continue;
				if (holder.connId !== conn.id) return send(conn, { type: "resp", token: msg.token, ok: false, error: "lease belongs to another client" });
				holder.expiresAt = Date.now() + queueConfigFor(queue.key).leaseTtlMs;
				return send(conn, { type: "resp", token: msg.token, ok: true, state: "renewed", leaseId: msg.leaseId, expiresAt: holder.expiresAt });
			}
			return send(conn, { type: "resp", token: msg.token, ok: false, error: "unknown lease" });
		}
		case "release": {
			for (const queue of queues.values()) {
				const holder = queue.holders.get(msg.leaseId);
				if (!holder) continue;
				if (holder.connId !== conn.id) return send(conn, { type: "resp", token: msg.token, ok: false, error: "lease belongs to another client" });
				queue.holders.delete(msg.leaseId);
				grantNext(queue);
				return send(conn, { type: "resp", token: msg.token, ok: true, state: "released", leaseId: msg.leaseId });
			}
			return send(conn, { type: "resp", token: msg.token, ok: false, error: "unknown lease" });
		}
		case "status":
			return send(conn, { type: "resp", token: msg.token, ok: true, state: "status", queues: statusPayload() });
		case "reload":
			config = loadConfig();
			for (const queue of [...queues.values()]) grantNext(queue);
			return send(conn, { type: "resp", token: msg.token, ok: true, state: "reloaded" });
		default:
			return send(conn, { type: "resp", token: msg.token, ok: false, error: `unknown message type: ${msg.type}` });
	}
}

function onConnection(sock) {
	const conn = { id: ++connSeq, sock, buf: "", tokens: new Map() };
	conns.set(conn.id, conn);
	sock.on("data", (chunk) => {
		conn.buf += chunk.toString("utf8");
		let newline;
		while ((newline = conn.buf.indexOf("\n")) >= 0) {
			const line = conn.buf.slice(0, newline).trim();
			conn.buf = conn.buf.slice(newline + 1);
			if (!line) continue;
			let msg;
			try {
				msg = JSON.parse(line);
			} catch {
				send(conn, { type: "resp", ok: false, error: "invalid json" });
				continue;
			}
			handle(conn, msg);
		}
	});
	sock.on("close", () => {
		conns.delete(conn.id);
		for (const queue of queues.values()) {
			queue.waiters = queue.waiters.filter((w) => w.connId !== conn.id);
			for (const [leaseId, holder] of [...queue.holders]) {
				if (holder.connId === conn.id) queue.holders.delete(leaseId);
			}
			grantNext(queue);
		}
	});
	sock.on("error", () => sock.destroy());
}

function pidAlive(pid) {
	if (!Number.isInteger(pid) || pid <= 0) return false;
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		return error && error.code === "EPERM";
	}
}

function claimLockfile() {
	try {
		const pid = Number(fs.readFileSync(LOCK_PATH, "utf8").trim());
		if (pidAlive(pid)) return false;
		fs.unlinkSync(LOCK_PATH);
	} catch {
		/* no lockfile */
	}
	fs.writeFileSync(LOCK_PATH, `${process.pid}\n`, "utf8");
	return true;
}

function canConnect(socketPath) {
	return new Promise((resolve) => {
		const sock = net.connect(socketPath);
		sock.once("connect", () => {
			sock.destroy();
			resolve(true);
		});
		sock.once("error", () => resolve(false));
	});
}

async function main() {
	if (await canConnect(SOCKET_PATH)) {
		// A live daemon already owns the socket; do not start a second one.
		process.exit(0);
	}
	if (!claimLockfile()) process.exit(0);
	try {
		fs.mkdirSync(path.dirname(SOCKET_PATH), { recursive: true });
		if (fs.existsSync(SOCKET_PATH)) fs.unlinkSync(SOCKET_PATH);
	} catch {
		/* best effort */
	}
	const server = net.createServer(onConnection);
	server.listen(SOCKET_PATH);
	const sweep = setInterval(() => {
		const now = Date.now();
		for (const queue of queues.values()) {
			for (const [leaseId, holder] of [...queue.holders]) {
				if (holder.expiresAt <= now) releaseLease(queue, leaseId, "ttl");
			}
		}
	}, 1000);
	const cleanup = () => {
		clearInterval(sweep);
		server.close();
		try {
			if (fs.existsSync(LOCK_PATH) && Number(fs.readFileSync(LOCK_PATH, "utf8").trim()) === process.pid) fs.unlinkSync(LOCK_PATH);
		} catch {
			/* best effort */
		}
		try {
			if (fs.existsSync(SOCKET_PATH)) fs.unlinkSync(SOCKET_PATH);
		} catch {
			/* best effort */
		}
		process.exit(0);
	};
	process.on("SIGINT", cleanup);
	process.on("SIGTERM", cleanup);
}

main();
