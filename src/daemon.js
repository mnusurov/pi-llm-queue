#!/usr/bin/env node
// pi-llm-queue daemon: serialises model access per endpoint over a unix socket.
// Protocol: newline-delimited JSON. See docs/daemon-protocol.md.
import { createConnection, createServer } from "node:net";
import { existsSync, readFileSync, unlinkSync } from "node:fs";

const args = process.argv.slice(2);
const arg = (name) => args[args.indexOf(name) + 1];
const socketPath = arg("--socket");
const configPath = arg("--config");
if (!socketPath) {
	console.error("usage: daemon.js --socket <path> [--config <path>]");
	process.exit(1);
}

const config = configPath ? JSON.parse(readFileSync(configPath, "utf8")) : {};
const defaults = config.defaults ?? {};

function send(conn, msg) {
	conn.write(`${JSON.stringify(msg)}\n`);
}

const queues = new Map();
function queueFor(key) {
	if (!queues.has(key)) {
		const q = config.queues?.[key] ?? {};
		queues.set(key, {
			queueKey: key,
			maxStreams: q.maxStreams ?? defaults.maxStreams ?? 1,
			holders: [],
			waiting: [],
		});
	}
	return queues.get(key);
}

function grantNext(queue) {
	while (queue.waiting.length && queue.holders.length < queue.maxStreams) {
		const waiter = queue.waiting.shift();
		queue.holders.push(waiter.sessionKey);
		remove(waiter.conn.__queued, waiter);
		waiter.conn.__leases.push({ queueKey: queue.queueKey, sessionKey: waiter.sessionKey });
		send(waiter.conn, { op: "granted", queueKey: queue.queueKey, sessionKey: waiter.sessionKey });
	}
}

function remove(list, item) {
	const at = list.indexOf(item);
	if (at >= 0) list.splice(at, 1);
}

function release(queue, sessionKey) {
	const at = queue.holders.indexOf(sessionKey);
	if (at >= 0) queue.holders.splice(at, 1);
	grantNext(queue);
}

function handle(msg, conn) {
	const queue = queueFor(msg.queueKey);
	switch (msg.op) {
		case "acquire": {
			if (queue.holders.includes(msg.sessionKey)) {
				send(conn, { op: "granted", queueKey: queue.queueKey, sessionKey: msg.sessionKey });
				return;
			}
			if (queue.holders.length < queue.maxStreams) {
				queue.holders.push(msg.sessionKey);
				conn.__leases.push({ queueKey: queue.queueKey, sessionKey: msg.sessionKey });
				send(conn, { op: "granted", queueKey: queue.queueKey, sessionKey: msg.sessionKey });
				return;
			}
			const waiter = { sessionKey: msg.sessionKey, role: msg.role ?? "parent", conn, queue };
			queue.waiting.push(waiter);
			conn.__queued.push(waiter);
			return;
		}
		case "release":
			release(queue, msg.sessionKey);
			send(conn, { op: "ok" });
			return;
		case "renew":
			send(conn, { op: "ok" });
			return;
		case "status":
			send(conn, {
				op: "status",
				queues: [...queues.values()].map((q) => ({
					queueKey: q.queueKey,
					maxStreams: q.maxStreams,
					holders: [...q.holders],
					waiting: q.waiting.map((w) => ({ sessionKey: w.sessionKey, role: w.role })),
				})),
			});
			return;
		default:
			send(conn, { op: "error", reason: `unknown op: ${msg.op}` });
	}
}

const server = createServer((conn) => {
	conn.__leases = [];
	conn.__queued = [];
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
	for (const lease of conn.__leases ?? []) release(queueFor(lease.queueKey), lease.sessionKey);
	for (const w of conn.__queued ?? []) remove(w.queue.waiting, w);
}

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
		unlinkSync(socketPath);
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
