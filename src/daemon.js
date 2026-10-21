#!/usr/bin/env node
// pi-llm-queue daemon: serialises model access per endpoint over a unix socket.
// Protocol: newline-delimited JSON. See docs/daemon-protocol.md.
//
// This file is the socket adapter (ADR 0004): framing, the Counted-client count + idle-exit, the
// Sweep timer, the `status` view, the `requestedKey` echo, config resolution through config.js,
// queue addressing through addressing.js, and emitting the decisions the Scheduler returns. All
// scheduling decisions live in src/scheduler.js.
import { createConnection, createServer } from "node:net";
import { appendFileSync, existsSync, readFileSync, unlinkSync } from "node:fs";
import { loadValidConfig, resolveQueuePolicy } from "./config.js";
import { watchConfigFile } from "./config-watch.js";
import { addressEndpoint, readProviders } from "./addressing.js";
import { termsToWire } from "./lease-terms.js";
import { Scheduler } from "./scheduler.js";
import { readJsonLines } from "./socket-lines.js";

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
// extension spawns the daemon detached, so a crash would be silent). A present catalog that yields
// no providers is reported: provider-name queue keys and group members would silently resolve to
// nothing. `readProviders` swallows the read failure (its contract), so the reason is read back here.
let modelsJson = {};
if (modelsJsonPath && existsSync(modelsJsonPath)) {
	modelsJson = readProviders(modelsJsonPath);
	if (!Object.keys(modelsJson).length) {
		let reason = "no providers";
		try {
			JSON.parse(readFileSync(modelsJsonPath, "utf8"));
		} catch (error) {
			reason = error.message;
		}
		console.error(`pi-llm-queue daemon: models.json: ${reason} — provider-name queue keys resolve to nothing`);
	}
}

function send(conn, msg) {
	conn.write(`${JSON.stringify(msg)}\n`);
}

// One session as a client reads it: id, tier, group label. A Lease (Holder) and a Waiter carry the
// tier under the Scheduler's word (`tier`); the wire field is `role` (protocol:15). This is the one
// boundary that renames it, so `status` reports both lists through one view and cannot drift apart.
function sessionView(entry) {
	return { sessionKey: entry.sessionKey, role: entry.tier, group: entry.group };
}

// Event log (#47): with `logFile` set, the daemon appends one JSON line per queue event, so a
// finished run can be read back from one file — the extension spawns the daemon detached and its
// stderr is discarded. The path is read per event, so a hot-reload can turn the log on, off, or
// move it. The queue key logged is the queue that owns the slot: a group's name when the client
// addressed it by a member key. The role comes off the decision, never off the request: `status`
// reports the same field, so a reader can never see the log and the reply disagree about the tier a
// session holds. The wall-clock `ts` is stamped here (adapter-side), not in the Scheduler.
function logEvent(event, queueKey, sessionKey, role, reason) {
	const line = { event, queueKey, sessionKey, role, ts: new Date().toISOString() };
	if (reason) line.reason = reason;
	writeEvent(line);
}

// A hot-reload can lower a queue's maxStreams below its live holder count (merging two endpoint
// queues into a narrower group does exactly that). No-preemption means the daemon cannot take a
// slot from a live Holder, so it reports the transient over-capacity in the log instead of
// exceeding the new limit silently (#82). The counts show how far over the queue is.
function logOverCapacity(report) {
	writeEvent({
		event: "config",
		queueKey: report.queueKey,
		reason: "over-capacity",
		holders: report.holders,
		maxStreams: report.maxStreams,
		ts: new Date().toISOString(),
	});
}

// The event-log sink: with `logFile` set, one JSON line is appended per event. Best effort,
// swallowed: a bad path, a full disk or a log rotation race must never take the daemon down
// (it is spawned detached, so a crash is silent) nor reject the request that logged.
function writeEvent(line) {
	const path = defaults.logFile;
	if (!path) return;
	// appendFileSync, not a stream: one short line per event, on disk before the reply goes out, so
	// a reader reads the file the moment the reply lands and a crash loses no buffered line.
	try {
		appendFileSync(path, `${JSON.stringify(line)}\n`, "utf8");
	} catch {}
}

// Idle-exit (#60): the daemon self-terminates `daemonIdleExitMs` after the last counted client
// disconnects. The default is 0 — exit at once — so daemon lifetime is the union of the live pi
// processes' lifetimes and a code change needs no manual kill: the next pi socket op autostarts a
// fresh daemon. A `> 0` value is an opt-in relaunch grace (a session restarting keeps the warm
// daemon); a huge value keeps the daemon resident. A `status`-only connection is never counted, so
// an observer polling every 2 s cannot defeat the exit.
let countedClients = 0;
let idleTimer = null;
function scheduleIdleExit(ms = defaults.daemonIdleExitMs ?? 0) {
	if (!isFinite(ms)) return;
	clearTimeout(idleTimer);
	// setTimeout coerces a delay past 2^31-1 (~24.8 days) to 1 ms, which would make "set it high to
	// keep the daemon resident" exit at once — the exact inverse. Clamp to the largest real delay.
	idleTimer = setTimeout(() => {
		try {
			unlinkSync(socketPath);
		} catch {}
		process.exit(0);
	}, Math.min(ms, 2147483647));
	idleTimer.unref();
}

// A connection joins the client count at its first state-touching op and leaves it on close. The
// extension holds one persistent connection per pi process, so the OS closes it on pi exit and on
// pi crash alike: socket close is the "last pi is gone" signal, crash-safe by construction.
function countClient(conn) {
	if (conn.counted) return;
	conn.counted = true;
	countedClients++;
	if (idleTimer) {
		clearTimeout(idleTimer);
		idleTimer = null;
	}
}

// A queue key is an endpoint URL, a provider name, or a group name. A provider-name key applies to
// that provider's baseUrl; a baseUrl key wins when both name the same endpoint. The rule lives in
// src/addressing.js (ADR 0005), in its endpoint form: a socket client names an endpoint and no
// provider, so the provider-name branch is unreachable through it by construction.
function policyFor(key) {
	return addressEndpoint(key, config.queues ?? {}, modelsJson).entry ?? {};
}

// Explicit resource groups: queues.<name>.match lists baseUrls and/or provider names that
// share one queue, overriding baseUrl keying so endpoints can be merged or re-slotted.
//
// This is also the rule the Scheduler asks where a Queue lives after a config reload: it reads the
// live `config`, so a hot-reload re-resolves through the new values, and it never sees Scheduler
// state (ADR 0005, #103).
function groupFor(key) {
	return addressEndpoint(key, config.queues ?? {}, modelsJson).queueKey;
}

// The Scheduler resolves no config: the adapter resolves each queue's policy through config.js and
// addressing.js and hands the Scheduler plain resolved values. The cascade and the Passthrough rule
// live in resolveQueuePolicy (#98); this adapter only maps config vocabulary to the Scheduler's
// (`ttlMs`). `enabled: false` is passthrough: maxStreams Infinity, so every session is granted
// immediately (status reports maxStreams null via JSON). The closure reads the live `config` /
// `defaults`, so a hot-reload re-resolves through the new values.
function resolvePolicy(key) {
	const p = resolveQueuePolicy(policyFor(key), defaults);
	return {
		maxStreams: p.maxStreams,
		ttlMs: p.leaseTtlMs,
		agingAfterSlots: p.agingAfterSlots,
		agingAfterMs: p.agingAfterMs,
		policy: p.policy,
		idleYieldMs: p.idleYieldMs,
	};
}

const scheduler = new Scheduler(resolvePolicy, groupFor);

// The adapter maps a connId (the Scheduler's connection identity) back to the live socket so a
// `grant` decision can send its reply. The Scheduler never sees a socket object.
let connSeq = 0;
const connById = new Map();

// Emit the decisions the Scheduler returned, in order: each maps to a log line and, for a grant, a
// `granted` reply. The reply echoes the key the client asked with (the `requestedKey` echo is
// adapter-side; the Scheduler only knows the resolved group key).
//
// Two passes, on purpose. A handover is two decisions — the grant and the switch it causes — and
// the reply belongs to both. One pass would put the `granted` reply out before the `switch` line
// is written, and the peer process can wake up on another core and read the file in between: the
// reader would see a grant with no switch. Logging the whole batch first keeps the promise the
// log sink makes — every event a reply caused is on disk before that reply goes out (#47).
function emit(decisions) {
	const replies = [];
	for (const d of decisions) {
		switch (d.type) {
			case "grant": {
				logEvent("grant", d.queueKey, d.sessionKey, d.tier);
				const conn = connById.get(d.connId);
				// Echo the key the request asked with (the per-request `requestedKey` the Scheduler
				// carries on the decision), falling back to the resolved queue key for a direct grant.
				replies.push([conn, grantedReply(d.requestedKey ?? d.queueKey, d.sessionKey, d.terms)]);
				break;
			}
			case "switch":
				logEvent("switch", d.queueKey, d.sessionKey, d.tier);
				break;
			case "queued":
				logEvent("queued", d.queueKey, d.sessionKey, d.tier);
				break;
			case "release":
				logEvent("release", d.queueKey, d.sessionKey, d.tier, d.reason);
				break;
			case "evict":
				logEvent("evict", d.queueKey, d.sessionKey, d.tier, d.reason);
				break;
		}
	}
	for (const [conn, msg] of replies) {
		if (conn) send(conn, msg);
	}
}

// The granted reply: the grant itself, plus the Lease's terms (#105). The client refreshes the
// Lease on the terms the daemon granted it under, never on its own copy of the config. The wire
// fields are the terms module's encoding (ADR 0009): the names and their meaning do not change.
function grantedReply(queueKey, sessionKey, terms) {
	return { op: "granted", queueKey, sessionKey, ...termsToWire(terms) };
}

function handle(msg, conn) {
	// One clock stamp per op: every expires/lastActivityAt/enqueuedAt this op writes uses this single
	// value, so no timestamp inside one op floats relative to another (spec #65 phase A).
	const now = Date.now();
	// A keyless acquire would create an unnamed queue that silently holds a slot.
	if ((msg.op === "acquire" || msg.op === "release" || msg.op === "cancel") && !msg.queueKey) {
		send(conn, { op: "error", reason: `${msg.op} requires queueKey` });
		return;
	}
	// Lifetime (#60): the ops that can change queue state make this connection a counted client.
	// `status` (and `release`, which can only free what an acquire already reserved) changes nothing
	// an Observer should be keeping alive, so Observers stay invisible to the daemon's lifetime.
	if (msg.op === "acquire" || msg.op === "renew" || msg.op === "role" || msg.op === "cancel") countClient(conn);
	// Every keyed op is resolved through groups first: a group's members all address one queue.
	// Replies always echo the key the client asked with — clients match replies on their own key.
	const requestedKey = msg.queueKey;
	if (msg.queueKey) msg.queueKey = groupFor(msg.queueKey);
	switch (msg.op) {
		case "acquire": {
			const result = scheduler.acquire(conn.connId, msg.sessionKey, msg.role, msg.group, msg.activity, msg.queueKey, requestedKey, now);
			emit(result.decisions);
			// A re-acquire of a held slot is a TTL refresh: `granted` with no Grant decision (the Grace
			// window logs one grant and no switch). The direct-grant path's reply rides on its own
			// `grant` decision, so reply here only for a refresh, keyed by the current op's requestedKey.
			if (result.refreshed) {
				send(conn, grantedReply(requestedKey, msg.sessionKey, result.terms));
			}
			return;
		}
		case "release":
			emit(scheduler.release(conn.connId, msg.sessionKey, msg.queueKey, now));
			send(conn, { op: "ok" });
			return;
		case "cancel":
			emit(scheduler.cancel(conn.connId, msg.sessionKey, msg.queueKey, now));
			return;
		// Bare renew: heartbeat, keeps every lease of this connection alive.
		// Keyed renew: an activity beat — the holder is using the slot, so its TTL refreshes. The
		// reply is authoritative: granted:false tells the client the slot is gone (a TTL lapse, a
		// release, a disconnect) and it must re-queue before it streams.
		case "renew": {
			if (msg.sessionKey && msg.queueKey) {
				const { granted } = scheduler.beat(conn.connId, msg.sessionKey, msg.queueKey, now);
				send(conn, { op: "ok", queueKey: requestedKey, sessionKey: msg.sessionKey, granted });
				return;
			}
			emit(scheduler.heartbeat(conn.connId, now));
			send(conn, { op: "ok" });
			return;
		}
		case "role":
			// /queue promote|demote: re-tier a live queued waiter in place (no reply, like cancel).
			scheduler.retier(conn.connId, msg.sessionKey, msg.role);
			return;
		case "status":
			send(conn, {
				op: "status",
				queues: scheduler.snapshot().map((q) => ({
					queueKey: q.queueKey,
					maxStreams: q.maxStreams,
					switches: q.switches,
					// Holders are reported like Waiters: a raw client reads tier and group from the
					// reply, so it needs no extension-side parsing to build a dashboard.
					holders: q.holders.map(sessionView),
					waiting: q.waiting.map(sessionView),
				})),
			});
			return;
		case "bye":
			// Shutdown intent: pi's session_shutdown announces "this connection is leaving" while
			// the socket is still open. Treating it as a disconnect (the same cleanup a close runs:
			// release leases, drop waiters, decrement the client count) arms the idle-exit at the
			// word, so a lingering pi process cannot keep the daemon resident past its last session.
			// No reply, like role/cancel; the connection is spent — the client destroys its socket.
			disconnect(conn);
			return;
		default:
			send(conn, { op: "error", reason: `unknown op: ${msg.op}` });
	}
}

const server = createServer((conn) => {
	conn.connId = ++connSeq;
	connById.set(conn.connId, conn);
	readJsonLines(
		conn,
		(msg) => {
			// Any JSON value parses: only an object can be a message. A `null` line reaching the op
			// handler used to throw on msg.op and exit the daemon, dropping every lease and waiter.
			if (typeof msg !== "object" || msg === null || Array.isArray(msg)) {
				send(conn, { op: "error", reason: "message must be a JSON object" });
				return;
			}
			handle(msg, conn);
		},
		() => send(conn, { op: "error", reason: "invalid json" }),
	);
	conn.on("close", () => disconnect(conn));
	conn.on("error", () => disconnect(conn));
});

// A disconnect is an abort: the Scheduler releases every lease it held and drops every queued
// request; the adapter then decrements the Counted-client count (the decrement is AFTER the
// disconnect, per the dual-ownership crux). close and error both fire: count the disconnect once.
function disconnect(conn) {
	if (conn.cleaned) return;
	conn.cleaned = true;
	const now = Date.now();
	emit(scheduler.disconnect(conn.connId, now));
	connById.delete(conn.connId);
	if (conn.counted && --countedClients <= 0) scheduleIdleExit();
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

// Rebuild the Queues from the reloaded config, carrying Holders and Waiters with them: adding a
// group at runtime must not leave the endpoint's old slot set running beside the group's.
// shortcut: deleting a group leaves its live queue keyed by the old group name until restart.
// The rebuild is the Scheduler's (ADR 0004, #103): the daemon asks Addressing where each Queue
// lives after the reload, the Scheduler carries the Holders, Waiters and switch counts there, and
// the adapter emits the decisions and logs the over-capacity report.
function rebuildQueues() {
	// One clock stamp per reload: every grant this rebuild makes, and every carried holder's
	// activity refresh, uses this single value.
	const now = Date.now();
	const { decisions, overCapacity } = scheduler.rebuildQueues(now);
	for (const report of overCapacity) logOverCapacity(report);
	emit(decisions);
}

// Sweep timer: the adapter reads the wall clock and hands the Scheduler a stamp.
setInterval(() => emit(scheduler.sweepAt(Date.now())), sweepMs).unref();

// Hot-reload: a config edit re-applies queue policy to live queues and grants whoever the
// new capacity frees — no daemon restart, no pi restart. A half-written file keeps the last
// good config. The watch itself lives in src/config-watch.js: it watches the directory (an
// atomic-save editor replaces the file, and a file watcher goes dead with it), and it is re-armed
// when it fails, so a watch that died cannot leave the daemon on a stale config for good (#105).
if (configPath) {
	watchConfigFile(configPath, {
		onChange: () => {
			if (!existsSync(configPath)) return; // config deleted: keep the last good config
			const next = loadValidConfig(configPath);
			if (next.errors.length) {
				// A half-written file (invalid JSON) is a silent no-op: the last good config is kept.
				if (!next.invalidJson) for (const e of next.errors) console.error(`pi-llm-queue daemon: config reload rejected: ${e}`);
				return;
			}
			config = next.config;
			defaults = config.defaults ?? {};
			// The Scheduler's move re-resolves every live Queue's rules (which read the new config through
			// the resolvePolicy closure); one reload, one move, one policy resolution.
			rebuildQueues();
		},
		onError: (error) => console.error(`pi-llm-queue daemon: config watch failed: ${error.message} — re-arming`),
	});
}

start();
// Arm idle-exit at startup too: a daemon spawned by ensureDaemon whose client never reaches it
// (connect fails, fail-open) must not hold the socket forever with zero clients. The client
// retries its connect for ~3 s, so the startup arm gets a floor well above that; the first
// counted op clears it.
scheduleIdleExit(Math.max(defaults.daemonIdleExitMs ?? 0, 30000));
