# Daemon socket protocol

The daemon owns one unix socket (`<agent dir>/llm-queue.sock`) and speaks newline-delimited
JSON. Every message is one JSON object on a single line.

## Requests

| op        | fields                                        | response                                                        |
| --------- | --------------------------------------------- | --------------------------------------------------------------- |
| `acquire` | `queueKey`*, `sessionKey`, `role`, `group`, `activity`     | `{"op":"granted","queueKey","sessionKey"}` — sent when granted   |
| `release` | `queueKey`*, `sessionKey`                     | `{"op":"ok"}`                                                    |
| `cancel`  | `queueKey`*, `sessionKey`                     | — (no reply) — drop a queued request the client aborted          |
| `role`    | `sessionKey`, `role`                         | — (no reply) — re-tier a live queued waiter (`/queue promote\|demote`) |
| `renew`   | — (heartbeat) or `queueKey`*, `sessionKey` (activity beat) | `{"op":"ok"}` (heartbeat) / `{"op":"ok","queueKey","sessionKey","granted"}` (beat) |
| `status`  | —                                            | `{"op":"status","queues":[{queueKey,maxStreams,switches,holders,waiting}]}` |

\* `acquire`, `release` and `cancel` require `queueKey`: a keyless one would create an unnamed
queue that silently holds a slot. The reply is `{"op":"error","reason":"... requires queueKey"}`.

`acquire` is the only request that may stay unanswered: while the queue's slots are taken the
request sits in the priority waiting list and the grant arrives when a slot frees. A client that
disconnects while queued is dropped from the queue (that is how Esc aborts feel immediate), and
a disconnect that held a slot releases it. A client can also send `cancel` to drop its own
queued request without closing the socket — this is how Esc cancels a request that is still
waiting for a slot.

`role` is `parent` (interactive) or `child` (subagent); `group` is an optional `PI_QUEUE_GROUP`
label carried through to `status`. `activity` is an optional boolean capability flag: a client
that sends `activity: true` in `acquire` opts into idle-yield tracking (the daemon records
`lastActivityAt` on its lease and may yield it under contention). A client that omits it (old
client) is never idle-yielded.

The scheduler grants by tier — parent over child, FIFO within a
tier — with aging anti-starvation: the longest-waiting child jumps the queue once it has waited
`agingAfterMs`, or once `agingAfterSlots` parents have been granted in a row.

## Leases and session switches

A grant is a **lease** with a TTL (`leaseTtlMs`, default 30 s, minimum 1000 ms). A live client
keeps its leases alive with a bare `renew` heartbeat (the extension sends one every
`leaseTtlMs / 3`, floor 250 ms, always under the TTL so a sweep never lands between two beats);
a session that dies without heartbeating has its lease reclaimed by the daemon and the next
waiter is granted.

The slot is held per session across a whole run, and **a live holder is never preempted**
(ADR 0002). A session switch happens only on:

- **release** — the run settled (the extension releases 3 s after `agent_settled`, so a parent
  woken by its child keeps the slot), the session shut down, the request aborted, or the socket died;
- **TTL expiry** — the client stopped heartbeating, i.e. it died: a session killed mid-stream, a
  process that vanished, a heartbeat starved by a tool that blocks the event loop;
- **idle yield** (ADR 0003) — an activity-flagged holder sent no keyed beat for `idleYieldMs`
  and a waiter is queued: the daemon releases the slot and grants the next waiter. A lone holder
  is never disturbed; an unflagged (old-client) lease is never yielded.

A keyed `renew` (activity beat) refreshes the TTL, and its reply is authoritative: `granted: false`
means the slot is gone (released, disconnected, TTL-expired) and the client must re-queue before it
streams again — a run that lost its slot never streams unqueued. A bare `renew` (no keys)
heartbeats every lease of the connection.

The daemon sweeps every `--sweep-ms` (default 1000 ms) and evicts every lease whose TTL lapsed,
whether or not anyone is waiting: an expired lease is a dead client, and `/queue` must never report
it as a holder.

shortcut: a child that stalls while holding the slot keeps it until it is stopped, its socket
closes, its TTL lapses (a dead client), or it is idle-yielded (ADR 0003, activity-flagged
leases only) — the queue never takes the slot back from an *active* holder. That is the
"hold for the whole run" contract; `queueTimeoutMs` remains the parent-side warning.

shortcut: a stream longer than `leaseTtlMs` keeps heartbeating, so its lease never expires and a
waiter waits for the whole stream. That is the serialisation promise, not a bug — raise
`httpIdleTimeoutMs` (recommended 900000) so the waiter's client stays patient.

The daemon counts every **session switch** — every grant of a queued waiter — as the event that
re-prefills the KV cache, and reports the per-queue count as `switches` in `status`. `/queue`
prints it with the slot holder and the per-tier queue depth; the footer indicator reads the same
status (a queued `acquire` stays unanswered, so the client polls `status` every 2 s for its position).

## Queues and config

`queueKey` is a provider `baseUrl`. Queues are created on first use from
`<agent dir>/llm-queue.json`:

```json
{
  "defaults": { "maxStreams": 1, "policy": "priority-aging", "leaseTtlMs": 30000, "agingAfterSlots": 3, "agingAfterMs": 60000, "failOpen": true, "daemonIdleExitMs": 60000 },
  "queues": {
    "http://192.168.1.100:8001/v1": { "maxStreams": 1 },
    "zen": { "maxStreams": 3, "policy": "fifo" },
    "free-api": { "enabled": false },
    "one-gpu": { "match": ["http://192.168.1.100:8001/v1", "http://192.168.1.100:8002/v1"], "maxStreams": 1 },
    "serial-llama": { "match": ["llamacpp"], "maxStreams": 1 }
  }
}
```

Keys may be a baseUrl (the endpoint itself) or a provider name. The daemon resolves a
provider-name key to that provider's `baseUrl` by reading `models.json` (passed as
`--models-json`, optional: a missing catalog is an empty one, never a startup crash); a baseUrl
key wins when both name the same endpoint. The extension wraps
every provider named by a key plus every provider serving an endpoint named by a key, and
queues its requests under the model's baseUrl — or under its group's name, when a group names it.
The group/entry resolution rule is one implementation (`groupKeyFor`, `queueEntryFor` in
`src/config.js`) shared by the extension and the daemon.

Per-queue fields: `maxStreams`, `policy`, `leaseTtlMs` (minimum 1000), `agingAfterSlots`,
`agingAfterMs`, `idleYieldMs` (all also valid in
`defaults`), plus `enabled` and `match` (queue entries only — `defaults` never reads them).
`failOpen` (default `true`) and `queueTimeoutMs`
are client-side (`defaults` only); `daemonIdleExitMs` (default `60000`) is daemon-side
(`defaults` only). An optional top-level `schemaVersion` (current: 1) must be a positive
integer; a newer version is rejected with an "upgrade pi-llm-queue" error. Validation
(`src/config.js`) reports every violation as a path-named error (`queues.zen.maxStreams must
be a positive integer (got 0)`): the extension notifies them through `ui.notify` at session
start and on a rejected reload; the daemon prints them to stderr — discarded when the extension
spawns it detached, visible when the daemon is run by hand — and serves with defaults at
startup, or keeps the last good config on hot-reload. A missing config
file is all defaults with no error; a half-written file (invalid JSON) is a silent no-op that
keeps the last good config.

- **`idleYieldMs`** — idle-yield threshold (default `30000`, `0` disables): an activity-flagged
  holder that sends no keyed beat for this long, with a waiter queued, is released and the next
  waiter is granted. A lone holder is never disturbed; an unflagged (old-client) lease is never
  yielded. Valid in `defaults` and per-queue.

- **`enabled: false`** — passthrough: the queue grants every session immediately (`status`
  reports `maxStreams: null`, printed as `passthrough` by `/queue`), and the extension leaves the provider unwrapped. A disabled
  group does not re-key its members, so their own endpoint entries still apply.
- **`match`** — an explicit resource group, overriding baseUrl keying:
  - **merge**: members (baseUrls and/or provider names) share the group's slot pool. The
    extension keys a grouped model's requests by the group name; a raw socket client naming a
    member baseUrl is resolved to the first group whose `match` names it.
  - **split**: a group naming one provider of an endpoint shared by several providers gives
    that provider its own slot pool (`match: ["strata"]` beside `match: ["llama"]` on one
    server). Splitting needs provider identity, so it works through the extension's key, not
    through a raw baseUrl.
  - **precedence**: a group naming the model's provider exactly owns it, ahead of a group naming
    only its endpoint — split beats merge, whatever the order in the config file.

### Hot-reload

The daemon watches `llm-queue.json` (250 ms debounce, directory watch so atomic saves keep
working), re-applies queue policy to live queues, and rebuilds them through the reloaded
groups — holders and waiters move with their queue, so an endpoint never ends up with two
slot pools. A widened `maxStreams` grants waiting sessions at once. The extension watches the
same file and re-resolves its wrapped provider set (registering new providers, unregistering
removed ones). A half-written file keeps the last good config. No daemon or pi restart is
needed; `leaseTtlMs` on the client side (heartbeat period), `failOpen` and `queueTimeoutMs` are
read once at connect and are the fields that need a pi restart.

## Client contract

- The extension starts the daemon on demand: one detached spawn per session start. The daemon
  resolves the race itself — a live socket makes the second daemon exit 0, a stale socket file is
  reclaimed — so the client needs no liveness probe and no lockfile.
- Leases are per session (`sessionKey` is the pi session UUID), not per process.
- Turn-based affinity: the extension acquires at `agent_start` and releases after the run
  settles, 3 s later (grace, so a parent woken by its child keeps the slot); an abort releases
  a held slot at once and cancels a still-queued request. Requests outside a run (compaction,
  classifier, nested calls) queue for themselves. Every request the run makes beats its lease, so
  the slot survives tool executions and a beat answered `granted: false` re-queues the run; the
  client heartbeats `renew` on a timer so a live session never
  loses the slot to the TTL.
- Reply matching: a client resolves `acquire` only on a `granted` reply naming its own
  `queueKey`/`sessionKey`; replies with no matching request (e.g. a `release` `ok`) are dropped,
  never buffered for a later request to consume. A request addressed at a group member gets its
  replies keyed by the key it asked with (its own `baseUrl`), never the group name — only
  `status` reports group keys.
- Socket framing: a peer that never sends a newline is disconnected past a 100 KB line buffer,
  and a message split inside a multi-byte character is decoded whole (streaming `TextDecoder`).
- Fail-open: daemon unreachable → the request goes through unqueued (`failOpen: true`, the
  default). A client whose daemon dies mid-wait is released to its request. With
  `failOpen: false` the request hard-errors instead (the queue becomes a single point of failure).
- Queue-wait warning: a parent `acquire` older than `queueTimeoutMs` is warned (visible
  `ui.notify`) but stays queued — passthrough is removed. With idle-yield a stuck queue
  self-heals; passing through behind a live streaming child would re-create the stampede. A
  child's wait is always unlimited — a subagent never passes through unqueued, which would
  defeat serialisation.
- Idle-exit: the daemon self-terminates `daemonIdleExitMs` (default 60 s) after the last client
  disconnects, unlinking its socket, so no orphan process lingers. A client that reconnects
  within the grace cancels the exit, so a quick relaunch keeps the warm daemon. A daemon that
  starts with no client arms the same timer, so a spawn whose client never arrives does not linger.
  Set `daemonIdleExitMs` high to keep the daemon resident.
- Recommended pi setting while queued requests queue up behind a long prefill:
  `httpIdleTimeoutMs: 900000`.

## Tests

`make test` drives this seam (`test/daemon.test.ts`, `test/lease-client.test.ts`,
`test/extension.test.ts`, `test/extension-wrapper.test.ts`) and never touches a real model
endpoint. `make e2e` (`node scripts/e2e-two-sessions.mjs`) is the human-run tracer bullet: two
real pi sessions against a stub endpoint, asserting zero request overlap.
