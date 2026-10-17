# Daemon socket protocol

The daemon owns one unix socket (`<agent dir>/llm-queue.sock`) and speaks newline-delimited
JSON. Every message is one JSON object on a single line.

## Requests

| op        | fields                                        | response                                                        |
| --------- | --------------------------------------------- | --------------------------------------------------------------- |
| `acquire` | `queueKey`, `sessionKey`, `role`, `group`     | `{"op":"granted","queueKey","sessionKey"}` — sent when granted   |
| `release` | `queueKey`, `sessionKey`                     | `{"op":"ok"}`                                                    |
| `cancel`  | `queueKey`, `sessionKey`                     | — (no reply) — drop a queued request the client aborted          |
| `role`    | `sessionKey`, `role`                         | — (no reply) — re-tier a live queued waiter (`/queue promote\|demote`) |
| `renew`   | — (heartbeat) or `queueKey`, `sessionKey` (activity beat) | `{"op":"ok"}` (heartbeat) / `{"op":"ok","queueKey","sessionKey","granted"}` (beat) |
| `status`  | —                                            | `{"op":"status","queues":[{queueKey,maxStreams,switches,holders,waiting}]}` |

`acquire` is the only request that may stay unanswered: while the queue's slots are taken the
request sits in the priority waiting list and the grant arrives when a slot frees. A client that
disconnects while queued is dropped from the queue (that is how Esc aborts feel immediate), and
a disconnect that held a slot releases it. A client can also send `cancel` to drop its own
queued request without closing the socket — this is how Esc cancels a request that is still
waiting for a slot.

`role` is `parent` (interactive) or `child` (subagent); `group` is an optional `PI_QUEUE_GROUP`
label carried through to `status`. The scheduler grants by tier — parent over child, FIFO within a
tier — with aging anti-starvation: the longest-waiting child jumps the queue once it has waited
`agingAfterMs`, or once `agingAfterSlots` parents have been granted in a row.

## Leases and session switches

A grant is a **lease** with a TTL (`leaseTtlMs`, default 30 s). A live client keeps its leases
alive with a bare `renew` heartbeat (the extension sends one every `leaseTtlMs / 3`, floor 1 s);
a session that dies without heartbeating has its lease reclaimed by the daemon and the next
waiter is granted.

The slot is held per session across a whole run. A session switch happens only on:

- **release** — the run settled (the extension releases 3 s after `agent_settled`, so a parent
  woken by its child keeps the slot), the session shut down, the request aborted, or the socket died;
- **idle quantum** (`idleQuantumMs`, default 120 s) — the holder sent no activity beat, i.e. made
  no model request, for that long. Long tool executions stay covered: every request beats the lease;
- **monopoly quantum** (`monopolyRequests` requests / `monopolyMs` seconds, default 12 / 180 s) —
  a busy session is preempted so a waiting session gets its turn; the count resets on the next grant.

Quanta are checked once a second and only evict when someone is waiting: an idle slot nobody
wants is not worth a context switch. A keyed `renew` (activity beat) refreshes the TTL, restarts
the idle quantum and counts one request toward the monopoly; re-`acquire`ing a slot the session
already holds counts the same way. A beat's reply is authoritative: `granted: false` means the
slot was switched away, and the client must re-queue before it streams again — a preempted run
never streams unqueued. A bare `renew` (no keys) heartbeats every lease of the connection.

shortcut: a tool execution longer than `idleQuantumMs` (default 120 s) with a session waiting
will lose the slot mid-run; raise `idleQuantumMs` for that queue if your tools run longer.

The daemon counts every **session switch** — every grant of a queued waiter, whether the slot was
released by its holder or evicted by a quantum — as the event that re-prefills the KV cache, and
reports the per-queue count as `switches` in `status`. `/queue` prints it with the slot holder and
the per-tier queue depth; the footer indicator reads the same status (a queued `acquire` stays
unanswered, so the client polls `status` every 2 s for its position).

## Queues and config

`queueKey` is a provider `baseUrl`. Queues are created on first use from
`<agent dir>/llm-queue.json`:

```json
{
  "defaults": { "maxStreams": 1, "policy": "priority-aging", "leaseTtlMs": 30000, "idleQuantumMs": 120000, "monopolyRequests": 12, "monopolyMs": 180000, "agingAfterSlots": 3, "agingAfterMs": 60000 },
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
`--models-json`); a baseUrl key wins when both name the same endpoint. The extension wraps
every provider named by a key plus every provider serving an endpoint named by a key, and
queues its requests under the model's baseUrl — or under its group's name, when a group names it.

Per-queue fields (all also valid in `defaults`): `maxStreams`, `policy`, `leaseTtlMs`,
`idleQuantumMs`, `monopolyRequests`, `monopolyMs`, `agingAfterSlots`, `agingAfterMs`,
`enabled`, `match`, `blockSubagentLaunch`. `queueTimeoutMs`, `failOpen` and
`daemonIdleExitMs` (#8) are documented in the schema and take effect in those tickets; schema
validation with clear errors is #9.

- **`blockSubagentLaunch`** — force the `subagent` tool's `async: true → false` on this queue so
  launches serialise. Defaults to `true` when `maxStreams == 1` (`false` otherwise); a disabled
  (passthrough) queue never blocks.

- **`enabled: false`** — passthrough: the queue grants every session immediately (`status`
  reports `maxStreams: null`), and the extension leaves the provider unwrapped. A disabled
  group does not re-key its members, so their own endpoint entries still apply.
- **`match`** — an explicit resource group, overriding baseUrl keying:
  - **merge**: members (baseUrls and/or provider names) share the group's slot pool. The
    extension keys a grouped model's requests by the group name; a raw socket client naming a
    member baseUrl is resolved to the first group whose `match` names it.
  - **split**: a group naming one provider of an endpoint shared by several providers gives
    that provider its own slot pool (`match: ["strata"]` beside `match: ["llama"]` on one
    server). Splitting needs provider identity, so it works through the extension's key, not
    through a raw baseUrl.

### Hot-reload

The daemon watches `llm-queue.json` (250 ms debounce, directory watch so atomic saves keep
working), re-applies queue policy to live queues, and rebuilds them through the reloaded
groups — holders and waiters move with their queue, so an endpoint never ends up with two
slot pools. A widened `maxStreams` grants waiting sessions at once. The extension watches the
same file and re-resolves its wrapped provider set (registering new providers, unregistering
removed ones). A half-written file keeps the last good config. No daemon or pi restart is
needed; `leaseTtlMs` on the client side (heartbeat period) is the one field that needs a pi
restart.

## Client contract

- The extension starts the daemon on demand: detached spawn, `<socket>.pid` lockfile, and
  reclaim when the socket does not answer.
- Leases are per session (`sessionKey` is the pi session UUID), not per process.
- Turn-based affinity: the extension acquires at `agent_start` and releases after the run
  settles, 3 s later (grace, so a parent woken by its child keeps the slot); an abort releases
  a held slot at once and cancels a still-queued request. Requests outside a run (compaction,
  classifier, nested calls) queue for themselves. Every request the run makes beats its lease,
  so the slot survives tool executions; the client heartbeats `renew` on a timer so a live
  session never loses the slot to the TTL.
- Reply matching: a client resolves `acquire` only on a `granted` reply naming its own
  `queueKey`/`sessionKey`; replies with no matching request (e.g. a `release` `ok`) are dropped,
  never buffered for a later request to consume. A request addressed at a group member gets its
  replies keyed by the key it asked with (its own `baseUrl`), never the group name — only
  `status` reports group keys.
- Fail-open: daemon unreachable → the request goes through unqueued. A client whose daemon dies
  mid-wait is released to its request.
- Recommended pi setting while queued requests queue up behind a long prefill:
  `httpIdleTimeoutMs: 900000`.

## Tests

`make test` drives this seam (`test/daemon.test.ts`, `test/lease-client.test.ts`,
`test/extension.test.ts`, `test/extension-wrapper.test.ts`) and never touches a real model
endpoint. `make e2e` (`node scripts/e2e-two-sessions.mjs`) is the human-run tracer bullet: two
real pi sessions against a stub endpoint, asserting zero request overlap.
