# Daemon socket protocol

The daemon is the scheduler. Everything about who holds a model slot, in what
order, and for how long is observable through this one seam — tests drive the
socket and assert external behaviour only.

- Transport: unix socket (`~/.pi/agent/llm-queue.sock`, `PI_QUEUE_SOCKET`).
- Framing: newline-delimited JSON, one message per line.
- Identity: a connection is a client; a `sessionKey` is a pi session. Leases are
  per session, so one process with several sessions holds several leases.

## Requests (client → daemon)

| type | fields | meaning |
|---|---|---|
| `ping` | `token` | liveness probe; answers `pong` with `pid`, `uptimeMs` |
| `acquire` | `token`, `queueKey`, `sessionKey`, `role`, `group?` | ask for a slot on `queueKey` |
| `cancel` | `token` | drop a queued acquire (abort) |
| `renew` | `token`, `leaseId` | extend lease TTL (heartbeat) |
| `release` | `token`, `leaseId` | give the slot back |
| `status` | `token` | machine-readable queue state |
| `reload` | `token` | re-read config (hot-reload) |

## Responses (daemon → client)

`{ "type": "resp", "token", "ok", "state", ... }` where `state` is one of:

- `queued` — `position`, `holders`: the request is waiting.
- `granted` — `leaseId`, `acquiredAt`, `expiresAt` (and `already: true` when the
  session already held this queue's slot: turn-based affinity).
- `renewed` — `expiresAt`.
- `released`, `cancelled`, `reloaded`, `pong`.
- `status` — `queues: [{ key, maxStreams, holders[], waiting{parent,child},
  waiters[], switches, grants }]`.

Unsolicited: `{ "type": "lease_expired", "leaseId", "queueKey", "reason" }`
(`reason` is `ttl` or `disconnect`).

## Scheduling semantics

- One queue per `queueKey` (the endpoint baseUrl), `maxStreams` slots.
- Parent tier is granted ahead of child tier; FIFO within a tier.
- A grant is retained by the session until release, lease TTL expiry, or the
  client's connection closes.
- `switches` counts grants that moved the slot to a different session;
  `grants` counts all grants.
- Fail-open is a client-side rule: no daemon, no answer, expired
  `queueTimeoutMs` → the request proceeds unqueued.

## Lockfile

`~/.pi/agent/llm-queue.lock` (`PI_QUEUE_LOCK`) holds the daemon pid. A client
reclaims it when the pid is dead; a daemon that finds a live pid exits.
