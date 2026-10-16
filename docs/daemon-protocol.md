# Daemon socket protocol

The daemon owns one unix socket (`<agent dir>/llm-queue.sock`) and speaks newline-delimited
JSON. Every message is one JSON object on a single line.

## Requests

| op        | fields                                        | response                                                        |
| --------- | --------------------------------------------- | --------------------------------------------------------------- |
| `acquire` | `queueKey`, `sessionKey`, `role`              | `{"op":"granted","queueKey","sessionKey"}` — sent when granted   |
| `release` | `queueKey`, `sessionKey`                     | `{"op":"ok"}`                                                    |
| `cancel`  | `queueKey`, `sessionKey`                     | — (no reply) — drop a queued request the client aborted          |
| `renew`   | `queueKey`, `sessionKey`                     | `{"op":"ok"}` — keepalive; TTL expiry is #1 scope                |
| `status`  | —                                            | `{"op":"status","queues":[{queueKey,maxStreams,holders,waiting}]}` |

`acquire` is the only request that may stay unanswered: while the queue's slots are taken the
request sits in the FIFO waiting list and the grant arrives when a slot frees. A client that
disconnects while queued is dropped from the queue (that is how Esc aborts feel immediate), and
a disconnect that held a slot releases it. A client can also send `cancel` to drop its own
queued request without closing the socket — this is how Esc cancels a request that is still
waiting for a slot.

`role` is `parent` (interactive) or `child` (subagent). FIFO ordering is implemented today;
parent-over-child priority and aging are spec'd in #1 and the field is carried now so the
protocol does not change when they land.

## Queues and config

`queueKey` is a provider `baseUrl`. Queues are created on first use from
`<agent dir>/llm-queue.json`:

```json
{
  "defaults": { "maxStreams": 1 },
  "queues": {
    "http://192.168.1.100:8001/v1": { "maxStreams": 1 },
    "zen": { "maxStreams": 3 }
  }
}
```

Keys may be a baseUrl (the endpoint itself) or a provider name. The daemon resolves a
provider-name key to that provider's `baseUrl` by reading `models.json` (passed as
`--models-json`); a baseUrl key wins when both name the same endpoint. The extension wraps
every provider named by a key plus every provider serving an endpoint named by a key, and
queues its requests under the model's baseUrl.

## Client contract

- The extension starts the daemon on demand: detached spawn, `<socket>.pid` lockfile, and
  reclaim when the socket does not answer.
- Leases are per session (`sessionKey` is the pi session UUID), not per process.
- Turn-based affinity: the extension acquires at `agent_start` and releases after the run
  settles, 3 s later (grace, so a parent woken by its child keeps the slot); an abort releases
  a held slot at once and cancels a still-queued request. Requests outside a run (compaction,
  classifier, nested calls) queue for themselves.
- Reply matching: a client resolves `acquire` only on a `granted` reply naming its own
  `queueKey`/`sessionKey`; replies with no matching request (e.g. a `release` `ok`) are dropped,
  never buffered for a later request to consume.
- Fail-open: daemon unreachable → the request goes through unqueued. A client whose daemon dies
  mid-wait is released to its request.
- Recommended pi setting while queued requests queue up behind a long prefill:
  `httpIdleTimeoutMs: 900000`.

## Tests

`make test` drives this seam (`test/daemon.test.ts`, `test/lease-client.test.ts`,
`test/extension.test.ts`, `test/extension-wrapper.test.ts`) and never touches a real model
endpoint. `make e2e` (`node scripts/e2e-two-sessions.mjs`) is the human-run tracer bullet: two
real pi sessions against a stub endpoint, asserting zero request overlap.
