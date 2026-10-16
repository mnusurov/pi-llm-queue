# Daemon socket protocol

The daemon owns one unix socket (`<agent dir>/llm-queue.sock`) and speaks newline-delimited
JSON. Every message is one JSON object on a single line.

## Requests

| op        | fields                                        | response                                                        |
| --------- | --------------------------------------------- | --------------------------------------------------------------- |
| `acquire` | `queueKey`, `sessionKey`, `role`, `group?`   | `{"op":"granted","queueKey","sessionKey"}` — sent when granted   |
| `release` | `queueKey`, `sessionKey`                     | `{"op":"ok"}`                                                    |
| `renew`   | `queueKey`, `sessionKey`                     | `{"op":"ok"}`                                                    |
| `status`  | —                                            | `{"op":"status","queues":[{queueKey,maxStreams,holders,waiting}]}` |

`acquire` is the only request that may stay unanswered: while the queue's slots are taken the
request sits in the FIFO waiting list and the grant arrives when a slot frees. A client that
disconnects while queued is dropped from the queue (that is how Esc aborts feel immediate), and
a disconnect that held a slot releases it.

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

Keys may be a baseUrl (daemon-side policy) or a provider name (the extension wraps that
provider, declared in `models.json`, and its requests are queued under the model's baseUrl).

## Client contract

- The extension starts the daemon on demand: detached spawn, `<socket>.pid` lockfile, and
  reclaim when the socket does not answer.
- Leases are per session (`sessionKey` is the pi session UUID), not per process.
- Fail-open: daemon unreachable → the request goes through unqueued. A client whose daemon dies
  mid-wait is released to its request.
- Recommended pi setting while queued requests queue up behind a long prefill:
  `httpIdleTimeoutMs: 900000`.

## Tests

`make test` drives this seam only (`test/daemon.test.ts`, `test/lease-client.test.ts`) and never
touches a real model endpoint. `node scripts/e2e-two-sessions.mjs` is the human-run tracer
bullet: two real pi sessions against a stub endpoint, asserting zero request overlap.
