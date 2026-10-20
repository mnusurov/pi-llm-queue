# Daemon socket protocol

The daemon owns one unix socket (`<agent dir>/llm-queue.sock`) and speaks newline-delimited
JSON. Every message is one JSON object on a single line.

## Requests

| op        | fields                                        | response                                                        |
| --------- | --------------------------------------------- | --------------------------------------------------------------- |
| `acquire` | `queueKey`*, `sessionKey`, `role`, `group`, `activity`     | `{"op":"granted","queueKey","sessionKey"}` — sent when granted   |
| `release` | `queueKey`*, `sessionKey`                     | `{"op":"ok"}`                                                    |
| `cancel`  | `queueKey`*, `sessionKey`                     | — (no reply) — drop a queued request, and release a Slot the session already holds |
| `role`    | `sessionKey`, `role`                         | — (no reply) — re-tier a live queued waiter (`/queue promote\|demote`); a Holder keeps the tier it was granted with |
| `renew`   | — (heartbeat) or `queueKey`*, `sessionKey` (activity beat) | `{"op":"ok"}` (heartbeat) / `{"op":"ok","queueKey","sessionKey","granted"}` (beat) |
| `status`  | —                                            | `{"op":"status","queues":[{queueKey,maxStreams,switches,holders,waiting}]}` — `holders` and `waiting` are each `[{sessionKey,role,group}…]` |
| `bye`     | — (shutdown intent)                          | — (no reply) — treat this connection as disconnected now: release its leases, drop its queued requests, decrement the client count; the client then closes the socket (#63) |

\* `acquire`, `release` and `cancel` require `queueKey`: a keyless one would create an unnamed
queue that silently holds a slot. The reply is `{"op":"error","reason":"... requires queueKey"}`.

`acquire` is the only request that may stay unanswered: while the queue's slots are taken the
request sits in the tier-ordered waiting list and the grant arrives when a slot frees. A client that
disconnects while queued is dropped from the queue (that is how Esc aborts feel immediate), and
a disconnect that held a slot releases it. A client can also send `cancel` to drop its own
queued request without closing the socket — this is how Esc cancels a request that is still
waiting for a slot. `cancel` covers both halves of an abort: for a session that was already granted
it releases that slot (`daemon.js` `case "cancel"`), so one op serves an Esc while queued and an
Esc while streaming.

`role` is `parent` (interactive) or `child` (subagent) — the wire field for the tier; `group` is an
optional `PI_QUEUE_GROUP` label. `status` reports both for every Holder and every waiter, so a raw
client can label which session holds the slot without the extension. `role` re-tiers a live **waiter** in
place; a live Holder keeps the tier it was granted with until it releases and re-acquires —
`/queue promote|demote` records the override per session, so it takes effect on that session's next
acquire. `activity` is an optional boolean capability flag: a client
that sends `activity: true` in `acquire` opts into idle-yield tracking (the daemon records
`lastActivityAt` on its lease and may yield it under contention). A client that omits it (old
client) is never idle-yielded.

The scheduler grants by tier — parent over child, FIFO within a
tier — with aging anti-starvation: the longest-waiting child jumps the queue once it has waited
`agingAfterMs`, or once `agingAfterSlots` parents have been granted in a row.

## Leases and session switches

A grant is a **lease** with a TTL (`leaseTtlMs`, default 30 s, minimum 1000 ms). A live client
keeps its leases alive with a bare `renew` heartbeat. Each queue resolves its own policy, and the
client's keyed activity beat follows it: `leaseTtlMs / 3` (floor 250 ms) and at most
`idleYieldMs / 2`, so a per-queue TTL never lapses and a live holder is never yielded between
beats. The connection-wide bare heartbeat uses the smallest period across the configured queues.
A config hot-reload re-resolves both and restarts live beats, so a policy edit never evicts a
holder.
**The TTL is the crash grace**: a client that dies mid-stream stops refreshing, and the daemon
reclaims its slot when the TTL lapses and grants the next waiter. That is the only reclamation of a
dead client, and it is bounded — the sweep evicts it within one sweep interval of `leaseTtlMs`
after the last heartbeat (`leaseTtlMs + sweepMs`).

The slot is held per session across a whole run, and **a live holder is never preempted**
(ADR 0002): the queue never switches a streaming holder away. A session switch happens only on:

- **release + Grace** — the run settled: the extension releases 3 s after `agent_settled`, so a
  parent woken by its child keeps the slot through the grace window and the handback is
  switch-free; a session shutdown releases the slot at once, with no grace;
- **abort** — the run's signal aborts: the extension releases a held slot at once, and sends
  `cancel` for a request that is still queued;
- **disconnect** — the socket dies: the daemon releases every lease it held and drops every
  request it had queued (a killed session and a closed laptop look the same from here);
- **TTL lapse** — the client stopped heartbeating, i.e. it died: a session killed mid-stream, a
  process that vanished, a heartbeat starved by a tool that blocks the event loop;
- **idle yield** (ADR 0003) — an activity-flagged holder sent no keyed beat for `idleYieldMs`
  and a waiter is queued: the daemon releases the slot and grants the next waiter. A lone holder
  is never disturbed; an unflagged (old-client) lease is never yielded. The yielded client learns
  on its next beat (`granted: false`) and re-queues.

A keyed `renew` (activity beat) refreshes the TTL, and its reply is authoritative: `granted: false`
means the slot is gone (released, disconnected, past its TTL) and the client must re-queue before it
streams again — a run that lost its slot never streams unqueued. A bare `renew` (no keys)
heartbeats every lease of the connection.

The daemon sweeps every `--sweep-ms` (default 1000 ms; the suites pass a short tick so a TTL-bound
test costs milliseconds). The sweep is two passes:

- **TTL eviction, unconditional** — every lease whose TTL lapsed is evicted whether or not anyone
  is waiting: a lapsed lease is a dead client, and `/queue` must never report it as a holder
  (ADR 0002).
- **idle yield, gated** (ADR 0003) — only when `idleYieldMs > 0`, a waiter is queued, and the
  lease is activity-flagged: a holder silent past `idleYieldMs` loses the slot to the next waiter.

shortcut: a child that stalls while holding the slot keeps it until it is stopped, its socket
closes, its TTL lapses (a dead client), or it is idle-yielded (ADR 0003, activity-flagged
leases only) — the queue never takes the slot back from an *active* holder. That is the
"hold for the whole run" contract; `queueTimeoutMs` remains the parent-side warning.

shortcut: a stream longer than `leaseTtlMs` keeps heartbeating, so its lease never lapses and a
waiter waits for the whole stream. That is the serialisation promise, not a bug — raise
`httpIdleTimeoutMs` (recommended 900000) so the waiter's client stays patient.

The daemon counts every **session switch** — every grant of a queued waiter — as the event that
re-prefills the KV cache, and reports the per-queue count as `switches` in `status`. A direct grant
into a free slot is a Grant, not a Switch, and is not counted. `/queue` prints each Holder and
each waiter as `sessionKey tier [group]`, beside the slot count, the per-tier queue depth and the
switch count. The footer indicator reads the same status: a queued `acquire` stays unanswered, so
the client polls `status` (first at 300 ms, then every 2 s) and finds itself in the queue entry
whose `queueKey` is the key it acquired with — the extension's key is already the group name, so a
group queue matches; a raw client naming a group member's `baseUrl` has to look up the group name,
because only `status` reports group keys. The position it shows is enqueue order, not grant order.

## Queues and config

A `queueKey` is an endpoint URL, a provider name, or a group name. Queues are created on first
use from `<agent dir>/llm-queue.json`:

```json
{
  "defaults": { "maxStreams": 1, "policy": "priority-aging", "leaseTtlMs": 30000, "agingAfterSlots": 3, "agingAfterMs": 60000, "failOpen": true, "daemonIdleExitMs": 0, "logFile": "/home/marat/.pi/agent/llm-queue.log" },
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
The group/entry addressing rule is one implementation (`addressModel`, `addressEndpoint` in
`src/addressing.js`, ADR 0005) shared by the extension and the daemon.

Per-queue fields: `maxStreams`, `policy`, `leaseTtlMs` (minimum 1000), `agingAfterSlots`,
`agingAfterMs`, `idleYieldMs` (all also valid in
`defaults`), plus `enabled` and `match` (queue entries only — `defaults` never reads them).
`failOpen` (default `true`), `queueTimeoutMs` and `blockingTools`
(the tools that pause the run-scoped beat) are client-side (`defaults` only); `daemonIdleExitMs`
(default `0`, exit at once — see Idle-exit) and `logFile` (no file
until you set a path) are daemon-side (`defaults` only). An optional top-level `schemaVersion` (current: 1) must be a positive
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

- **`logFile`** — the daemon's event log path (`defaults` only, a `path` string). Unset: no file,
  and the daemon behaves exactly as before (its stderr is discarded when the extension spawns it
  detached). Set: the daemon appends one JSON line per queue event, so a finished run can be read
  back from a single file. Every line carries `event`, `queueKey`, `sessionKey`, `role` and `ts`
  (ISO timestamp), plus `reason` where the event has one. The vocabulary is the scheduler's own:

  | event     | when                                                                                          |
  | --------- | --------------------------------------------------------------------------------------------- |
  | `grant`   | a session takes the slot — a direct grant into a free slot, or a queued waiter granted         |
  | `switch`  | the grant that hands the slot to a **new** session: the same event `switches` counts           |
  | `queued`  | an `acquire` parks as a waiter                                                                  |
  | `release` | the slot returns — voluntary release, a disconnect, a `cancel` of a granted slot; `reason: "idle"` for an idle yield (ADR 0003) |
  | `evict`   | a TTL reclaim — `reason: "ttl"`, the only eviction after ADR 0002                               |

  The logged `queueKey` is the queue that owns the slot — a group's name when the client addressed
  it by a member key — and the logged `role` is the tier off the lease (or off the waiter, while it
  is still queued), the same field `status` reports, so the log and the reply cannot disagree about
  who holds what. The path is read per event, so a hot-reload can turn the log on, off, or move it.
  **Best effort**: every write is swallowed on failure — an unwritable path, a full disk or a log
  rotation race leaves the daemon serving requests, because observability must never become an
  availability risk.

- **`enabled: false`** — passthrough: the queue grants every session immediately (`status`
  reports `maxStreams: null`, printed as `passthrough` by `/queue`), and the extension leaves the provider unwrapped. A disabled
  group does not re-key its members, so their own endpoint entries still apply.
- **`enabled: false` precedence** — a baseUrl (endpoint) key wins over a provider-name key naming
  the same endpoint (`addressEndpoint`), so the endpoint entry's `enabled` governs the whole endpoint:
  `enabled: false` on the endpoint key disables every provider on it, and no provider-name key can
  silently re-enable it. The reverse is not true — `enabled: false` on a provider-name key alone
  does not disable an endpoint that an enabled endpoint key names, because that key wraps every
  provider serving the endpoint. A disabled endpoint key whose endpoint an enabled provider-name key
  still names leaves that provider wrapped, and the daemon grants it immediately: passthrough by the
  queue, not by the wrap set. Write the disable on the key that owns the endpoint: the endpoint
  key, or the group that names it.
- **`match`** — an explicit resource group, overriding baseUrl keying:
  - **merge**: members (baseUrls and/or provider names) share the group's slot set. The
    extension keys a grouped model's requests by the group name; a raw socket client naming a
    member baseUrl is resolved to the first group whose `match` names it.
  - **split**: a group naming one provider of an endpoint shared by several providers gives
    that provider its own slot set (`match: ["strata"]` beside `match: ["llama"]` on one
    server). Splitting needs provider identity, so it works through the extension's key, not
    through a raw baseUrl.
  - **precedence**: a group naming the model's provider exactly owns it, ahead of a group naming
    only its endpoint — split beats merge, whatever the order in the config file.

### Hot-reload

The daemon watches `llm-queue.json` (250 ms debounce, directory watch so atomic saves keep
working), re-applies queue policy to live queues, and rebuilds them through the reloaded
groups — holders and waiters move with their queue, so an endpoint never ends up with two
slot sets. A widened `maxStreams` grants waiting sessions at once. The extension watches the
same file and re-resolves its wrapped provider set (registering new providers, unregistering
removed ones). A half-written file keeps the last good config. No daemon or pi restart is
needed; the client's per-queue beat periods follow the edit and live beats restart under the new
period. `failOpen` and `queueTimeoutMs` are read once at connect and are the fields that need a
pi restart.

## Client contract

- The extension starts the daemon on demand: `ensureDaemon` runs from the client's connect, so the
  spawn happens on the first socket op (an `acquire`, a `beat` or a `status`) and again whenever the
  socket has to be re-established — not at `session_start`. One detached spawn per connect with
  `stdio: "ignore"`, which is why `logFile` is the only durable record of queue events. The daemon
  resolves the race itself — a live socket makes the second daemon exit 0, a stale socket file is
  reclaimed — so the client needs no liveness probe and no lockfile.
- Leases are per session, keyed by `options.sessionId` (the pi session UUID), not per process: a pi
  process running a parent and its children holds one lease each, and those sessions queue against
  each other instead of colliding on one slot (#53). A request with no `sessionId` (compaction,
  classifier, a direct call) has no session to name: it joins whichever session holds that
  endpoint's slot, and the most recently started session when none does.
- Turn-based affinity: the extension acquires at `agent_start` and releases after the run
  settles, 3 s later (grace, so a parent woken by its child keeps the slot); an abort releases
  a held slot at once and cancels a still-queued request. Requests outside a run (compaction,
  classifier, nested calls) queue for themselves. Every request the run makes beats its lease
  run-scoped, so a compute-busy holder never goes idle; the beat pauses only while the run is parked
  in a blocking tool (`subagent`, `subagent_wait`, `bg_wait`, `ask_user`, `contact_supervisor`,
  `intercom`, configurable via `defaults.blockingTools`), so a parked holder yields the slot to the
  agent it waits on (ADR 0003). A beat answered `granted: false` re-queues the run before it
  streams; the client heartbeats `renew` on a timer so a live session never loses the slot to the
  TTL.
- Reply matching: a client resolves `acquire` only on a `granted` reply naming its own
  `queueKey`/`sessionKey`; replies with no matching request (e.g. a `release` `ok`) are dropped,
  never buffered for a later request to consume. A request addressed at a group member gets its
  replies keyed by the key it asked with (its own `baseUrl`), never the group name — only
  `status` reports group keys, so a raw client polling for its own queue position has to match the
  group name, while the extension (whose key is already the group name) matches its own.
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
- Idle-exit (#60): the daemon self-terminates `daemonIdleExitMs` (default `0` = at once) after the
  last **counted** client disconnects, unlinking its socket, so daemon lifetime is the union of the
  live pi processes' lifetimes: every pi restart picks up new daemon code with no manual kill, and
  the next pi socket op autostarts a fresh daemon. A connection is counted from its first
  state-touching op (`acquire`, `renew`, `role`, `cancel`) and stops being one when it closes. A
  `status`-only connection — the queue observer, a dashboard, a monitoring script — changes no
  queue state, so it never keeps the daemon alive. The extension holds one persistent connection
  per pi process, so the OS closing it is the "last pi is gone" signal, crash-safe by construction.
  A graceful pi does not wait for its process to die: `session_shutdown` sends `bye` (#63), which
  runs the same cleanup a close would, so the idle-exit arms at the word and a pi that lingers
  after its last session cannot keep stale daemon code resident for the next pi to connect to. `> 0` is an opt-in relaunch grace (a session restarting within
  it keeps the warm daemon, and a counted reconnect cancels the pending exit); set it high to keep
  the daemon resident (the daemon clamps the delay to `setTimeout`'s 2^31-1 cap, ~24.8 days, so an
  unclamped "a year" cannot become the 1 ms exit Node coerces it to). A daemon that starts with no
  client arms the same timer with a 30 s floor, so
  a spawn whose client never arrives does not linger.
- Recommended pi setting while queued requests queue up behind a long prefill:
  `httpIdleTimeoutMs: 900000`.

## Tests

`make test` (`npx tsc --noEmit`, then `node --test test/*.test.ts`) drives these seams and never
touches a real model endpoint. Every suite, and what it pins — 41 suites, the count `node --test`
reports, plus one top-level test in `extension-starter-empty.test.ts`:

| file                                   | suites                                                                                                                                                                     |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `test/config.test.ts`                  | config schema validation; config file loading                                                                                                                               |
| `test/daemon.test.ts`                  | daemon socket protocol; lease lifecycle and no preemption; tier ordering (parent > child) and aging; queue policy resolution; per-endpoint queues, groups and hot-reload; observability: status counters; daemon idle-exit; daemon lifetime: counted clients and status invisibility (#60); config schema validation at the daemon seam; connection framing and buffer bounds; idle-yield under contention |
| `test/daemon-event-log.test.ts`        | daemon event log — the `logFile` seam: the JSON-lines sequence of a handover, every event's fields, a TTL reclaim as `evict` + `reason: "ttl"`, an idle yield as `release` + `reason: "idle"`, an unwritable path that leaves the daemon serving, the path followed across a hot-reload (on, moved, off), no file when `logFile` is unset |
| `test/lease-client.test.ts`            | lease client; daemon autostart                                                                                                                                              |
| `test/extension.test.ts`               | self-hosted endpoint detection; starter config generation; role resolution                                                                                                  |
| `test/extension-wrapper.test.ts`       | provider wrapper (queuedStream); config hot-reload (provider set)                                                                                                           |
| `test/extension-timeout.test.ts`       | queueTimeoutMs visible warning — a parent warned past the cap stays queued                                                                                                  |
| `test/extension-config-watch.test.ts`  | config created after startup                                                                                                                                                |
| `test/extension-starter.test.ts`       | starter config on first launch; reconcile of an existing config on session start, the tuned entry's exact text, and the reconciled key as a live queue at the daemon socket                                  |
| `test/extension-starter-empty.test.ts` | (top-level test) creates no config when every endpoint is cloud or native-owned                                                                                             |
| `test/extension-bye.test.ts`           | `bye` on `session_shutdown`: the daemon exits with the last session's shutdown while the client process is still alive (#63)                                                 |
| `test/extension-idle-beat.test.ts`     | run-scoped keyed beat (idle-yield, #57)                                                                                                                                     |
| `test/extension-per-queue-lease.test.ts` | per-queue lease policy (#81): per-queue TTL, short idle window, hot-reloaded TTL and idle window                                                                            |
| `test/addressing.test.ts`              | the group rule, entry-ownership rule and provider/queue-key resolution at their own seam: addressModel, addressEndpoint, queuedProviders, readProviders                     |
| `test/scheduler.test.ts`               | lease / TTL (in memory); idle yield (in memory); serialisation (in memory); tier ordering (in memory); aging (in memory); no preemption (in memory); requestedKey echo (in memory); status view (in memory) |

`test/harness.ts` is not a suite: it is the daemon socket seam (`startDaemon`, `connectClient`,
`waitForDaemon`, `sleep`, `exited`, `QUEUE_KEY`) plus the `after` cleanup every suite calls (`stopAllDaemons`,
`closeAllClients`) — without that cleanup `node --test` hangs. `startDaemon` injects
`daemonIdleExitMs: 1e9` into a config that omits it, so suites that reuse one daemon across
connection gaps are not exited by the production default of 0 (#60); the lifetime and idle-exit
suites set the field explicitly. Assertions go through the JSON-line
protocol and the extension's public seams; the Scheduler's decisions are asserted in memory
(`scheduler.test.ts`). `make e2e`
(`node scripts/e2e-two-sessions.mjs`) is the human-run tracer bullet: two real pi sessions against
a stub endpoint, asserting zero request overlap.
