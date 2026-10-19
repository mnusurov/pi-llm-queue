# pi-llm-queue

Pi extension + local daemon that serialises LLM access to protect KV-cache
warmth on constrained local model servers.

## Why

Local model servers (llama.cpp on a single GPU) thrash their KV cache when
multiple pi sessions / subagents interleave requests: every session switch
re-prefills the whole context. pi-llm-queue gives the model one session at a
time (turn-based slot lease), with tier ordering (parent > child), aging,
and per-endpoint queues.

## Install (fresh machine)

Requires Node ≥ 22.19 (pi's own floor — the daemon runs TypeScript directly via Node's
built-in type stripping) on macOS/Linux (unix socket). Zero runtime dependencies; pi supplies
the `@earendil-works/*` peer packages.

```bash
pi install npm:pi-llm-queue
```

That single command is the whole install:

1. **Provider wrapper registration** — at first session start the extension wraps every
   provider named by a queue key in `~/.pi/agent/llm-queue.json`, plus every provider serving
   a queued endpoint (providers on one endpoint share one queue automatically). Providers owned
   by native extensions are left unqueued. Providers must be declared in
   `~/.pi/agent/models.json` — queue keys name providers or their baseUrls.
2. **Daemon autostart** — the extension spawns the daemon on demand: on the client's first socket
   op (an acquire, a beat, a status poll), not at session start, and again whenever the socket has
   to be re-established. Detached, `stdio` ignored (`~/.pi/agent/llm-queue.sock`, stale-socket
   reclaim, exit the moment the last counted client leaves — a `status`-only connection (an
   Observer) is not counted, #60).
   Nothing to run manually, no launch agent, no
   systemd unit — and no stderr to read afterwards, which is why `logFile` is the durable record.
3. **Config file** — `~/.pi/agent/llm-queue.json` is created for you on the first launch, and an
   existing file is reconciled add-only on the first session start of each pi process (see
   [Auto-starter](#auto-starter-first-launch) below). It hot-reloads on save. An empty file
   queues nothing, because queue keys are what select the providers to wrap; add a `queues`
   entry per provider/endpoint to widen the queue (reference below).

### Auto-starter (first launch)

On the first session start after install, when `~/.pi/agent/llm-queue.json` is absent, the
extension generates a starter config from your `~/.pi/agent/models.json` and notifies you — the
queue is live from that first session, no manual config needed. On the first session start of
each later pi process an existing file is **reconciled add-only**: a self-hosted endpoint that
no entry addresses gains its Queue key as `enabled: true`, and nothing already in the file is
touched — not a tuned entry, not an explicit `enabled: false`, not the `defaults` block.
Reconcile runs once per process, inside the same guard as provider registration: a session
started later in the same process does not trigger it, and a config edited mid-process is
hot-reloaded for queue policy and the wrap set, so a key the file is missing waits for the next
pi process. A config the schema rejects is left alone (its rejection is already reported at
session start).

- **Self-hosted endpoints are queued** (enabled, default single slot → strict serialisation). A
  provider is self-hosted when its baseUrl host is loopback (`localhost`, `127.x`, `::1`),
  RFC1918 (`10.x`, `192.168.x`, `172.16–31.x`), link-local (`169.254.x`), or a local/mDNS domain
  (`*.local`, `*.lan`, `*.home.arpa`).
- **Cloud endpoints are written as passthrough** (`enabled: false`) — public IPs and other
  hostnames. They stay fully parallel; flip `enabled: true` to queue them. Providers with no
  baseUrl (pure cloud APIs) are omitted from the file entirely — add a provider-name key to queue
  one.
- **Native-owned providers are excluded** (providers owned by native extensions are left unqueued).
- **Generated file shape**: `schemaVersion: 1`, one endpoint-keyed `queues` entry per baseUrl
  (deduplicated, sorted), and a `defaults` block containing only the Parent queue-wait cap
  `queueTimeoutMs: 120000` — a Parent's queue wait warns visibly at 120 s and stays queued; Children
  keep an unbounded wait. The client reads `queueTimeoutMs` once at startup, so the cap binds from
  the next pi restart, not for the session that wrote the file. Nothing else in `defaults`: every
  other field stays absent so the schema's own defaults apply (ADR 0001, as amended by #46).

It is created only when there is at least one self-hosted, non-native endpoint. To disable the queue
permanently, set every key to `enabled: false` — the stable disable path. Deleting the file is not a
permanent disable while such an endpoint exists, because it regenerates on the next launch when
absent, and deleting a single key is not one either, because reconcile re-adds it. `enabled: false`
is the disable that survives every restart: reconcile reads it as a deliberate passthrough and never
touches it.

Recommended pi setting (`~/.pi/agent/settings.json`) so queue wait behind a long prefill does
not trip pi's HTTP idle timeout:

```json
{ "httpIdleTimeoutMs": 900000 }
```

Verify: `/queue` in pi prints each Holder's and Waiter's session id, tier and group label, the slot
count, per-tier queue depth and session-switch counts. `make e2e` (human-run) is the two-session
tracer bullet against a stub endpoint.

## Config reference

`~/.pi/agent/llm-queue.json` (global-only, hot-reload). Full semantics:
[docs/daemon-protocol.md](docs/daemon-protocol.md). Invalid configs never kill the queue: a schema violation surfaces a path-named error through the
extension's `ui.notify` (the daemon also logs it to stderr, discarded when the extension spawns
it detached — run `node src/daemon.js --socket … --config …` by hand to see it) and falls back
to defaults at startup, or keeps the last good config
on hot-reload; a half-written file (invalid JSON) is a silent no-op that keeps the last good
config. Optional `schemaVersion` (current: 1) — a newer version tells you
to upgrade pi-llm-queue instead of failing silently.

```json
{
  "defaults": { "maxStreams": 1, "policy": "priority-aging", "agingAfterSlots": 3, "agingAfterMs": 60000, "failOpen": true, "daemonIdleExitMs": 0, "logFile": "/home/marat/.pi/agent/llm-queue.log" },
  "queues": {
    "http://192.168.1.100:8001/v1": { "maxStreams": 1, "idleYieldMs": 30000 },
    "zen": { "maxStreams": 3, "policy": "fifo" },
    "free-api": { "enabled": false },
    "one-gpu": { "match": ["http://192.168.1.100:8001/v1", "http://192.168.1.100:8002/v1"] }
  }
}
```

| Field | Default | Meaning |
| ----- | ------- | ------- |
| `maxStreams` | `1` | Concurrent slots for the queue. `1` = strict serialisation. |
| `policy` | `priority-aging` | `priority-aging` (parent > child with aging) or `fifo`. |
| `agingAfterSlots` | `3` | Boost a waiting child after this many consecutive parent grants. |
| `agingAfterMs` | `60000` | Boost a waiting child after this long waiting. |
| `leaseTtlMs` | `30000` | A lease dies without heartbeats (crashed session reclaim — the crash grace). Minimum `1000`. |
| `enabled` | `true` | `false` = passthrough: provider unwrapped, fully parallel. An endpoint (baseUrl) key wins over a provider-name key naming the same endpoint. |
| `match` | — | Explicit resource group: baseUrls and/or provider names sharing one queue. |
| `idleYieldMs` | `30000` | Idle-yield threshold: an activity-flagged holder with no keyed beat this long yields the slot when a waiter is queued. The pi client beats for the whole run except while parked in a blocking tool (ADR 0003), so this is a dead-client/stall guard and the Parent→Child handover latency — never a tool-pause handover. Non-beating clients (raw socket, e2e scripts) have no run-scoped beat: use `120000` there to protect long builds/e2e from mid-run yield. `0` disables. |
| `failOpen` | `true` | Daemon unreachable → pass through. `false` → hard-error. `defaults` only. |
| `queueTimeoutMs` | unlimited | Parent queue-wait warning threshold → visible warning, stays queued. Child is always unlimited. `defaults` only. The Starter config ships `120000`. |
| `blockingTools` | `["subagent","subagent_wait","bg_wait","ask_user","contact_supervisor","intercom"]` | Tools that park the run on another agent or the user: the run-scoped keyed beat pauses while one is in flight, so a parked holder yields the slot to the agent it waits on (ADR 0003). Add a custom waiting tool here. `defaults` only. |
| `daemonIdleExitMs` | `0` | Daemon self-exit after the last **counted** client disconnects: `0` exits at once, so daemon lifetime = the live pi processes' lifetimes and a daemon code change needs no manual kill (the next pi op autostarts a fresh daemon). A connection is counted from its first state-touching op (`acquire`/`renew`/`role`/`cancel`); a `status`-only connection (an Observer — a dashboard, a monitoring script) is invisible to it. `> 0` is an opt-in relaunch grace; set it high to keep the daemon resident (clamped to `setTimeout`'s 2^31-1 cap, ~24.8 days). `defaults` only. |
| `logFile` | — (no file) | Daemon event log path: one JSON line per queue event — `grant`, `switch`, `queued`, `release` (`reason: "idle"` for an idle yield), `evict` (`reason: "ttl"` for a TTL reclaim) — each carrying `event`, `queueKey`, `sessionKey`, `role`, `ts` (+ `reason`). Best-effort: an unwritable path leaves the daemon serving. `defaults` only. |

**`enabled: false` precedence.** A baseUrl (endpoint) key wins over a provider-name key naming the
same endpoint, so the endpoint entry's `enabled` governs the whole endpoint: disabling the endpoint
key disables every provider on it, and no provider-name key can silently re-enable it. The reverse
does not hold — `enabled: false` on a provider-name key alone does not disable an endpoint that an
enabled endpoint key names, because that key wraps every provider serving it. Write the disable on
the key that owns the endpoint: the endpoint key, or the Group that names it.

**Event log.** With `logFile` set, the daemon appends the queue's own events, so a finished run can
be read back from one file (the extension spawns the daemon detached, so its stderr is gone). The
`queueKey` on a line is the queue that owns the slot — a Group's name when the client addressed it
by a member key — and the `role` is the tier off the Lease (or the Waiter, while it is still
queued), the same field `status` reports. The path is read per event, so a hot-reload can turn the
log on, off, or move it. Writes are swallowed on failure: observability never becomes an
availability risk. Full semantics: [docs/daemon-protocol.md](docs/daemon-protocol.md).

## Role contract (env vars)

Neutral, string-only — zero hard dependency on pi-subagents; any runner can declare roles:

| Variable | Effect |
| -------- | ------ |
| `PI_QUEUE_ROLE=child\|parent` | Explicit tier for the process. Wins over everything below. |
| `PI_SUBAGENT_CHILD=1` | pi-subagents hint → `child`. |
| `PI_SUBAGENT_PARENT_SESSION=…` | pi-subagents hint → `child`. |
| `PI_QUEUE_GROUP=<id>` | Grouping label carried through to `/queue` status. |
| (default) | `parent` (interactive). |

`/queue promote|demote` overrides the tier for the current session ad hoc: it re-tiers a request
already queued, and takes effect on that session's next acquire. A live Holder keeps the tier it
was granted with until it releases the Slot.

## Dev notes

- **Tests are stub-only.** `make test` (typecheck + `node --test`) drives the daemon socket seam
  and the thin pi-glue seam with stub endpoints only — it never touches a real model endpoint. The
  suites: `config.test.ts`, `daemon.test.ts`, `daemon-event-log.test.ts`, `lease-client.test.ts`,
  `extension.test.ts`, `extension-wrapper.test.ts`, `extension-timeout.test.ts`,
  `extension-config-watch.test.ts`, `extension-starter.test.ts`, `extension-starter-empty.test.ts`
  (26 suites, the count `node --test` reports, plus one top-level test in
  `extension-starter-empty.test.ts`); `test/harness.ts` is the daemon socket seam, not a suite. The
  per-suite list is in [docs/daemon-protocol.md](docs/daemon-protocol.md#tests).
  `make e2e` is the human-run tracer bullet (two real pi sessions against a stub endpoint).
  Real cache-warmth validation against llama.cpp is a separate human ticket (#10).
- **Serial subagents.** This repo's own implementing sessions run on a local model: launch
  subagents sequentially and wait for each to hand back control — never fire parallel model
  requests. Serialisation of children is the queue's job (child tier, unbounded wait); batch a
  group as one workflow call so the parent is woken once, not per child.
- **Model guidance for implementing sessions.** Use a strong coding model for implementation
  sessions; keep prompts append-only so the session's prefix stays cache-warm; run the queue
  daemon while implementing so sessions share the endpoint instead of thrashing it. Tests and
  typecheck (`make test`) need no GPU.

## Design decisions (from grilling session)

- Client-side: pi extension + shared local daemon (unix socket), fail-open.
- Queue key = provider baseUrl; explicit resource groups (`queues.<name>.match`) override;
  providers can be excluded (`enabled: false`); config hot-reload. An endpoint key wins over a
  provider-name key naming the same endpoint, so a deliberate passthrough is not silently overridden.
- Turn-based session affinity: slot held per session across a run
  (`agent_start` → `agent_settled`), lease with TTL, released on settle.
- Leases are per session, keyed by `options.sessionId` (#53): a process running a Parent and its
  Children holds one Lease each, and those sessions queue against each other.
- Tiers: parent (interactive) above child (subagent), with aging anti-starvation.
- No preemption (ADR 0002): a live holder keeps the slot until it releases it, aborts,
  disconnects, its TTL lapses, or it is idle-yielded under contention (ADR 0003); a switch
  always re-prefills the KV cache.
- Role detection: `PI_QUEUE_ROLE` contract, `PI_SUBAGENT_CHILD` /
  `PI_SUBAGENT_PARENT_SESSION` as optional hints, `PI_QUEUE_GROUP` grouping label;
  zero hard dependency on pi-subagents. `/queue promote|demote` overrides the tier.
- Batched subagent launches (one workflow call → one parent wake) as the
  documented pattern; `wakeBatching` is roadmap.
- Observability: `/queue` command + live TUI indicator.
- Daemon auto-starts from the extension, exits the moment the last counted client leaves
  (`status` polls don't count, #60).
- Config: `~/.pi/agent/llm-queue.json`, hot-reload.

## Subagent launches (batch pattern)

On a single-slot queue (`maxStreams: 1`) children queue behind the parent (child tier,
unbounded wait) and serialise through the queue. Launch one batch as a single
workflow call (one `subagent` call with a `workflow`/`tasks`/`chain` argument): the
parent is woken once at the end, so its large context is re-prefilled once, not
once per child. Idle-yield (ADR 0003) ensures a blocked parent yields the slot to its
children when it stops streaming; the parent re-queues after the batch completes.

Status: tracer bullet (#2) implemented — daemon socket protocol, FIFO scheduler, extension
provider wrapper with turn-based lease, daemon autostart, fail-open. Lease TTL + heartbeat and the
session switch on release are implemented (#3); the session-switch quanta (#3) were removed by
#49 — see ADR 0002. Tier ordering
(parent > child, FIFO within tier), aging anti-starvation, the role/group env contract and
`/queue promote|demote` are implemented (#4). Per-endpoint queues with explicit `match` groups,
`enabled: false` passthrough, the config schema (all fields accepted; `queueTimeoutMs` / `failOpen` /
`daemonIdleExitMs` implemented in #8) and config hot-reload
(daemon + extension) are implemented (#5). Observability (#7) — the queued footer indicator, the
`/queue` status (slot holder, per-tier queue depth, session-switch counts) and JSON status — is
implemented. Fail-open hardening + idle-exit (#8) — `failOpen` (default true), the `queueTimeoutMs`
parent queue-wait warning (child unlimited) and the daemon `daemonIdleExitMs` idle-exit — is implemented;
the daemon now exits with the last counted client and an Observer polling `status` no longer keeps it alive (#60);
packaging + docs (#9) — npm package, versioned config schema validation, install/config/env/dev docs —
is implemented; e2e cache-warmth validation (#10) is human-run; the auto-starter (#37/#38) — a
starter `llm-queue.json` generated from `models.json` on first launch — is implemented. Idle-yield
(#51) — an idle Holder yields its Slot when Waiters exist, `blockSubagentLaunch` deleted,
`queueTimeoutMs` warn-only — is implemented (ADR 0003). Queue observability (#43) — `status` and
`/queue` reporting tier and group label for Holders and Waiters (#45), the Starter config's
`defaults` queue-wait cap and add-only reconcile (#46, amending ADR 0001), the daemon's `logFile`
event log (#47) and this documentation pass (#48) — is implemented.
