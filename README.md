# pi-llm-queue

Pi extension + local daemon that serialises LLM access to protect KV-cache
warmth on constrained local model servers.

## Why

Local model servers (llama.cpp on a single GPU) thrash their KV cache when
multiple pi sessions / subagents interleave requests: every session switch
re-prefills the whole context. pi-llm-queue gives the model one session at a
time (turn-based slot lease), with priority tiers, aging, and per-endpoint
queues.

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
2. **Daemon autostart** — the extension spawns the daemon on demand (detached,
   `~/.pi/agent/llm-queue.sock`, stale-socket reclaim, idle-exit after the
   last client leaves). Nothing to run manually, no launch agent, no systemd unit.
3. **Config file** — `~/.pi/agent/llm-queue.json` is optional: a missing (or empty) file
   queues nothing, because queue keys are what select the providers to wrap. Create it to
   activate the queue — the simplest is `{}` plus a `queues` entry per provider/endpoint
   (reference below). Hot-reloads on save.

Recommended pi setting (`~/.pi/agent/settings.json`) so queue wait behind a long prefill does
not trip pi's HTTP idle timeout:

```json
{ "httpIdleTimeoutMs": 900000 }
```

Verify: `/queue` in pi prints the slot holder, per-tier queue depth and session-switch counts.
`make e2e` (human-run) is the two-session tracer bullet against a stub endpoint.

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
  "defaults": { "maxStreams": 1, "policy": "priority-aging", "agingAfterSlots": 3, "agingAfterMs": 60000, "failOpen": true, "daemonIdleExitMs": 60000 },
  "queues": {
    "http://192.168.1.100:8001/v1": { "maxStreams": 1, "blockSubagentLaunch": true },
    "zen": { "maxStreams": 3, "policy": "fifo", "blockSubagentLaunch": false },
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
| `leaseTtlMs` | `30000` | A lease dies without heartbeats (crashed session reclaim). Minimum `1000`. |
| `idleQuantumMs` | `120000` | A holder with no model request for this long yields to a waiter — never while a request is in flight. |
| `monopolyRequests` / `monopolyMs` | `12` / `180000` | A holder is preempted after N requests or M seconds — never while a request is in flight. |
| `enabled` | `true` | `false` = passthrough: provider unwrapped, fully parallel. |
| `match` | — | Explicit resource group: baseUrls and/or provider names sharing one queue. |
| `blockSubagentLaunch` | `maxStreams == 1` | Force `subagent` launches to `async: false` on this queue. |
| `failOpen` | `true` | Daemon unreachable → pass through. `false` → hard-error. `defaults` only. |
| `queueTimeoutMs` | unlimited | Parent queue-wait cap → pass through with warning. Child is always unlimited. `defaults` only. |
| `daemonIdleExitMs` | `60000` | Daemon self-exit after the last client disconnects. `defaults` only. |

## Role contract (env vars)

Neutral, string-only — zero hard dependency on pi-subagents; any runner can declare roles:

| Variable | Effect |
| -------- | ------ |
| `PI_QUEUE_ROLE=child\|parent` | Explicit tier for the process. Wins over everything below. |
| `PI_SUBAGENT_CHILD=1` | pi-subagents hint → `child`. |
| `PI_SUBAGENT_PARENT_SESSION=…` | pi-subagents hint → `child`. |
| `PI_QUEUE_GROUP=<id>` | Grouping label carried through to `/queue` status. |
| (default) | `parent` (interactive). |

`/queue promote|demote` overrides the tier for the current session ad hoc.

## Dev notes

- **Tests are stub-only.** `make test` (typecheck + `node --test`) drives the daemon socket seam
  and the thin pi-glue seam with stub endpoints only — it never touches a real model endpoint.
  `make e2e` is the human-run tracer bullet (two real pi sessions against a stub endpoint).
  Real cache-warmth validation against llama.cpp is a separate human ticket (#10).
- **Serial subagents.** This repo's own implementing sessions run on a local model: launch
  subagents sequentially and wait for each to hand back control — never fire parallel model
  requests. On `maxStreams: 1` queues the extension enforces this (`async: true → false`);
  batch a group as one workflow call so the parent is woken once, not per child.
- **Model guidance for implementing sessions.** Use a strong coding model for implementation
  sessions; keep prompts append-only so the session's prefix stays cache-warm; run the queue
  daemon while implementing so sessions share the endpoint instead of thrashing it. Tests and
  typecheck (`make test`) need no GPU.

## Design decisions (from grilling session)

- Client-side: pi extension + shared local daemon (unix socket), fail-open.
- Queue key = provider baseUrl; explicit resource groups (`queues.<name>.match`) override;
  providers can be excluded (`enabled: false`); config hot-reload.
- Turn-based session affinity: slot held per session across a run
  (`agent_start` → `agent_settled`), lease with TTL, released on settle.
- Priority: interactive parent > subagent child, with aging anti-starvation.
- Role detection: `PI_QUEUE_ROLE` contract, `PI_SUBAGENT_CHILD` /
  `PI_SUBAGENT_PARENT_SESSION` as optional hints, `PI_QUEUE_GROUP` grouping label;
  zero hard dependency on pi-subagents. `/queue promote|demote` overrides the tier.
- `blockSubagentLaunch: true` by default for `maxStreams: 1` queues.
- Batched subagent launches (one workflow call → one parent wake) as the
  documented pattern; `wakeBatching` is roadmap.
- Observability: `/queue` command + live TUI indicator.
- Daemon auto-starts from the extension, idle-exits when last client leaves.
- Config: `~/.pi/agent/llm-queue.json`, hot-reload.

## Subagent launches (batch pattern)

On a single-slot queue (`maxStreams: 1`) the extension's `tool_call` hook rewrites
the `subagent` tool's `async: true → false`, so the orchestrator blocks on each
launch and parallel launches become a serial queue. Launch one batch as a single
workflow call (one `subagent` call with a `workflow`/`tasks`/`chain` argument): the
parent is woken once at the end, so its large context is re-prefilled once, not
once per child. `blockSubagentLaunch` defaults to `true` when `maxStreams == 1`
(`false` otherwise); override per queue with `queues.<name>.blockSubagentLaunch`.
The hook is a no-op when the `subagent` tool is absent (pi-subagents not installed).

Status: tracer bullet (#2) implemented — daemon socket protocol, FIFO scheduler, extension
provider wrapper with turn-based lease, daemon autostart, fail-open. Lease TTL + heartbeat and
the session-switch quanta (release / idle / monopoly) are implemented (#3). Priority tiers
(parent > child, FIFO within tier), aging anti-starvation, the role/group env contract and
`/queue promote|demote` are implemented (#4). Per-endpoint queues with explicit `match` groups,
`enabled: false` passthrough, the config schema (all fields accepted; `blockSubagentLaunch` is
implemented (#6); `queueTimeoutMs` / `failOpen` / `daemonIdleExitMs` implemented in #8) and config hot-reload
(daemon + extension) are implemented (#5). Observability (#7) — the queued footer indicator, the
`/queue` status (slot holder, per-tier queue depth, session-switch counts) and JSON status — is
implemented. Fail-open hardening + idle-exit (#8) — `failOpen` (default true), the `queueTimeoutMs`
parent queue-wait cap (child unlimited) and the daemon `daemonIdleExitMs` idle-exit — is implemented;
packaging + docs (#9) — npm package, versioned config schema validation, install/config/env/dev docs —
is implemented; e2e cache-warmth validation (#10) is human-run.
