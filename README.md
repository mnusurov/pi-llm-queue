# pi-llm-queue

Pi extension + local daemon that serialises LLM access to protect KV-cache
warmth on constrained local model servers.

## Why

Local model servers (llama.cpp on a single GPU) thrash their KV cache when
multiple pi sessions / subagents interleave requests: every session switch
re-prefills the whole context. pi-llm-queue gives the model one session at a
time (turn-based slot lease), with priority tiers, aging, and per-endpoint
queues.

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
packaging (#9) is not built.
