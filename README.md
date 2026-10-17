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
- Queue key = provider baseUrl; explicit resource groups override; providers
  can be excluded (`enabled: false`).
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

Status: tracer bullet (#2) implemented — daemon socket protocol, FIFO scheduler, extension
provider wrapper with turn-based lease, daemon autostart, fail-open. Lease TTL + heartbeat and
the session-switch quanta (release / idle / monopoly) are implemented (#3). Priority tiers
(parent > child, FIFO within tier), aging anti-starvation, the role/group env contract and
`/queue promote|demote` are implemented (#4). `blockSubagentLaunch` and the TUI indicator are
#1 scope, not built.
