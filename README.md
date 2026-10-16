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
- Role detection: `PI_QUEUE_ROLE` contract, `PI_SUBAGENT_CHILD` as optional
  hint; zero hard dependency on pi-subagents.
- `blockSubagentLaunch: true` by default for `maxStreams: 1` queues.
- Batched subagent launches (one workflow call → one parent wake) as the
  documented pattern; `wakeBatching` is roadmap.
- Observability: `/queue` command + live TUI indicator.
- Daemon auto-starts from the extension, idle-exits when last client leaves.
- Config: `~/.pi/agent/llm-queue.json`, hot-reload.

Status: tracer bullet implemented (issue #2); remaining tickets tracked in
GitHub Issues.

## What works today (tracer bullet)

- Daemon (`src/daemon/daemon.js`, plain Node, zero deps): unix socket, NDJSON
  protocol (`acquire`/`renew`/`release`/`cancel`/`status`/`ping`/`reload`),
  one queue per endpoint baseUrl, `maxStreams` slots, parent tier ahead of
  child tier, FIFO within a tier, lease TTL reclaim, disconnect cleanup,
  session-switch counters. Protocol: `docs/protocol.md`.
- Extension (`src/index.ts`): provider wrapper registered with
  `pi.registerProvider()` that waits for a per-session slot, then delegates to
  the built-in `openai-completions` stream; lease held across a run
  (`agent_start` → `agent_settled` + grace), released immediately on abort.
- Daemon autostart (`src/lib/autostart.ts`): detached spawn, lockfile pid,
  stale-lockfile reclaim.
- Fail-open: no daemon, dead daemon, or `queueTimeoutMs` expiry → requests
  pass through unqueued (timeout degrades with a warning).
- `/queue` prints slot holder, per-tier depth, and switch counts.

## Config

`~/.pi/agent/llm-queue.json` (global only):

```json
{
  "defaults": { "maxStreams": 1, "policy": "priority-aging", "agingAfterSlots": 3, "agingAfterMs": 60000, "failOpen": true, "daemonIdleExitMs": 60000 },
  "queues": {
    "http://192.168.1.100:8001/v1": { "maxStreams": 1, "blockSubagentLaunch": true },
    "zen": { "maxStreams": 3, "policy": "fifo", "blockSubagentLaunch": false }
  }
}
```

Keys are endpoint baseUrls (providers on one GPU merge automatically) or
provider names resolved to their baseUrl; baseUrl entries win on collision.
`enabled: false` → passthrough. Recommended pi setting:
`httpIdleTimeoutMs: 900000` so queue wait plus long prefill never trips pi's
HTTP idle timeout.

Accepted today by the daemon: `maxStreams`, `enabled`, `failOpen`, `leaseTtlMs`.
Accepted by the client: `queueTimeoutMs`. Parsed but not yet acted on (tracked
by later tickets): `policy`, `agingAfterSlots`, `agingAfterMs` (#4 aging),
`blockSubagentLaunch` (#6 subagent-launch hook), `daemonIdleExitMs` (#8
idle-exit). The daemon re-reads config on a `reload` request; a file watcher
for automatic hot-reload is #5. `failOpen: false` makes a daemon outage fail
the request instead of passing it through.

Role contract (no dependency on any subagent extension): `PI_QUEUE_ROLE=child|parent`
wins over the `PI_SUBAGENT_CHILD` hint (read as an env string); `PI_QUEUE_GROUP`
groups sessions.

## Development

```bash
make setup      # npm install
make typecheck  # tsc --noEmit
make test       # typecheck + full suite (~6s)
make test-socket# daemon socket seam only
```

Dev notes (honoured by implementing sessions):

- Tests are stub/socket only. They drive the daemon over a temp unix socket
  and never contact a real model endpoint.
- Subagents in this repo run sequentially: launch one, wait for it to hand
  control back, then launch the next. The model is local, and parallel
  requests evict each other's KV cache — which is exactly what this project
  exists to prevent.

