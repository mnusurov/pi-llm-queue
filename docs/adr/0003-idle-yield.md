# 3. Idle Holder yields under contention

Date: 2026-10-18

## Status

Accepted (implements #51, refines ADR 0002).

## Context

A live run (2026-10-18, ticket #43 test): an orchestrator parent launched a `workflow:true`
batch of subagents. Deadlock for 30 minutes, server idle:

1. Parent acquires the Slot at `agent_start` and holds it for the whole run, kept alive by
   keyless heartbeats.
2. The `blockSubagentLaunch` hook forces `async:false` on the `subagent` tool, so the parent
   blocks awaiting the batch while still holding the Slot.
3. Workflow children are separate sessions: they `acquire` as tier `child`, park behind the
   parent, and never stream. The parent never settles, never releases, heartbeats forever.
   Circular wait.

Two structural gaps make this possible:

- **The daemon cannot see activity.** A holder's keyed beat fires once at stream start; during
  an 11-minute stream and during a blocked-forever tool call the daemon sees the same keyless
  heartbeats. `Beat` is documented as "the daemon's only view of stream activity" — today that
  is false.
- **The extension knows only plugin specifics.** `tool_call` keyed on `toolName === "subagent"`
  cannot generalise: pi ships no subagent concept, and no pi event says "this tool waits on
  other sessions."

## Decision

**Idle-yield**: a Holder that is not streaming loses the Slot when someone else is waiting.
Purely behavioural — no plugin names, no tool names, works for any current or future subagent
mechanism.

1. **Client (activity truth)**: while `queuedStream` is actually streaming (`for await` open),
   send a keyed beat every `heartbeatMs`. Between streams — tool execution, blocked subagent —
   no keyed beats.
2. **Daemon (decision)**: per-lease `lastActivityAt` (updated by grant + keyed beats). In
   `sweep`: if `waiters.length > 0 && now - lastActivityAt > idleYieldMs` →
   `release(reason: "idle")` → `grantNext`. Waiters-gated: a lone Holder is never disturbed.
3. **Recovery needs no new path**: the yielded client's next keyed beat / next stream gets
   `granted:false` → existing re-queue before streaming; the Slot moves child1 → child2 → … →
   woken parent exactly as the serialisation contract promises.
4. **Config**: `idleYieldMs` — valid in `defaults` and per-queue (like `leaseTtlMs`), default
   `30000`, `0` disables. 30 s = three missed beats (safe against beat jitter) and ≫ typical
   inter-stream gaps (tool execs are ms–s; a blocked subagent is minutes).
5. **Version-skew guard**: the client sends capability `activity: true` in `acquire`; the daemon
   only yield-tracks/yields flagged leases. New daemon + old client = no yield (safe). Old
   daemon = unknown field ignored (safe). New + new = works with no config opt-in.
6. **`queueTimeoutMs` becomes warn-only**: on expiry the parent gets the warning but stays
   queued — passthrough is removed. Rationale: with idle-yield a stuck queue self-heals; the
   only remaining long legal wait is "behind a live streaming child", and passing through exactly
   there re-creates the stampede this queue exists to prevent.
7. **Delete `blockSubagentLaunch`**: the `tool_call` hook, `resolveBlockSubagentLaunch`, the
   config field, tests and docs. Serialisation of children is the queue's job (child tier,
   unbounded wait); launch batching is pi-subagents' native `runs.all` + completion batcher
   (one wake). The forced-sync path is additionally broken on its own (30-min foreground
   workflow timeout + orphaned runners).

## Consequences

- ADR 0002's no-preemption rule stands for *active* holders: a streaming holder is never
  preempted. Idle-yield adds a fifth switch cause: daemon-initiated idle yield. ADR 0002's
  "voluntary release / abort / disconnect / TTL" list becomes those plus idle-yield.
- `Beat` becomes truthful: the daemon's only view of stream activity. Keyed beats during
  streaming refresh `lastActivityAt`; between streams they stop, and the daemon sees the silence.
- `blockSubagentLaunch` is deleted: the `tool_call` hook, `resolveBlockSubagentLaunch`, the
  config field, tests and docs. Serialisation of children is the queue's job (child tier,
  unbounded wait); launch batching is pi-subagents' native mechanism.
- `queueTimeoutMs` is warn-only: a parent past the cap is warned but stays queued. Passthrough
  is removed — with idle-yield a stuck queue self-heals, and passing through behind a live
  streaming child re-creates the stampede.
- The event log gains `reason: "idle"` on the `release` event (same shape as `evict` +
  `reason: "ttl"`). No new event types.
- `status` is unchanged — after a yield the parent is honestly neither Holder nor Waiter (it is
  not requesting).

## Amendment (2026-10-19, #57): idle is "no keyed beat", not "between streams"

Date: 2026-10-19

Status: Accepted (amends the Decision's item 1; the daemon's item 2 is unchanged).

A parallel implementer run (two Children, one queue, 2026-10-19) showed the client-side rule
above is wrong: a Child inside a 9-minute tool call (a `bash` running tests) sends no keyed
beat, goes activity-stale, and is idle-yielded mid-run — the two lanes alternated
re-prefilling each other's KV cache, which is the harm the queue exists to prevent. Measured
on 990 sessions: the 30 s default fires inside 9.06 % of compute tool calls.

**Amended client rule**: the keyed beat is run-scoped, not stream-scoped. The client starts
one keyed beat per held lease at the Grant (`agent_start` grant, stream grant) and stops it at
release (settle grace, abort, shutdown) or when a beat answers `granted: false`. While the run
sits in a **blocking tool** — one that waits on another agent or the user (`subagent`,
`subagent_wait`, `bg_wait`, `ask_user`, `contact_supervisor`, `intercom`; configurable via
`defaults.blockingTools`) — the beat pauses, so the Parent→Child handover that motivated this
ADR is preserved.

Consequences of the amendment:

- Idle is defined as "no keyed beat". For a pi client that means: dead client, stalled event
  loop, or parked on an agent/user. A compute-busy Holder is never idle; idle-yield is a
  stall guard plus the Parent→Child handover, never a tool-pause handover.
- `idleYieldMs` keeps its 30 s default. With run-scoped beats the threshold is purely
  handover latency, not work interruption. Rejected alternatives:
  - *Raise the threshold only* (60 s cuts interruptions to 2.72 %, 120 s to 0.69 %): doubles
    to quadruples the Parent→Child handover latency for everyone, and a live 300 s experiment
    showed the failure mode — a Child parked in `contact_supervisor` held the Slot ~18 minutes
    and starved its sibling. 120 s remains documented for clients that do not beat per run
    (raw socket clients, e2e scripts), where the threshold is their only protection.
  - *A daemon-side `runActive` protocol flag*: the daemon would have to model pi's run
    lifecycle; the client already knows it, and the version-skew `activity` flag pattern
    says activity truth belongs in the client's beats.
- The daemon is unchanged: keyed renew refreshes `lastActivityAt`, bare heartbeat refreshes
  only the TTL; the sweep and the `activity` capability flag stand as decided above.

## Amendment (2026-10-20, #81): the beat period follows the queue's policy

Date: 2026-10-20

Status: Accepted (amends the Decision's item 1 and the 2026-10-19 amendment's beat cadence; the
daemon's item 2 is unchanged).

The `defaults`-only beat period broke the one-slot promise when a queue overrode `leaseTtlMs` or
`idleYieldMs`: a holder beating for a 30 s TTL lost a queue's 1 s lease, and a 1 s idle window
yielded an actively beating holder between beats. The client's keyed beat now runs at
`min(heartbeatMs(the queue's leaseTtlMs), the queue's idleYieldMs / 2)` for the queue it holds
(`idleYieldMs: 0` imposes no bound), and the connection-wide bare heartbeat runs at the smallest
such period across the configured queues. A config hot-reload re-resolves both, fires one immediate
keyed beat, and restarts the timer; the daemon refreshes every carried holder's `lastActivityAt`
when it rebuilds after a reload, so a shortened idle window cannot yield a holder before its client
has had a chance to beat under the new policy. At the defaults (30 s / 30 s) the period is still
10 s — three missed beats.

## Amendment (2026-10-21, #105): the idle-yield window is a Lease term

Date: 2026-10-21

Status: Accepted (amends the Decision's item 2 and the 2026-10-20 (#81) amendment's beat period;
the yield itself is unchanged).

The #81 amendment made the client's Beat period follow the queue's policy — read from the client's
own copy of the config file. That copy is not the daemon's: the two processes read the file
independently, each through its own watcher, so one of them can see a config edit the other never
sees. The client's period then stops matching the window the daemon enforces, and the Sweep yields
a Holder that is still streaming.

**Amended rule**: the idle-yield window a Lease is measured against is the window it was granted
under, and the client derives its Beat period from the terms the Grant carried — the TTL and the
window alike (ADR 0002's #105 amendment).

- A changed window applies at the next Grant, never to a Lease that is already held: a Holder keeps
  the window it was granted under, so the client that refreshes on those terms is never slower than
  the window allows.
- The window is not lengthened under a held Lease either: a longer window would leave that
  Holder's client beating slower than the window allows — the Sweep would Yield the Slot to a
  Waiter while the Holder still streams. (The TTL may grow: a longer TTL only makes the client's
  refresh more than sufficient.)
- The client's own copy of the config is the fallback for a daemon that sends no terms. A config
  hot-reload no longer changes how often a held Lease is refreshed — it only re-resolves the
  provider and queue sets, and restarts the live beats under the current period.
