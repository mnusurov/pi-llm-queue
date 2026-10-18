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
