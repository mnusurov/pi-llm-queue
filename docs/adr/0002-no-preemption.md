# 2. No preemption: a live holder keeps the slot until it releases it

Date: 2026-10-18

## Status

Accepted (implements #49, supersedes the switch quanta of #3).

## Context

The daemon preempted a live holder mid-run whenever a waiter was queued, on three quanta:
`idleQuantumMs` (120 s of no model request), `monopolyRequests` (12 requests) and `monopolyMs`
(180 s of unbroken monopoly). Observed in a 2-worker parallel workflow (session 01a14ee4,
2026-10-18): worker A ran solo, a 121 s tool pause evicted A on the idle quantum, then the
12-request monopoly evicted B, and the slot ping-ponged between the two children 4–6 times in
20 minutes. Every switch re-prefills the KV cache (~180K input tokens) — exactly the cost the
queue exists to prevent.

Child-to-child preemption benefits nobody: both sessions are background, and the queue already
delivers the slot at each child's `agent_settled`. The quanta only produced switches, never
throughput.

## Decision

Remove all three quanta. No forced preemption of a live holder under any role combination.

- **Switch causes.** A slot changes hands only on voluntary release (`agent_settled` + the 3 s
  grace), an abort, a disconnect, or the TTL expiry of a client that stopped heartbeating.
- **Sweep is TTL-only and unconditional.** It evicts every lease whose TTL lapsed, whether or not
  anyone is waiting, so `/queue` never reports a ghost holder.
- **No config surface.** `idleQuantumMs`, `monopolyRequests` and `monopolyMs` are deleted from the
  schema. Pre-release software: no back-compat, no deprecation warning. A config carrying them
  fails validation and the daemon serves defaults, so preemption cannot be re-enabled.
- **The `done` beat is dead protocol.** It existed only to make a lease switchable again once a
  stream ended. With no quanta, the daemon has nothing to gate on: the beat is a TTL refresh and
  its reply is the grant state. `inFlight` and the request counter go with it.
- **Accepted costs.** A parent may queue indefinitely behind a child (that is the tier contract;
  `queueTimeoutMs` stays the opt-in parent escape hatch). A live stuck child holds the slot
  forever — the "hold for the whole run" contract. Escape hatches: stop the child session, close
  its socket, or let its TTL lapse (a dead client).
- **Unchanged.** Aging, the settle grace, TTL, fail-open, and the `switches` counter.

## Consequences

- A run never loses its slot mid-stream, so a long tool execution is safe at any length: the
  `idleQuantumMs` tuning knob and its documented shortcut are gone.
- Session switches drop to one per run handover, which is the minimum the serialisation promise
  allows; KV-cache warmth is now a property of the design, not of a tuned timer.
- A wedged child blocks its queue until it is stopped. `/queue` shows the holder and the waiters,
  so the operator sees who to stop; the daemon cannot decide this for them.
- A client whose event loop is blocked longer than `leaseTtlMs` (a synchronous tool) loses its
  slot to the TTL, and its next beat answers `granted: false` — the run re-queues instead of
  streaming unqueued. Heartbeat period stays a third of the TTL for that reason.
- Pre-#49 configs break loudly (a path-named unknown-field error at startup) rather than
  silently keeping a knob that no longer exists.
