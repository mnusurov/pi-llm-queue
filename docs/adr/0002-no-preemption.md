# 2. No preemption: a live holder keeps the slot until it releases it

Date: 2026-10-18

## Status

Accepted (implements #49, supersedes the switch quanta of #3). Refined by ADR 0003 (#51): an
idle-yield adds a fifth Switch cause, and the Sweep gains a gated idle-yield pass beside its
unconditional TTL eviction. Amended 2026-10-21 (#104): a policy edit never shortens a Lease that is
already held — a Lease's term only ever grows, and its expiry is refreshed under the longer of that
term and the Queue's current TTL.

## Context

The daemon preempted a live holder mid-run whenever a waiter was queued, on three quanta:
`idleQuantumMs` (120 s of no model request), `monopolyRequests` (12 requests) and `monopolyMs`
(180 s of unbroken monopoly). Observed in a 2-child parallel workflow (session 01a14ee4,
2026-10-18): child A ran solo, a 121 s tool pause evicted A on the idle quantum, then the
12-request monopoly evicted B, and the slot ping-ponged between the two children 4–6 times in
20 minutes. Every switch re-prefills the KV cache (~180K input tokens) — exactly the cost the
queue exists to prevent.

Child-to-child preemption benefits nobody: both sessions are background, and the queue already
delivers the slot at each child's `agent_settled`. The quanta only produced switches, never
throughput.

## Decision

Remove all three quanta. No forced preemption of a live holder under any role combination.

- **Switch causes.** A slot changes hands only on voluntary release (`agent_settled` + the 3 s
  grace), an abort, a disconnect, or the TTL lapse of a client that stopped heartbeating. ADR 0003
  adds the fifth: a daemon-initiated idle-yield of an activity-flagged Holder with Waiters queued.
- **Sweep is unconditional.** It evicts every lease whose TTL lapsed, whether or not anyone is
  waiting, so `/queue` never reports a ghost holder. ADR 0003 adds a second, gated pass to the same
  sweep (idle-yield: `idleYieldMs > 0`, waiters present, activity-flagged leases only); the TTL
  eviction stays unconditional.
- **No config surface.** `idleQuantumMs`, `monopolyRequests` and `monopolyMs` are deleted from the
  schema. Pre-release software: no back-compat, no deprecation warning. A config carrying them
  fails validation and the daemon serves defaults, so preemption cannot be re-enabled.
- **The `done` beat is dead protocol.** It existed only to make a lease switchable again once a
  stream ended. With no quanta, the daemon has nothing to gate on: the beat is a TTL refresh and
  its reply is the grant state. `inFlight` and the request counter go with it.
- **Accepted costs.** A parent may queue indefinitely behind a child (that is the tier contract;
  `queueTimeoutMs` is a warn-only signal, not an escape — a parent past the cap stays queued,
  ADR 0003). A live stuck child holds the slot forever — the "hold for the whole run" contract.
  Escape hatches: stop the child session, close its socket, let its TTL lapse (a dead client), or —
  once it goes idle with someone waiting — let the idle-yield take the slot (ADR 0003).
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

## Amendment (2026-10-21, #104): a policy edit never shortens a held Lease

Date: 2026-10-21

Status: Accepted (amends the Decision's TTL lapse: a Lease is reclaimed only when the TTL it was
granted under lapses).

The daemon and the Extension read the same config file independently, each with its own debounce. A
config edit that shortened a queue's `leaseTtlMs` reached the daemon at once, while the Extension
may still have been beating at the old period (a third of the old TTL — ten seconds at the default).
The first beat after the daemon's rebuild re-dated the Holder's Lease with the new, shorter TTL, so
the Lease lapsed between two beats: the Sweep reclaimed it, and the Holder — still streaming — lost
its Slot to a Waiter. Two sessions then streamed to the same endpoint at once: the serialisation
promise this daemon exists to keep. The hole needed the new TTL to be under a third of the old one
(the client's beat period), and the schema allows a TTL of 1000 ms — far under a third of the 30 s
default.

**Amended rule**: a Lease records the TTL it is held under, and that term only ever grows.

- Every refresh of its expiry (a keyed beat, a bare heartbeat, a re-acquire) uses the longer of the
  Lease's term and the Queue's current TTL, and the term absorbs the longer one.
- A shortened TTL applies at the next Grant, never to a Lease that is already held: the Holder keeps
  its Slot until it releases it — the promise this record makes for every other cause of a Switch.
- A lengthened TTL applies at once: the rebuild re-dates the carried Holder's Lease under it.
- A Queue whose TTL did not change behaves exactly as before: the effective TTL is the TTL it always
  was, and the expiry still advances with every beat.

The term only ever growing is what closes the last hole: a lengthening followed by a shortening
cannot take the lengthening back, so a Holder that keeps beating is never evicted by a policy edit
sequence either.

The hot-reload rebuild carries a Holder's Lease with the terms it was granted under; only the
Queue's own terms change.

Not closed here: the mirror direction, where the client saw the edit and the daemon did not, so the
client refreshed slower than the TTL the daemon was enforcing. The client must refresh on the terms
the daemon granted the Lease under, not on its own copy of the config — that is #105.

## Amendment (2026-10-21, #105): the client refreshes on the Lease's terms

Date: 2026-10-21

Status: Accepted (amends the Decision's TTL lapse and the 2026-10-21 (#104) amendment: the Lease's
terms are what both processes act on).

The #104 rule closed the direction where the daemon enforced a shorter TTL than the client beat at.
The other direction was open: the daemon and the Extension read the config file independently, each
through its own watcher, so a config edit one of them saw and the other did not decided how long a
Lease lived. A client that saw a lengthened TTL slowed its refresh below the TTL the daemon was
still enforcing: the Lease lapsed between two beats, the Sweep reclaimed it, and the Holder — still
streaming — lost its Slot to a Waiter.

**Amended rule**: the Grant carries the Lease's terms — the TTL and the idle-yield window it is
granted under — and the client refreshes the Lease on them.

- The client derives its keyed Beat period from those terms, through the same derivation it used
  before (`leaseTtlMs / 3`, at most `idleYieldMs / 2`), and the connection-wide bare heartbeat
  runs at the smallest such period across the Leases it covers. The client's own copy of the config
  is its fallback for a daemon that sends no terms; it never decides a Lease's life.
- The TTL term still only ever grows (#104): a Lease is measured against the longer of the term it
  was granted under and the Queue's current one.
- The idle-yield window does **not** change under a held Lease: it is the window the Lease was
  granted under, for as long as it is held. A window the daemon lengthened under a live Holder
  would leave that Holder's client beating slower than the window allows — the Sweep would Yield
  the Slot to a Waiter while the Holder still streams — so a changed window (shortened, lengthened
  or disabled) applies at the next Grant.
- The terms are additive on the wire: a client that does not read them keeps working.

Together the two halves close the class: the daemon never cuts a Lease short, and the client never
refreshes slower than the terms the daemon enforces allow.
