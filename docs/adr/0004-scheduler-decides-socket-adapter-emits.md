# 4. The Scheduler decides; the socket adapter emits

Date: 2026-10-19

## Status

Accepted (spec #65). Structural, not behavioural: ADR 0002's no-preemption rule and ADR 0003's
idle-yield rule are preserved verbatim by the Scheduler. This ADR records where they live.

## Context

Every scheduling decision in the daemon — Tier ordering, Aging, TTL reclamation, idle Yield, the
Switch count — is fused with its side effects at the same call site: `grantNext` picks a waiter and
then appends to the event log and writes to a socket, and seven bare `Date.now()` calls read the
wall clock inside the decisions. So the decisions are reachable only over the socket:
`test/daemon.test.ts` spawns 38 daemon processes across 60 tests, spends 43 sleeps to prove a Grant
did *not* arrive, and the harness grew a `--sweep-ms` knob so that a TTL-bound test costs
milliseconds. A boundary — a Lease expiring at `now − 1` versus at `now` — cannot be asserted at all.

## Decision

Extract a **Scheduler** module (`src/scheduler.js`, plain JS for the `config.js` reason: the daemon
imports it and Node refuses type-stripping under `node_modules`). It owns the Queue state and the
Lease records, takes a resolved policy and a clock instant, and returns decisions —
`grant · release · evict · yield · switch · queued` — performing no socket write, no log append and
no clock read. The socket adapter emits: it maps each decision to a reply and a log line in the
returned order, stamps the log's wall-clock `ts`, and keeps framing, the Counted-client count and
the idle-exit, the Sweep timer, the `status` view, the `requestedKey` echo, and config resolution
through `config.js`.

The Scheduler owns the **connection scope** of a Lease: each Lease record carries the connId it was
granted to, so the Beat's authoritative `granted` answer is one Scheduler query. The connection
scope is a protocol fact ("a bare `renew` heartbeats every lease of the connection"), not plumbing.

## Considered options

- **Scheduler owns Queue state only, `conn.leases` stays on the connection.** Rejected: the
  `granted` check is an identity comparison across both collections, so "a live Holder is never
  preempted" (ADR 0002) would live in the Scheduler *and* in the adapter at once — the duplication
  the extraction exists to remove.
- **Test the decisions only over the socket, as today.** Rejected: 38 process spawns and 43
  wall-clock sleeps per suite is the measured cost, and a time boundary cannot be pinned at all.
- **A second production adapter.** Rejected: the extension running a Scheduler in-process would
  duplicate the daemon, not vary at the seam. The second adapter is the in-memory test driver, which
  satisfies the interface fully.

## Consequences

- The seam is real: two adapters — the socket daemon, and a synchronous in-memory driver in tests
  that fixes `now` and reads the returned decisions.
- Hot-reload (`rebuildQueues`) deliberately stays adapter-side: it mutates `lease.queueKey` and
  `waiter.queue` in place because the connection records reference the old Queue objects, and with
  the Scheduler owning those records the reference repair needs its own contract. Amendment (ADR
  0005): the addressing rules live in `src/addressing.js` — the adapter asks it for each live
  queue's remap target, applies the remap and migrates the state itself. Amendment (2026-10-21,
  #103): the Queue rebuild itself moves into the Scheduler, which iterates its own Queues and asks for
  each Queue's remap target through the resolver the adapter supplies.
- The Switch predicate becomes explicit: a Switch **iff** the granted session came from the waiting
  list — the protocol's wording, not an inference from the presence of a prior Holder.
- The Aging streak is bumped in one Grant path, so the direct-grant fast path cannot drift from the
  queued path.
- `GLOSSARY.md` gains **Scheduler** as a term, and **Sweep** loses `--sweep-ms` and the 1 s
  interval: the glossary carries no implementation details.

## Amendment (2026-10-20, #100): the whole batch is logged before any reply goes out

Date: 2026-10-20

Status: Accepted (amends the Decision's "in the returned order" clause; the Scheduler, the decision
list and the wire protocol are unchanged).

The Decision says the socket adapter "maps each decision to a reply and a log line in the returned
order". Read as *the log line, then the reply, for each decision in turn*, that ordering let a
reply outrun the log: a handover is two decisions — the grant and the switch it causes — and the
`granted` reply went out between their two log lines. The peer process can wake on another core
and read the event log in that gap, so a reader saw a grant with no switch. The event-log sink
already promised the opposite — every event a reply caused is on disk before that reply goes out
(#47) — and the promise was the one the code broke.

**Amended adapter rule**: the adapter writes the whole batch's log lines first and sends the
replies after. The log order is still the returned order; what changed is that no reply leaves
before the batch it belongs to is on disk. A reader that opens the file the moment a reply lands
sees the whole story.

Consequences of the amendment:

- The `grant` decision's reply and the `switch` decision's log line no longer interleave: one
  decision batch, one write, then the replies.
- Nothing else moves. The decision list, the log's field shape, the reply's shape and the order of
  the log lines are exactly as decided above.
- The rule is now testable as a promise rather than a race: the log's completeness at reply time
  is asserted over repeated handovers in `test/daemon-event-log.test.ts`.

## Amendment (2026-10-21, #103): the Scheduler owns the hot-reload Queue rebuild

Date: 2026-10-21

Status: Accepted (amends the Consequences clause that keeps the hot-reload adapter-side).

The Consequences clause keeps `rebuildQueues` in the socket adapter because "the connection records
reference the old Queue objects" — a Waiter held `waiter.queue = moved`, a direct reference to the
Queue record. That was true when the Scheduler was extracted. It is not true now: the extraction
replaced the reference with `waiter.queueKey = target`, and this record's own Decision moved the
Lease records and their connection scope into the Scheduler. Nothing outside the Scheduler
references a Queue record any more, so the reference repair the clause protects no longer exists.

**Amended rule**: the Scheduler owns the hot-reload Queue rebuild end to end.

- The Scheduler takes a second resolver beside the one it already has for a Queue's rules: given a
  Queue key, it answers the Queue key that Queue lives under after a reload. The Adapter supplies
  it — the rule stays in Addressing, which never sees Scheduler state (ADR 0005).
- The Scheduler iterates its own Queues, so the Adapter never enumerates them, and the queue map
  and the members that existed only for the Adapter's surgery stop being part of its public
  surface.
- The rebuild carries each Queue's Holders (each Holder's activity clock restarted, as before), its
  Waiters and its switch count; re-resolves the Queue's rules; reports the Queues left over
  capacity; and grants whatever the new capacity frees.
- It returns the decisions to emit and the over-capacity report. The Adapter emits the decisions
  and logs the report as the `config` event it already writes — the report's shape is the event's
  shape, so the protocol does not change.

Structural, not behavioural: the wire protocol, the daemon's replies and every queue behaviour are
byte-for-byte unchanged. The rebuild becomes reachable in memory, so it is asserted through the
Scheduler's interface; the socket-level hot-reload suites stay the integration oracle.
