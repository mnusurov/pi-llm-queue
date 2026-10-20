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
  queue's remap target, applies the remap and migrates the state itself.
- The Switch predicate becomes explicit: a Switch **iff** the granted session came from the waiting
  list — the protocol's wording, not an inference from the presence of a prior Holder.
- The Aging streak is bumped in one Grant path, so the direct-grant fast path cannot drift from the
  queued path.
- `GLOSSARY.md` gains **Scheduler** as a term, and **Sweep** loses `--sweep-ms` and the 1 s
  interval: the glossary carries no implementation details.
