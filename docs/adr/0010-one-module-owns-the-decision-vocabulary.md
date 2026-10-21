# 10. One module owns the Decision vocabulary and its effects

Date: 2026-10-21

## Status

Accepted (implements #111). Structural, not behavioural: the wire protocol, the daemon's replies and
the event log are byte-for-byte unchanged.

## Context

The Scheduler's output language had no owner. The Scheduler wrote its answers as bare object
literals at the point of use, the socket adapter switched on them in a closure and built the
effects inline, and the in-memory adapter — the Adapter's second implementation, by ADR 0004's own
words — re-declared the shape by hand and re-implemented the routing rule.

Five kinds of decision leave the Scheduler: `grant`, `switch`, `queued`, `release`, `evict`. Each
was an object literal with no shared shape and no exported type.

- `src/scheduler.js` pushed the decisions as literals: `grant` carried the connId, the requested key
  and the Lease's terms; `switch`, `queued`, `release` and `evict` carried the queue key, the
  session key and the tier.
- `src/daemon.js` switched on `d.type` in a closure and built the effect inline: the log line, the
  `granted` reply and the batch order. The log line came from a second closure, which renamed the
  Scheduler's `tier` to the wire's `role` on the way out; the reply came from a third.
- `test/scheduler-harness.ts` declared `interface Decision` and `interface OverCapacity` by hand
  and re-implemented the routing rule in `capture()`: a decision that names a connection goes to
  that connection, one that does not goes to the actor.

Two consequences followed.

**The seam leaked.** The event log's vocabulary is a public contract — `docs/daemon-protocol.md`
gives the event table and the field shape — yet it was reachable only through a real daemon and a
temp file: the socket adapter's file cannot be imported (it parses `process.argv` at the top level
and exits when the socket path is missing), so the effect mapping had no seam a test could cross.

**The batch-before-reply promise was proven by racing.** A handover is two decisions — the grant
and the switch it causes — and the reply belongs to both, so the whole batch's lines must be
written before any reply goes out (ADR 0004's amendment). The socket suite repeated a handover 50
times, opening two fresh clients each time, to catch a reply that outran its log line.

And the vocabulary drifted: when #105 put the Lease's terms on the grant decision, the driver's
hand-written `interface Decision` had to be extended by hand — with a shape that
`src/lease-terms.js` now owns (ADR 0009).

## Decision

One module owns the Decision vocabulary and its effects: `src/decisions.js`, plain JavaScript (the
`src/config.js` reason: the daemon imports it and Node refuses type stripping under
`node_modules`, so a `.ts` module would kill an installed daemon).

Its interface is the shape plus four things:

- `Decision` — the shape: `{ type, connId?, queueKey, sessionKey, tier, requestedKey?, reason?,
  terms? }`, where `type` is one of `grant · switch · queued · release · evict`. A JSDoc typedef in
  the module; the Scheduler imports it, and the TypeScript driver imports it as a type (`allowJs`
  is on, so TS infers it). The `terms` field is typed by the terms module (ADR 0009), not declared
  again. The over-capacity report — the Scheduler's other output shape — is a typedef here too.
- `decisionLogLine(decision, now)` — the event line: `{ event, queueKey, sessionKey, role, ts,
  reason? }`. The module renames `tier` to `role` on the way out: the Scheduler's word is `tier`,
  the wire's word is `role`, and the meeting point is the effect.
- `grantedReply(queueKey, sessionKey, terms)` — the `granted` reply. One builder, two callers: the
  grant path takes the terms off the Decision, and the refresh path — a re-acquire of a held Slot,
  which produces a reply and no Decision — takes them off the acquire result.
- `overCapacityLine(report, now)` — the `config` event line.
- `emit(decisions, { write, send, now })` — the batch order: every line is handed to the writer
  before any reply is handed to the sender. A reply is sent only for a `grant`, and it echoes the
  key the client asked with (the requested key, falling back to the resolved queue key).
- `decisionOwner(decision)` — the ownership rule: a Decision names the connection it belongs to, or
  names none (a sweep's decisions name no actor). The socket adapter uses it to find the socket;
  the in-memory adapter uses it to file the record. Neither re-derives it.

The edges stay outside the module. The file write — the path read per event, best effort,
swallowed — and the socket write are injected, so the module never touches the filesystem or a
socket. The clock is injected too, and it is a function: every line keeps its own timestamp,
exactly as today. One stamp per batch would be a behaviour change to the log, and it is not part
of this move.

What the module does not own: the status view (a projection of the Queue's state, not of a
Decision — the log is an event stream, the status view a snapshot), the config, the file, the
socket.

## Considered options

- **A TypeScript decisions module.** Rejected: the daemon imports it, and Node's built-in type
  stripping is refused under `node_modules` — an installed daemon would die on the import. The
  same reason the other daemon-loaded modules are plain JS.
- **Keep the mapping in the socket adapter and only export the shape.** Rejected: the batch order
  and the `tier` → `role` rename are rules, not shapes, and they would stay untestable without a
  daemon and without a socket — the leak the ticket is about.
- **Let the Scheduler build the lines and the replies.** Rejected: the Scheduler performs no side
  effect and reads no clock (ADR 0004). A line is an effect.
- **One timestamp per batch.** Rejected: a behaviour change to the log, and not part of this move.
  The clock stays a function that the module calls per line.

## Consequences

- The Scheduler's decisions carry the module's shape; the Scheduler imports the typedef and writes
  decisions of it.
- The socket adapter's effect mapping leaves its file: the inline switch, the inline log line, the
  inline reply and the inline batch order become one call into the module, with the file writer and
  the socket sender injected.
- The in-memory adapter consumes the module: it records the same lines and the same replies the
  socket adapter would, next to the raw Decisions it files per connection. Its hand-written
  `interface Decision` and `interface OverCapacity` are gone.
- The wire bytes do not change: same field names, same event names, same replies, same order.
- The rules become reachable without a daemon and without a socket. A new per-module suite drives
  the module through its interface: the log line for each decision kind, the `tier` → `role`
  rename, the reply (the echoed key, the terms), the over-capacity line, and the batch order —
  every line handed to the writer before any reply handed to the sender, asserted deterministically.
- The 50-iteration race retires: the batch-before-reply promise is a property of the module, so it
  is asserted once and deterministically instead of being raced for. The socket suite keeps only
  what the socket adds: the file exists, the lines are on disk when the reply lands (one handover,
  not fifty), no file is written when `logFile` is unset, the path moves on a hot-reload, and an
  unwritable path does not take the daemon down.
- The event log's vocabulary is asserted in the module's suite, not only over a socket.
- The existing seams stay the integration oracle: the Scheduler's suites, the socket suites, and
  the pi-seam suites.
- ADR 0004 is amended: the mapping moves out of the socket adapter's file into the module. The
  record's core stands (the Scheduler decides, the Adapter emits) and the batch-order amendment
  stands; the rule is now the module's.
- `GLOSSARY.md` gains **Decision** — the Scheduler's answer about one Queue event — and **Evict**,
  the TTL reclaim, whose term lived in the code and in the protocol doc but not in the glossary.
