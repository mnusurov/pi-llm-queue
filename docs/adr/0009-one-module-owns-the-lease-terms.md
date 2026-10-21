# 9. One module owns the Lease's terms

Date: 2026-10-21

## Status

Accepted (implements #109). Structural, not behavioural: the wire protocol, the daemon's replies and
every queue behaviour are byte-for-byte unchanged.

## Context

The Lease's terms — the TTL and the idle-yield window a Lease is held under — are a glossary term
with no owner. Their shape was declared in three places, their rule lived in two, and the ADR 0007
port leaked them.

Since #105 the terms are the Grant's payload: the daemon sends them on the `granted` reply and the
client refreshes the Lease on them. Four modules owned a piece of that:

- `src/scheduler.js` — `termsOf()` built the object; `effectiveTtlMs()` held the "the TTL term only
  grows" rule; the Lease record carried `ttlMs` and `idleYieldMs` as two separate fields, so the
  terms lived in two representations at once.
- `src/daemon.js` — `grantedReply()` wrote the wire fields `leaseTtlMs` and `idleYieldMs` by hand.
- `src/lease-client.ts` — `interface LeaseTerms` declared the same shape again; the client kept a
  `Map` of the terms per live Lease and derived the keyed beat period and the connection-wide
  heartbeat period from them.
- `src/config.js` — `heartbeatMs()` and `keyedBeatMs()` derived the refresh period from the terms,
  and lived in the config-schema module although a refresh period is not a config default.

The rule was split too. The daemon-side rule ("the TTL term only grows, the idle-yield window is
frozen") lived in the Scheduler; the client-side rule ("take the terms the Grant carried, never your
own copy of the config") lived in the lease client. Both terms bugs, #104 and #105, had one cause:
the terms were derived in two places.

The seam leaked as well. `RunLifecycleOptions` took the keyed beat period as a separate injected
callback, while the period itself came from the Lease's terms — which the Extension read from the
concrete `LeaseClient`, not through the `LeasePort` the Run lifecycle crosses. ADR 0007's amendment
recorded that as it stands ("the port and the injection are unchanged"), so it was a recorded
decision, not an accident. It is still a leak: the Run lifecycle's beat period is a property of the
Lease, and the port that owns the Lease does not hand it over.

## Decision

One module owns the terms: `src/lease-terms.js`, plain JavaScript (Node refuses type stripping under
`node_modules`, so a `.ts` module would kill an installed daemon — the same reason the other
daemon-loaded modules are plain JS).

Its interface is the shape, the refresh period, the wire encoding and the growth rule — six exports.

- `LeaseTerms` — the shape: `{ leaseTtlMs, idleYieldMs }`. A JSDoc typedef in the module; the
  TypeScript client imports it as a type (`allowJs` is on, so TS infers it). `termsFor(leaseTtlMs,
  idleYieldMs)` builds one, so no other module writes the shape.
- `periodForTerms(terms)` — the period a Lease held under those terms is refreshed at. It absorbs
  `keyedBeatMs` and `heartbeatMs` from the config module: a refresh period is a behaviour constant,
  not a config default, and it belongs beside the code that owns it.
- `termsToWire(terms)` / `termsFromWire(msg)` — the wire encoding. The daemon writes the terms
  through them, the client reads them through them. The wire field names do not change.
- `growTerms(granted, current)` — the rule: the TTL term is the longer of the two, the idle-yield
  window is the one the Lease was granted under.

What the module does not own: the config. It does not read the config file, does not know the config
schema, and does not resolve a Queue's rules. The Extension owns its copy of the config and hands
the module terms; the module turns terms into a period.

Three consequences:

- **The Queue and the Lease hold one object.** A Queue holds the terms new Leases are granted
  under; a Lease holds the terms it was granted under. The Scheduler stops carrying `ttlMs` and
  `idleYieldMs` as two separate fields, and the translation function that turns a Lease into terms
  disappears — the Lease already holds them.
- **The client stores the period, not the terms.** The client uses the terms for exactly one thing:
  deriving periods. It keeps a period per live Lease instead of the terms themselves, so the terms'
  shape exists on the daemon side and on the wire, and nowhere else.
- **The port carries the period.** The `LeasePort` grows a beat-period entry and the
  `RunLifecycleOptions` beat-period callback goes away, so the Run lifecycle has one collaborator for
  the period and asks for it through the seam it already crosses. The fallback for a daemon that
  sends no terms — the client's own copy of the config — is supplied where the port is built, and
  the module derives the period from those config-implied terms.

## Considered options

- **A TypeScript terms module.** Rejected: the daemon imports it, and Node's built-in type
  stripping is refused under `node_modules` — an installed daemon would die on the import.
- **Keep the terms in the Scheduler and let the client read the config.** Rejected: that is the
  #104/#105 class of bug — the terms derived in two places.
- **Let the terms module read the config.** Rejected: the config is the Extension's and the daemon
  adapter's business; a terms module that resolved a Queue's rules would own the policy cascade
  too, and the fallback for a daemon that sends no terms would move out of the Extension.
- **Pass the terms through the port instead of the period.** Rejected: the client stores a period,
  not terms; handing terms over the port would put the terms' shape on the client side again.

## Consequences

- The Scheduler's public surface does not grow: the growth rule is a call into the terms module, not
  a new concept in the Scheduler.
- The lease client's public surface grows by nothing: it already exposed a beat period; what changes
  is what it stores to answer it. It re-arms its connection-wide heartbeat when the period changes,
  not when the terms change: the timer runs on the period, so a Grant whose terms change but whose
  period does not leaves the timer where it is.
- The Run lifecycle's constructor options lose the beat-period callback; the port gains a beat-period
  entry.
- The Extension builds the port with the fallback, so the config-implied terms stay the Extension's
  business and the terms module stays free of the config.
- The rules become reachable without a daemon and without a socket: `test/lease-terms.test.ts`
  drives the module through its interface — the period derived from terms, the growth rule, and the
  wire encoding in both directions, including the "a daemon that sends no terms" case. The existing
  seams stay the integration oracle: the Scheduler's suites for the daemon-side rules, the
  lease-client suite for the terms travelling over the wire, and the pi-seam suites for a Holder
  keeping its Slot.
- ADR 0007 is amended: the `LeasePort` carries the beat period and the Run lifecycle's separate
  beat-period injection is gone. ADR 0005 is amended: the config module no longer owns the
  refresh-period derivation. ADR 0002 and ADR 0003 stand unchanged — the rules they record (the
  TTL term only grows, the window is frozen) do not change.
