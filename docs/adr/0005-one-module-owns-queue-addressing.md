# 5. One module owns queue addressing

Date: 2026-10-20

## Status

Accepted (spec #77). Structural, not behavioural: the wire protocol, the daemon's replies and every
queue behaviour are byte-for-byte unchanged. This ADR records where the addressing rule lives.

## Context

"Which Queue addresses this request?" is read through several places with no single owner. The
shared key/entry rule sat in `src/config.js` (`groupKeyFor` / `queueEntryFor`), but callers had to
know invariants the signature did not carry: the provider catalog had to be the `.providers`
sub-object (both processes unwrapped it themselves), the daemon only ever passed a `{ baseUrl }`
model — so the provider-name match branch was unreachable from it, unmarked — and the key rewrite
plus the reload remap were protocol steps the socket adapter mirrored by hand. The reverse mapping
(Queue key → providers to wrap) lived only inside the extension (~50 lines). The policy defaults
table and the heartbeat derivation `max(250, floor(leaseTtlMs/3))` were re-typed in the daemon's
policy resolution, the extension's heartbeat setup, the lease client's fallback and the test
harnesses.

## Decision

Extract an **Addressing** module (`src/addressing.js`, plain JS for the `config.js` reason: the
daemon imports it and Node refuses type-stripping under `node_modules`). It owns the addressing
rules in both directions and nothing else. Four structural decisions:

- **Rule, not state.** The module owns the rules — queue key, owning entry, remap target, provider
  mapping. It never sees Scheduler state: `rebuildQueues` stays in the socket adapter (ADR 0004),
  asks the module for each live queue's remap target, and migrates holders, waiters and switch
  counts itself.
- **Entry, not policy.** `address*()` returns `{ queueKey, entry }` — the raw config entry. The
  daemon's pick-chain (entry field → defaults field → fallback) stays daemon-side; the module only
  feeds it the exported `DEFAULTS`.
- **Two forms, no union.** `addressModel(model, queues, providers)` for the extension, where the
  provider-name branch is live, and `addressEndpoint(baseUrl, queues, providers)` for the daemon's
  world, where that branch is unreachable by construction. The split makes the daemon-side dead
  branch explicit instead of hiding it behind one signature with an optional field.
- **Catalog as argument.** `readProviders(path)` absorbs the `.providers`-unwrap invariant both
  callers repeated, and the addressing functions take the catalog as an argument. Each process
  keeps its own reporting around the read: the daemon's unreadable-catalog stderr warning stays in
  the daemon.

The reverse direction is `queuedProviders(queues, providers)`; `membersOf`, `matchingProviders` and
`keyAddressesEndpoint` move in as its internals, exported only for the extension's Starter-config
coverage check. `config.js` keeps schema, validation and defaults, and gains `DEFAULTS` (the policy
defaults table) and `heartbeatMs(ttlMs)`.

## Considered options

- **Keep the rule in `config.js`.** Rejected: the module is about queue addressing, the config
  module about schema and validation, and the reverse mapping has no home in a file the extension
  reads for validation only.
- **One function with an optional `provider`.** Rejected: it hides the fact that the daemon can
  never reach the provider-name branch, which is the split limitation the docs describe.
- **Move the pick-chain in, returning a resolved policy.** Rejected (entry-not-policy): the
  pick-chain is the daemon's protocol-side resolution, and the extension reads entry fields of its
  own. Merging them would give the module a caller-specific notion of "resolved".
- **Addressing as a stateful object shared by both processes.** Rejected: the two processes are
  separate OS processes with separate config watches; only the rule can be shared, and state
  belongs to the Scheduler (ADR 0004).

## Consequences

- The group rule, the entry-ownership rule and the provider mapping exist once. The extension's
  `resolveQueueKey` / `resolveQueueProviders` wrappers are gone; the daemon's `policyFor` /
  `groupFor` closures are calls into the module.
- The defaults table has one spelling. The lease client's `10000` fallback and the daemon's
  idle-exit startup floor stay out of it: they are fallbacks of other layers, not policy defaults.
- Tests stop baking constants: the rule tests live in `test/addressing.test.ts`, the heartbeat case
  derives its period from `heartbeatMs`, and the in-memory driver reads `DEFAULTS`.
- The honest limit of the split: the provider-match branch is live for the extension and
  unreachable only through the endpoint form. A raw socket client names an endpoint, so it lands in
  the endpoint-level group — the documented split limitation, unchanged by this ADR.
- `GLOSSARY.md` gains **Addressing** as a term; a Queue key is its result, not the rule itself.
