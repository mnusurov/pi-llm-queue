# 7. One client-side module owns the Run lifecycle

Date: 2026-10-20

## Status

Accepted (implements #92). Structural, not behavioural: the daemon, wire protocol, Grace, Beat
periods, idle-yield and no-preemption are unchanged. This ADR records where the Extension's Run
lifecycle lives.

## Context

The Extension's run lifecycle was spread across three places, held together by prose comments:

- `SessionState` carried mutable fields (`heldQueueKey`, `releaseTimer`, `runActive`, `beatTimers`,
  `blockingCalls`, `pendingAcquire`, `queuedInfo`, `indicatorTimer`) plus a dozen methods.
- The pi handlers (`agent_start`, `agent_settled`, `session_shutdown`, tool start/end) read and
  wrote those fields directly, including the ordering rules for a Run switching Queues.
- `queuedStream` had its own `finally`: `selfQueued` / `wasHeld` / `inGrace` snapshots decided the
  Grant handover, duplicating what the Run already knew.

Recent fixes were patches on exactly these fields: #55 (a stream before `agent_start`'s grant
queued the session twice) and #81 (a Run switching Queues leaked the old lease/beat and
request-owned leases were missed at release). The rules existed, but callers had to know them.

## Decision

Extract `src/run-lifecycle.ts` (TypeScript, Extension-side only, same reason as the other `.ts`
modules: it never reaches an installed daemon). One `RunLifecycle` per session owns:

- Run state transitions (`idle` / `queued` / `held` / `grace`).
- Per-Queue lease ownership (`run` vs `request`) in a map, because one session may hold more than
  one Queue at once (already required by the idle-beat suite).
- The Grace timer.
- Run-scoped Beat timers and the Beat gate.
- The parked Blocking Tool set.
- The in-flight acquire join.
- Footer indicator state and timer.

A `SessionRegistry` maps session keys to lifecycles, answers `ownerFor(queueKey)` (the Holder, or
the newest session) for sessionless requests, and reports the last `forget`.

Dependencies are injected: the Extension builds a per-session `LeasePort` over `LeaseClient` (so
the module never resolves a Queue key, a tier, a group, or a session key), a `Clock` (timers at
the edge; unit tests move time exactly), a `UiPort`, a live `blockingTools` resolver and the
per-Queue Beat period. `blockingTools` defaults move to `DEFAULTS.blockingTools` in
`src/config.js`, read on every tool start so a hot-reload applies at once.

`withSlot(queueKey, signal, body)` owns the Grant handover the old `finally` did: the Beat probe,
joining the Run's in-flight acquire (#55), request-versus-Run lease ownership, stopping the Beat
at release, and re-arming Grace for a lone request. `queuedStream` shrinks to resolving the Queue
key and session, then calling `withSlot` with `streamSimple` as the body.

The pi handlers become one-line delegations. `SessionState` and its public fields are deleted.

## Considered options

- **Keep `SessionState`.** Rejected: the fields were the interface, and each recent bug was a
  caller writing one out of order. Moving the verbs inside makes the ordering the contract.
- **A daemon-side `runActive` flag.** Rejected by ADR 0003: the daemon would have to model pi's
  Run lifecycle, while the client already knows it.
- **Per-stream state instead of per-Run state.** Rejected: it cannot hold a Slot across tool
  executions, which is the KV-cache warmth the queue exists for.
- **Keep the handover in `queuedStream.finally`.** Rejected: it duplicated the Run's view with
  snapshots and was the source of #55.

## Consequences

- Handlers are one-liners; the lifecycle invariants now live in one module, with the order
  expressed by verbs rather than comments.
- Exact-time tests without `sleep` or prototype patching (`test/run-lifecycle.test.ts`).
- `ownerFor` owns sessionless attribution; the old `heldQueueKey` scan is gone.
- The existing pi seam suites remain the regression oracle for the Extension wiring.
- ADR 0002 (no preemption) and ADR 0003 (idle-yield) stand unchanged; the Beat gate keeps the
  gate-at-tick form, so `lastActivityAt` semantics do not move.
- Risk: `extension.ts` is the hottest file in the repo, so the refactor touches the pi seam. It
  lands green against the integration suites, not as a protocol change.
