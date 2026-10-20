# 6. The pi seam is tested through a harness, not a factory

Date: 2026-10-20

## Status

Accepted (issue #88). Test-only: `src/extension.ts` keeps its module-level agent-dir paths and its
default export; the production boot path is unchanged.

## Context

The Extension's interface toward pi is "react to events, register a provider, expose `/queue`".
Eight test files crossed it instead: **137** direct reads of `handlers[...]` / `registered[...]` /
`commands[...]`, **15** copies of `makePi`/`makeCtx`, **46** lines rebuilding the same agent dir,
and **8** `pkill -f <socketPath>` cleanups. A second `pi.on(...)` handler on an event breaks every
file that assumes `[0]`.

The obvious alternative was to invert the dependency: export `createExtension({ agentDir })` from
`src`, with the default export as `createExtension()`, and let tests inject paths. That is a real
seam, but it serves one consumer — tests — while changing how the package boots. `PI_CODING_AGENT_DIR`
is already the documented agent-dir contract, and `node --test` runs each file in its own process,
so the import-time read is safe and per-file isolation comes for free.

## Decision

The harness lives in `test/extension-harness.ts` (test-only, like `test/harness.ts`; the package
ships only `src`). It sets `PI_CODING_AGENT_DIR`, writes the config and models fixtures, dynamically
imports the Extension, and wraps a pi stub that records handlers, providers and commands. Tests
drive it by name — dispatch an event, take the wrapped stream for a model, read notifications and
statuses, dispose — and never index a handler array or the provider registry.

The surface has two levels:

- `startExtension({ config, models, env?, daemon? })` — one per `extension(pi)` call. `daemon`
  defaults to spawning a daemon on the harness's agent dir; `false` leaves the Extension to
  autostart one, which the fail-open and lifecycle suites need. `env` is applied around the
  registration call (where the Extension reads `PI_QUEUE_ROLE` / `PI_QUEUE_GROUP`) and restored on
  dispose.
- `startSession(sessionId, { model?, signal? })` — the per-session handle. It dispatches
  `session_start` and owns the `ctx` it was built with: `dispatch(event, payload?)`,
  `stream(model)`, `notifications()`, `statuses()`, `command(args)`.

There is no `ext.dispatch(event, ctx)`. A pi event always arrives with a session's `ctx`, so the
session handle is the only entry that does not ask the test to build one; a flat dispatcher would
reintroduce exactly the `ctx` construction being removed. `providers()` is read-only, for the
"nothing was registered" assertions.

## Considered options

- **`createExtension({ agentDir })` in `src`.** Rejected for now: an injection port with one
  consumer, on the production boot path. Promote it only when a test genuinely needs two agent dirs
  in one process.
- **Keep the per-file `makePi`/`makeCtx` fixtures.** Rejected: the seven `makeCtx` variants already
  disagree, and the duplicated agent-dir ritual is where the hermeticity bug (#59) came from.
- **Kill autostarted daemons by pid.** Rejected without a `LeaseClient` change: the child is
  detached, `unref`'d and exposes no handle. The socket path is unique per temp dir, so an
  endpoint-scoped match is unambiguous.

## Consequences

- The pi seam has one test surface: a new `pi.on(...)` handler requires no test change.
- One agent dir per test file. A second Extension instance in the same file remains independent —
  its state is per `extension(pi)` call, not module-level; only the agent dir is shared.
- A suite imports one module: the harness re-exports the socket primitives it builds on
  (`startDaemon`, `connectClient`, `stopAllDaemons`, `closeAllClients`, `pollUntil`).
- `pkill -f <socketPath>` stays for daemons the Extension autostarted, but lives in one `dispose()`
  instead of eight `after` hooks.
- `GLOSSARY.md` gains **Extension** and **Adapter**; the harness itself stays out — it is test
  tooling, not domain language.
