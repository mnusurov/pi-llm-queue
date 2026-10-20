# Coding standards

Rules for new code. Structural decisions live in `docs/adr/`; when this file and an ADR
disagree, the ADR wins. Domain terms live in `GLOSSARY.md`.

## Language and imports

- Daemon-loaded modules under `src/` stay JavaScript (`.js`). Node refuses type stripping under
  `node_modules`, so an installed daemon must be able to import them; add no TypeScript syntax or
  `.ts` imports to that path.
- Extension code is TypeScript (`.ts`). Relative imports name the real file extension: `.js` for
  JavaScript, `.ts` for TypeScript.
- Keep the package dependency-free at runtime. Dev and peer dependencies only.
- Import Node built-ins statically with the `node:` prefix.

## Testing

- Drive behavior through a public seam: an exported API, the daemon protocol, or a harness. Test
  what the system does, not how it does it.
- Mock system boundaries only — network, filesystem, time, external SDK. Use the real modules for
  our own code.
- Observe behavior through the seam or an injected collaborator. Do not patch a class or module
  prototype to spy on it.
- Put clocks and side effects at the edge. Pure decision code takes `now` as an argument and
  returns decisions; the adapter emits them (ADR 0004). Tests move `now` and read decisions.
- TDD in vertical slices: one failing test, one implementation, repeat. Refactor only after green.
- Name the behavior in the test title. One logical assertion per case.
- Land a bug fix with the regression test; prove it red before making it green.
- Wait for presence by polling with a deadline; sleep only to prove absence.
- Harnesses own cleanup: close sockets and stop daemons in `after` hooks.

## Design

- Prefer deep modules: small interface, complexity inside. If callers must know an internal
  invariant, move the invariant inside the module.
- Pass dependencies in and return results. Side effects live at the edge.
- One module owns a policy. Call it rather than re-deriving its rule elsewhere (ADR 0005).
- Keep test-only overrides out of public types. Inject config or a collaborator instead.
- Scrutinize optional parameters: every optional field is a branch. Add one only when absence has
  meaning; centralize defaults in `src/config.js`.
- A daemon flag and its config field change together.
- Public exports get a JSDoc comment: purpose, contract, and any non-obvious failure mode.
- Comments explain why, not what.

## Errors and protocol

- Throw at the boundary with context: what failed and why. Never turn a real failure into silence;
  fail-open behavior must be explicit and logged.
- Wire messages are a public contract: change the code and `docs/daemon-protocol.md` in the same
  change.

## Documentation

- A term: `GLOSSARY.md`. A structural or behavioral choice: an ADR. A protocol fact:
  `docs/daemon-protocol.md`.
- Follow `docs/agents/domain.md` when deciding where a fact belongs.
