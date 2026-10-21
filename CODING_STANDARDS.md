# Coding standards

Rules for all code in this repository. Structural decisions live in `docs/adr/`; when this file and an ADR
disagree, the ADR wins. Domain terms live in `GLOSSARY.md`.

## Language and imports

- Daemon-loaded modules under `src/` stay JavaScript (`.js`). Node refuses type stripping under
  `node_modules`, so an installed daemon must be able to import them; add no TypeScript syntax or
  `.ts` imports to that path.
- The JavaScript under `src/` is type-checked: `tsconfig.json` runs `checkJs` over the whole program,
  and every module in it — the daemon entry point included — is checked without an opt-in. A new
  JavaScript module is checked the moment it is written; a file that no one imports is checked
  because the program covers `src/`, not because someone imported it.
- The JavaScript sources are held to the same `strict` rules as the TypeScript ones. Their types are
  written as JSDoc: every parameter, every return and every shared record's shape carries a type.
  A type that says it all needs no prose beside it.
- `@ts-expect-error` is allowed when a suppression is the honest answer, and every one of them
  carries a comment saying why the suppression is needed. `@ts-ignore` is not allowed anywhere: it
  silences a check forever, and a check that is wrong should be fixed, not muted.
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
  meaning.
- A default's home follows where its value comes from. A config default — the value the code
  substitutes for a field the config file omits — lives in `src/config.js`: the `DEFAULTS` policy
  table, or the schema's own tables for the fields outside the policy. A behaviour constant — a
  window, a cap or a period the code owns whatever the config says, like the Run's Grace or the
  100 KB line cap — lives beside the code that owns it, not in the config table. A value the code
  writes into a config file is the generator's opinion and belongs to the module that writes it
  (ADR 0001: the Starter writes the parent queue-wait cap).
- A daemon flag and its config field change together.

## Comments

- A comment earns its place only when the why is not obvious from the code: the constraint that
  forced this shape, the alternative that failed, or what a later change would break. That is the
  context an agent in a new session needs so it does not guess and "fix" deliberate behavior.
- Write code so the what is visible; reserve comments for the why. A comment that narrates the
  next line rots as soon as the code moves, and a stale comment is worse than none.
- Update or delete a comment in the same change that invalidates it.
- Public exports get JSDoc for the contract callers need — purpose, inputs, and failure modes — not
  a play-by-play of the implementation. The check (`npm run check:jsdoc`) enforces that the block
  exists; whether the contract says the right thing is the reviewer's call.

## Errors and protocol

- Throw at the boundary with context: what failed and why. Never turn a real failure into silence;
  fail-open behavior must be explicit and logged.
- Wire messages are a public contract: change the code and `docs/daemon-protocol.md` in the same
  change.

## Documentation

- A term: `GLOSSARY.md`. A structural or behavioral choice: an ADR. A protocol fact:
  `docs/daemon-protocol.md`.
- Follow `docs/agents/domain.md` when deciding where a fact belongs.
