# pi-llm-queue

Pi extension + local daemon that serialises LLM access to protect KV-cache
warmth on constrained local model servers.

## Merge policy

Integration is a PR merged with `gh pr merge --squash`: squash only, and pushing straight to
`main` is not a path — `main` is protected, so a direct push is refused, admins included.
`gh pr merge` is silent on a pipe: done when
`gh pr view <n> --json state -q .state` returns `MERGED`.

Branch-off from local `main`, and deleting the merged branch: `docs/agents/merge-policy.md`.

## Coding standards

Rules for all code — JS/TS split, test seams, design, comments, errors, and docs: `CODING_STANDARDS.md`.

## Checks

`npm run check` (typecheck + the public-export contract check + the doc check) and `npm test` (the
suite) both run in CI on node 22 and 24: `.github/workflows/ci.yml`. Both must be green before a
commit.

## Agent skills

### Issue tracker

GitHub Issues via `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Canonical five-role defaults. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `GLOSSARY.md` + `docs/adr/` at root (index: `docs/adr/README.md`). See `docs/agents/domain.md`.

### LLM queue

All LLM access goes through pi-llm-queue: one model slot, serialized. When delegating, run implementers one at a time and set child `timeoutMs` to include queue wait — a long queue wait is normal, not a hang.
