# pi-llm-queue

Pi extension + local daemon that serialises LLM access to protect KV-cache
warmth on constrained local model servers.

## Merge policy

Squash only: merge commits and rebase merges are disabled on the remote. Merge
with `gh pr merge --squash`; the remote head branch is auto-deleted (repo
setting), so `--delete-branch` is not needed. Push straight to `main` is not a
path to integration.

`gh pr merge` is silent on a pipe: done when
`gh pr view <n> --json state -q .state` returns `MERGED`.

Work committed on local `main` moves to a branch before it reaches the remote:

    git switch -c <branch> && git push -u origin <branch>
    git branch -f main origin/main

After merging, clean up the local branch from `main`:

    git checkout main && git pull --prune
    git branch -D <branch>

Squash rewrites history, so the merged branch is never an ancestor of `main`:
`git branch -d` refuses, `-D` is the delete that works. Done when
`git branch --list <branch>` and `git branch --list -r <branch>` are both empty.

## Agent skills

### Issue tracker

GitHub Issues via `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Canonical five-role defaults. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `GLOSSARY.md` + `docs/adr/` at root. See `docs/agents/domain.md`.

### LLM queue

All LLM access goes through pi-llm-queue: one model slot, serialized. When delegating, run implementers one at a time and set child `timeoutMs` to include queue wait — a long queue wait is normal, not a hang.
