# Issue tracker: Local Markdown

Issues and PRDs for this repo live as markdown files in `docs/issues/` (exported from GitHub `mnusurov/pi-llm-queue`; the GitHub tracker is **read-only** for this copy — never create/comment/close there).

## Conventions

- **Read an issue**: `docs/issues/<number>.md`.
- **List issues**: `ls docs/issues/` — each file starts with `# Issue <number>: <title>` and a `State:` / `Labels:` line.
- **Create a new issue**: write `docs/issues/<number>.md` with the same header format.
- **Comment on an issue**: append a `## Comments` section to the issue file.
- **Apply / remove labels**: edit the `Labels:` line.
- **Close**: set `State: CLOSED` in the header.

## Pull requests as a triage surface

**PRs as a request surface: no.**

## When a skill says "publish to the issue tracker"

Create a markdown file in `docs/issues/`.

## When a skill says "fetch the relevant ticket"

Read `docs/issues/<number>.md`.

## Wayfinding operations

Used by `/wayfinding`. The **map** is `docs/issues/map.md`, holding the Notes / Decisions-so-far / Fog body; each child ticket is its own file with `Part of map` at the top of the body.

- **Frontier query**: list open ticket files (header `State: OPEN`), drop any with an open blocker (listed in its `Blocked by` section) or an assignee; first in map order wins.
- **Claim**: set `Assignee:` in the ticket header to the driving dev.
- **Resolve**: append the answer to `## Comments`, set `State: CLOSED`, then append a context pointer (gist + file path) to the map's Decisions-so-far.
