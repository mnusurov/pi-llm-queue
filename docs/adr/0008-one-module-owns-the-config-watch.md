# 8. One module owns the config watch

Date: 2026-10-21

## Status

Accepted (implements #105).

## Context

Both processes watch the same config file, and each had its own copy of the watch: the daemon
(`src/daemon.js`) and the Extension (`src/extension.ts`) each armed an `fs.watch` on the config's
directory, each debounced 250 ms, and each threw the watch's error away with `watcher.on("error",
() => {})`.

The thrown-away error is the defect. A watch is armed once per process and never re-armed, so a
watch that dies stays dead:

- a watch that *fails* reports an error, and both processes discarded it — the config stopped
  reloading and nobody could see it;
- a watch that dies *quietly* reports nothing at all: on darwin, a watch whose directory is renamed
  away delivers no further events and no failure. Measured, not assumed.

Either way the process serves a stale config for the rest of its life. Before #105 a stale config
decided how long a Lease lived — the client refreshed on its own copy of the TTL — so a dead watch
could hand a live Holder's Slot to a Waiter. #105 takes that away: the terms the daemon granted the
Lease under decide the refresh, so a stale config now degrades the provider and queue sets, which
the operator can see.

## Decision

One module owns the watch: `src/config-watch.js` (plain JavaScript, like the other daemon-loaded
modules — Node refuses type stripping under `node_modules`).

- It watches the config's *directory*, so an atomic save (an editor that replaces the file) keeps
  working, and a config created after startup is picked up too.
- It debounces (250 ms), so a save that writes the file twice is one edit.
- It reports a failed watch with its context and re-arms it.
- It replaces the watch on a tick (30 s), so a watch that died quietly is not left in place.
- The caller owns what a change means: the daemon re-applies the policy and rebuilds its queues,
  the Extension re-resolves its provider set. They report a bad config differently (stderr against
  `ui.notify`), so that stays with them.

The module takes the watcher factory as an injectable collaborator, so the suite drives the watch's
own lifecycle (a failed watch, a dead watch, a debounced burst) without a real file.

## Considered options

- **Keep the watch inline in both processes.** Rejected: the two copies had already drifted (one
  swallowed the error, one did not re-arm), and the rule that keeps a watch alive is exactly the
  kind of rule that must live in one place.
- **Re-arm only on an error event.** Rejected: a watch that dies quietly reports nothing, so the
  error path never runs. The replacement tick covers the quiet death.
- **Watch the file instead of its directory.** Rejected: an atomic save replaces the file and a
  file watcher goes dead with it.

## Consequences

- A stale config is now a visible annoyance (a provider set that lags an edit) instead of a silent
  decision about a Lease's life.
- The watch is replaced on a tick in both processes: one `close` and one `open` per tick, free next
  to the file reads it saves.
- The Extension reports a failed watch through `ui.notify`, the daemon through stderr (discarded
  when the Extension spawns it detached) — the same split as a rejected config.
