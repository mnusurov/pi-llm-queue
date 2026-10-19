# 1. Auto-generate a starter config on first launch

Date: 2026-10-18

## Status

Accepted (implements #37 / #38). Amended by #46 (spec #43) in two clauses: the Starter config
writes a `defaults` block carrying the parent queue-wait cap, and "create-if-missing, never
overwrite" becomes add-only reconcile of a missing Queue key.

## Context

A fresh install shipped with `~/.pi/agent/llm-queue.json` absent, and a missing config queues
nothing — so the queue was inert until the user hand-wrote a config. On a constrained local model
server the whole point is to serialise access, so an inert default defeats the tool for the users
who need it most. We want the queue live from the very first session without asking the user to
author a config.

## Decision

On the first `session_start` after install, when the config file is absent, generate a starter
from the user's own `~/.pi/agent/models.json`:

- **Override the safe default.** Absent config no longer means "queues nothing"; it means "queue
  the self-hosted endpoints." The queue is live from the first session.
- **Self-hosted detection scope.** A baseUrl is self-hosted when its host is loopback
  (`localhost`, `127.x`, `::1`), RFC1918 (`10.x`, `192.168.x`, `172.16–31.x`), link-local
  (`169.254.x`), or a local/mDNS domain (`*.local`, `*.lan`, `*.home.arpa`). Everything else is
  cloud.
- **Cloud passthrough.** Cloud endpoints (public IPs, other hostnames) are written `enabled: false`
  — fully parallel, untouched. Queueing a shared cloud API has no KV-cache benefit and would only
  add latency, so the starter queues only what the cache-warmth goal actually targets. Providers
  with no baseUrl (pure cloud APIs) are omitted from the file entirely — no key, so they stay
  unwrapped.
- **Native exclusion.** Providers owned by native extensions are excluded from the starter.
- **Create-if-missing, then add-only reconcile.** Write a starter only when the file is absent; the
  create guard is re-checked immediately before an atomic rename, so a racing first launch is
  last-writer-wins with identical content. On every later `session_start` an existing file is
  reconciled: each self-hosted, non-native endpoint that no existing entry addresses gains its Queue
  key as `enabled: true`, and nothing already in the file is modified — not a tuned entry, not an
  explicit `enabled: false`, not the `defaults` block. An endpoint an entry already addresses (its
  own key, a provider-name key naming it, or a Group's `match`) counts as addressed, so reconcile
  never splits a queue in two. A config the schema rejects is left alone: its rejection is already
  reported at session start, and writing into it would bury that report.
- **Shape.** `schemaVersion: 1`, one endpoint-keyed `queues` entry per baseUrl (deduplicated,
  sorted), and a `defaults` block containing only the parent queue-wait cap `queueTimeoutMs` (120 s).
  Nothing else in `defaults`: every other field stays absent so the schema's own defaults apply, and
  the generated file names only what the generator has an opinion about.
- **Notify.** Surface the created path and the queued-endpoint count through `ui.notify`, and on
  reconcile name the endpoints that were added, so a file the extension changed is never a surprise.
- **Stable disable path.** Set every key to `enabled: false`. Deleting the file is not a permanent
  disable while at least one self-hosted, non-native endpoint exists — it regenerates when absent —
  and deleting a single key is not one either, because reconcile re-adds it. `enabled: false` is the
  disable that survives every restart: reconcile reads it as a deliberate passthrough and never
  touches it.

## Considered alternative: derive defaults at resolve time

The parent queue-wait cap could be applied in the client whenever `defaults.queueTimeoutMs` is
absent, leaving a generated file with no `defaults` block, and reconcile could be dropped entirely
by resolving an endpoint's queue at read time. Rejected: the config file is the user's contract and
the daemon reads the same file, so a value that lives only in code is invisible to the file's owner,
needs a second place to tune it per install, and risks the extension and the daemon disagreeing
about the same endpoint. Keeping one visible `defaults` field makes the file the single source of
truth; the price is that the generator writes one more block and reconcile must never touch it.

## Consequences

- Fresh installs queue their local endpoints with zero config; the common case needs no setup.
- The generated file is a real file the user now owns and edits: reconcile only ever adds a key that
  is missing, so manual tuning and deliberate passthroughs stay stable, and an install whose file
  predates the starter (hand-written, or generated then edited) no longer leaves an endpoint
  completely unwrapped — no serialisation, no KV-cache protection, nothing in `/queue`.
- A Parent's queue wait is bounded by a visible warning at 120 s: a stuck queue surfaces as a
  notify instead of a silent wait behind a long Child generation. Children keep an unbounded wait.
- "Disable" is a config edit (`enabled: false`), not a key deletion; the README documents this.
- Every file change the extension makes is notified, naming the endpoints it added.
- Detection is host-based: a self-hosted server reached by a public IP or a non-local hostname is
  classified as cloud and left as passthrough — flip `enabled: true` to queue it. Reconcile never
  adds such an endpoint, so a hand-written file keeps its cloud passthroughs untouched.
