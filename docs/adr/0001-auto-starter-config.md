# 1. Auto-generate a starter config on first launch

Date: 2026-10-18

## Status

Accepted (implements #37 / #38).

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
- **Create-if-missing, never overwrite.** Write only when the file is absent; the create guard is
  re-checked immediately before an atomic rename, so a racing first launch is last-writer-wins
  with identical content. An existing file is never modified.
- **Shape.** `schemaVersion: 1`, one endpoint-keyed `queues` entry per baseUrl (deduplicated,
  sorted), no `defaults` block.
- **Notify.** Surface the created path and the queued-endpoint count through `ui.notify` so the
  user knows a file appeared and where to tune it.
- **Stable disable path.** Set every key to `enabled: false`. Deleting the file is not a permanent
  disable while at least one self-hosted, non-native endpoint exists — it regenerates on the next
  launch when absent (with no such endpoint, the starter returns null and a deleted file stays gone).

## Consequences

- Fresh installs queue their local endpoints with zero config; the common case needs no setup.
- The generated file is a real file the user now owns and edits; the auto-starter never touches it
  again, so manual tuning is stable.
- "Disable" is a config edit (`enabled: false`), not a file deletion; the README documents this.
- Detection is host-based: a self-hosted server reached by a public IP or a non-local hostname is
  classified as cloud and left as passthrough — flip `enabled: true` to queue it.
