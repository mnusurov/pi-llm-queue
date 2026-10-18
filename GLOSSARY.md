# pi-llm-queue

Local queueing of LLM requests to a self-hosted model endpoint, so that one session at a time
holds the endpoint and its KV cache stays warm.

## Language

### Queue and slot

**Queue**:
A per-endpoint admission lane with a fixed number of slots; requests for that endpoint wait in
it until a slot is granted.
_Avoid_: pool, channel

**Slot**:
The right to stream to one endpoint at one moment; a queue has `maxStreams` of them.
_Avoid_: token, permit

**Holder**:
The session currently occupying a slot in a queue.
_Avoid_: owner, occupant

**Waiter**:
A session whose acquire request is parked in the queue's waiting list because no slot was free.
_Avoid_: queued client, pending request

**Grant**:
The daemon's answer that a slot is now the requester's; the only reply an `acquire` may delay.
_Avoid_: approval, assign

**Switch**:
One handover of a slot from one session to another — the event that re-prefills the KV cache.
Made only by a voluntary release, an abort, a disconnect, or a dead client's TTL (ADR 0002).
_Avoid_: preemption, context switch

**Passthrough**:
A request streaming to the endpoint without holding a slot: queue disabled, fail-open, or a
parent whose queue wait timed out.
_Avoid_: bypass, fallback

### Lease and beat

**Lease**:
The timed right of a holder to keep its slot, renewed by beats and heartbeats; reclaimed by the
daemon when it stops being refreshed.
_Avoid_: lock, ticket

**Beat**:
A keyed lease refresh: the holder's report that it is using the slot. The daemon's only view of
stream activity, and its reply is the authoritative grant state.
_Avoid_: ping, signal

**Heartbeat**:
A keyless lease refresh that keeps a live session's leases alive.
_Avoid_: keepalive, ping

**In-flight**:
A request whose stream is running on the slot. The daemon cannot see the stream and tracks nothing
for it: a live holder is never switched away, so an in-flight request is safe by that rule.
_Avoid_: busy, active

**TTL**:
The lease's expiry horizon: a client that dies mid-stream stops refreshing and the slot is
reclaimed when the TTL lapses.
_Avoid_: timeout, expiry (as an event)

### Scheduler

**Tier**:
A requester's priority class — `parent` (interactive) above `child` (subagent) — that orders the
waiting list.
_Avoid_: priority, role (use for the client-side concept)

**Aging**:
Anti-starvation: the longest-waiting child jumps the tier order once it has waited long enough
or enough parents have been granted in a row.
_Avoid_: starvation boost, promotion

**Grace**:
The short window after a run settles during which a new run by the same session keeps the slot
without a switch.
_Avoid_: cooldown, buffer

**Sweep**:
The daemon's periodic pass (`--sweep-ms`, default 1 s) that evicts leases whose TTL has lapsed, whether or not anyone is
waiting, so `/queue` never reports a ghost holder.
_Avoid_: GC, reaper

### Roles and config

**Parent**:
An interactive pi session; queues with a bounded queue wait and a tier above children.
_Avoid_: interactive session, main session

**Child**:
A subagent session; queues with an unbounded queue wait so it never streams unqueued.
_Avoid_: sub-agent, worker

**Queue key**:
The config name addressing a queue — an endpoint URL, a provider name, or a group's member keys.
_Avoid_: endpoint, target

**Group**:
Named config members that all address one queue, so several providers share one slot set.
_Avoid_: alias set, bundle

**Starter config**:
The config a fresh install auto-generates from the user's own model catalog, queueing only
self-hosted endpoints.
_Avoid_: default config, scaffold

**Fail-open**:
Queue behaviour when the daemon is unreachable: requests pass through unqueued (the default) or
are refused, per config.
_Avoid_: failover, degradation
