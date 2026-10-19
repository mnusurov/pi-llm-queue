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
Made by a voluntary release, an abort, a disconnect, a dead client's TTL, or a daemon-initiated
idle yield (ADR 0002 + ADR 0003).
_Avoid_: preemption, context switch

**Passthrough**:
A request streaming to the endpoint without holding a slot: queue disabled, or fail-open.
_Avoid_: bypass, fallback

**Yield**:
A daemon-initiated release of an idle Holder's Slot when Waiters are present and the Holder has
sent no keyed beat for `idleYieldMs`. The yielded client learns via its next beat (`granted:
false`) and re-queues. ADR 0003.
_Avoid_: preemption, eviction

### Lease and beat

**Lease**:
The timed right of a holder to keep its slot, renewed by beats and heartbeats; reclaimed by the
daemon when it stops being refreshed.
_Avoid_: lock, ticket

**Beat**:
A keyed lease refresh: the holder's report that it is using the slot. The daemon's only view of
stream activity, and its reply is the authoritative grant state. Keyed beats refresh
`lastActivityAt` for the whole live run — streaming and compute tools alike — and pause only
while the run is parked in a blocking tool, enabling idle-yield (ADR 0003 + amendment).
_Avoid_: ping, signal

**Heartbeat**:
A keyless lease refresh that keeps a live session's leases alive.
_Avoid_: keepalive, ping

**Blocking Tool**:
A tool that parks the run on another agent or the user (`subagent`, `bg_wait`, `ask_user`, …;
configurable via `defaults.blockingTools`). The run-scoped beat pauses while one is in flight, so
a parked Holder yields the Slot to the agent it waits on (ADR 0003 amendment).
_Avoid_: waiting tool, long tool

**Compute Tool**:
A tool that does work itself (`bash`, `read`, `edit`, …) — the complement of a Blocking Tool. The
run keeps beating through it: a compute-busy Holder is never idle.
_Avoid_: short tool, local tool

**In-flight**:
A request whose stream is running on the slot. The daemon cannot see the stream and tracks nothing
for it: a live holder is never switched away, so an in-flight request is safe by that rule.
_Avoid_: busy, active

**TTL**:
The lease's expiry horizon: a client that dies mid-stream stops refreshing and the slot is
reclaimed when the TTL lapses.
_Avoid_: timeout, expiry (as an event)

**Counted client**:
A connection the daemon counts toward its own lifetime, from its first state-touching op
(`acquire`, `renew`, `role`, `cancel`) until it closes or says `bye`: the daemon exits
`daemonIdleExitMs` (default 0 = at once) after the last one goes, so its lifetime is the union of
the live pi processes' (#60). A graceful pi says `bye` at `session_shutdown` (#63), so the exit
arms at the word, not at process death; a crash still counts as a close. A `status`-only
connection — an Observer — changes no queue state and is never counted, so polling cannot keep
stale code alive.
_Avoid_: client (unqualified), watcher, resident connection

**Observer**:
A reader of `status` (the `/queue` indicator, a dashboard, a monitoring script) that opens its own
connection and never touches queue state; invisible to the daemon's lifetime.
_Avoid_: poller, monitor

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
The daemon's periodic pass (`--sweep-ms`, default 1 s) that evicts leases whose TTL has lapsed,
whether or not anyone is waiting, so `/queue` never reports a ghost holder, then idle-yields an
activity-flagged lease silent past `idleYieldMs` when waiters are present (ADR 0003).
_Avoid_: GC, reaper

### Roles and config

**Parent**:
An interactive pi session; queues with a warn-only queue wait and a tier above children.
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
self-hosted endpoints, with a `defaults` block carrying only the Parent queue-wait cap. On a later
session start it reconciles add-only: a Queue key a self-hosted endpoint is missing is added, and
nothing already in the file is modified.
_Avoid_: default config, scaffold

**Fail-open**:
Queue behaviour when the daemon is unreachable: requests pass through unqueued (the default) or
are refused, per config.
_Avoid_: failover, degradation
