# Architecture decision records

One record per structural or behavioural choice. Read the one that touches your area — the table
says which.

Records are immutable: a decision that changes is amended in place, dated, or superseded by a new
record. Never rewrite the old decision.

| ADR | Decides | Read it when |
| --- | --- | --- |
| [0001 Auto-generate a starter config on first launch](0001-auto-starter-config.md) | the config file's schema, and the starter config generated from the user's own `models.json` on first launch | the config file, its schema, `schemaVersion`, what a fresh install writes |
| [0002 No preemption: a live holder keeps the slot until it releases it](0002-no-preemption.md) | when a Slot changes hands — voluntary release, abort, disconnect, TTL lapse, and nothing else | the handover rules, what may take a Slot away, the `evict` event |
| [0003 Idle Holder yields under contention](0003-idle-yield.md) | the idle-yield of an activity-flagged Holder, the keyed Beat and its period, the Blocking Tool gate | idle yield, beat periods, parked tools |
| [0004 The Scheduler decides; the socket adapter emits](0004-scheduler-decides-socket-adapter-emits.md) | the daemon's two halves: the Scheduler decides, the Adapter writes the socket and the log | daemon structure, where a decision becomes an effect, the decision list |
| [0005 One module owns queue addressing](0005-one-module-owns-queue-addressing.md) | the addressing rules in both directions, and the module that owns them | queue keys, groups, provider wrapping, which module owns a rule |
| [0006 The pi seam is tested through a harness, not a factory](0006-pi-seam-tested-through-a-harness.md) | how the pi seam is tested: a test-only harness at the Extension's interface | writing an Extension test, the harness, the agent dir |
| [0007 One client-side module owns the Run lifecycle](0007-run-lifecycle-module.md) | the Run's state, its lease ownership, the Grace and Beat timers, the parked Blocking Tool set | the Run lifecycle, Grace, the session registry, Extension-side state |

Amended records — read the amendment, not only the original decision:

- **0001** amended by #46: the starter config writes a `defaults` block, and create-if-missing
  becomes add-only reconcile.
- **0002** refined by 0003: an idle-yield adds a fifth cause of a Switch.
- **0003** amended twice: 2026-10-19 (#57) — idle is "no keyed beat", not "between streams";
  2026-10-20 (#81) — the beat period follows the queue's own policy.
- **0004** amended 2026-10-20 (#100) — the whole decision batch is logged before any reply of that
  batch goes out.

The next record takes the next number. Where a fact belongs: [`docs/agents/domain.md`](../agents/domain.md).
The vocabulary the records use: [`GLOSSARY.md`](../../GLOSSARY.md).
