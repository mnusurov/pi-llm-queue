// The one answer shape for every lease decision (#96). A verdict says what happened to a lease, so
// each verb returns only the subset it can produce: acquire → held | passthrough | cancelled;
// beat → held | lost | passthrough. An unreachable daemon is not a verdict: with failOpen: false it
// is a DaemonUnreachableError, because a refusal a caller can ignore is the wrong signal.
//
// Type-only: the Run lifecycle imports this without importing LeaseClient (ADR 0007), and the lease
// client imports it without handing the lifecycle a client-shaped answer.
export type LeaseVerdict =
	| { kind: "held" }
	| { kind: "passthrough" }
	| { kind: "lost" }
	| { kind: "cancelled" };

/** The subset each verb can produce: an acquire never loses a slot it holds, a beat is never cancelled. */
export type AcquireVerdict = Extract<LeaseVerdict, { kind: "held" | "passthrough" | "cancelled" }>;
export type BeatVerdict = Extract<LeaseVerdict, { kind: "held" | "lost" | "passthrough" }>;
