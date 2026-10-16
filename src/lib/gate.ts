/**
 * Lease gate: the seam between a model request and the daemon scheduler.
 *
 * A session holds one lease per queue for a whole run (turn-based affinity),
 * so consecutive requests reuse the slot instead of re-queuing.
 */
import type { LeaseClient, LeaseGrant, Role } from "./lease-client.ts";
import type { QueueSettings } from "./config.ts";

export interface SessionLeaseState {
	sessionKey: string;
	role: Role;
	group?: string;
	/** Lease currently held by this session, with the queue it was granted on. */
	leaseId?: string;
	queueKey?: string;
	heartbeat?: NodeJS.Timeout;
}

/**
 * Role resolution: explicit `PI_QUEUE_ROLE` wins, then the pi-subagents
 * child hint (read as an env string: no import, no hard dependency),
 * then default parent (interactive).
 */
export function resolveRole(env: NodeJS.ProcessEnv = process.env): Role {
	const explicit = (env.PI_QUEUE_ROLE ?? "").trim().toLowerCase();
	if (explicit === "child" || explicit === "parent") return explicit;
	if ((env.PI_SUBAGENT_CHILD ?? "").trim() && (env.PI_SUBAGENT_CHILD ?? "").trim() !== "0") return "child";
	return "parent";
}

export function resolveGroup(env: NodeJS.ProcessEnv = process.env): string | undefined {
	const group = (env.PI_QUEUE_GROUP ?? "").trim();
	return group ? group : undefined;
}

export function hasLease(state: SessionLeaseState, queueKey: string): boolean {
	return !!state.leaseId && state.queueKey === queueKey;
}

export interface GateDeps {
	client: LeaseClient;
	state: SessionLeaseState;
	settings: QueueSettings;
	queueKey: string;
	onWarn?: (message: string) => void;
}

/**
 * Wait until this session may issue a model request on `queueKey`.
 * Never rejects: daemon failure, timeout, and abort all resolve to a
 * passthrough grant so a run is never hard-failed by the queue.
 */
export async function acquireForRequest(deps: GateDeps, signal?: AbortSignal): Promise<LeaseGrant> {
	const { client, state, settings, queueKey } = deps;
	if (hasLease(state, queueKey)) {
		const renewed = await client.renew(state.leaseId!);
		if (renewed) return { granted: true, leaseId: state.leaseId, position: 0 };
		// Lease was reclaimed (TTL, daemon restart, disconnect): re-queue.
		state.leaseId = undefined;
		state.queueKey = undefined;
	}
	const grant = await client.acquire({
		queueKey,
		sessionKey: state.sessionKey,
		role: state.role,
		group: state.group,
		signal,
		timeoutMs: settings.queueTimeoutMs,
	});
	if (grant.granted && grant.leaseId) {
		state.leaseId = grant.leaseId;
		state.queueKey = queueKey;
	}
	if (grant.failOpen && grant.timedOut) {
		deps.onWarn?.(`llm-queue: waited ${settings.queueTimeoutMs}ms for ${queueKey}, passing through unqueued`);
	}
	return grant;
}

export async function releaseLease(deps: GateDeps | { client: LeaseClient; state: SessionLeaseState }): Promise<void> {
	const { client, state } = deps;
	stopHeartbeat(state);
	const leaseId = state.leaseId;
	state.leaseId = undefined;
	state.queueKey = undefined;
	if (leaseId) await client.release(leaseId);
}

export function startHeartbeat(state: SessionLeaseState, client: LeaseClient, intervalMs: number): void {
	stopHeartbeat(state);
	if (!state.leaseId || intervalMs <= 0) return;
	state.heartbeat = setInterval(() => {
		if (!state.leaseId) {
			stopHeartbeat(state);
			return;
		}
		void client.renew(state.leaseId);
	}, intervalMs);
	state.heartbeat.unref?.();
}

export function stopHeartbeat(state: SessionLeaseState): void {
	if (state.heartbeat) {
		clearInterval(state.heartbeat);
		state.heartbeat = undefined;
	}
}
