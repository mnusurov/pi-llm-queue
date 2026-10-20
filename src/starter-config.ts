// The Starter config transaction (ADR 0001, #94): one synchronous call owns reading the file,
// deciding create-or-reconcile, the pre-rename guard, the atomic write and the adoption re-read.
// The Extension maps the result to a notification and adopts the returned config.
import { readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { SCHEMA_VERSION, validateConfig } from "./config.js";
import { keyAddressesEndpoint, membersOf } from "./addressing.js";

interface QueueEntry {
	maxStreams?: number;
	enabled?: boolean;
	leaseTtlMs?: number;
	idleYieldMs?: number;
	match?: string[];
}

interface StarterConfig {
	schemaVersion: number;
	defaults: { queueTimeoutMs: number };
	queues: Record<string, { enabled: boolean }>;
}

/**
 * What the Starter transaction did. `created` and `reconciled` carry the validated re-read config
 * so the caller can adopt it; `untouched` reasons are benign no-writes and stay silent; `failed`
 * means the transaction could not complete and the caller should warn.
 */
export type StarterResult =
	| { status: "created"; config: Record<string, any> }
	| { status: "reconciled"; added: string[]; config: Record<string, any> }
	| { status: "untouched"; reason: "nothing-to-queue" | "nothing-to-add" | "rejected-config" | "raced" }
	| { status: "failed"; reason: "unreadable" | "unwritable" | "unstable" };

// The only defaults field a Starter config writes: a Parent's queue-wait cap, above a long Child
// generation (the 2026-10-18 run held the Slot 11+ minutes) so a stuck queue warns visibly while
// the normal case stays silent. Children keep an unbounded wait — this cap is parent-only.
const STARTER_QUEUE_TIMEOUT_MS = 120000;
const LOCAL_DOMAIN = /\.(local|lan|home\.arpa)$/;

/**
 * Self-hosted endpoint: loopback, RFC1918, link-local, or a local/mDNS domain. Public IPs and
 * other hostnames are cloud. A provider with no baseUrl (pure cloud API) is not self-hosted.
 */
function isLocalEndpoint(baseUrl?: string): boolean {
	if (!baseUrl) return false;
	let host: string;
	try {
		host = new URL(baseUrl).hostname.replace(/^\[|\]$/g, "");
	} catch {
		return false;
	}
	if (host === "localhost" || LOCAL_DOMAIN.test(host)) return true;
	const v4 = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.\d{1,3}$/.exec(host);
	if (v4) {
		const [a, b] = [Number(v4[1]), Number(v4[2])];
		return a === 127 || a === 10 || (a === 192 && b === 168) || (a === 172 && b >= 16 && b <= 31) || (a === 169 && b === 254);
	}
	return host === "::1";
}

/**
 * Starter config for a fresh install: one queue per endpoint baseUrl (deduplicated, sorted),
 * self-hosted enabled with the default single slot, cloud as passthrough, native-owned providers
 * excluded, and a `defaults` block carrying only the parent queue-wait cap. Null when there is
 * nothing to queue: no self-hosted, non-native endpoint.
 */
function buildStarterConfig(providers: Record<string, any>, isNativeOwned: (name: string) => boolean): StarterConfig | null {
	const collected: Record<string, { enabled: boolean }> = {};
	for (const [name, provider] of Object.entries(providers ?? {})) {
		const baseUrl = provider?.baseUrl;
		if (!baseUrl || baseUrl in collected || isNativeOwned(name)) continue;
		collected[baseUrl] = { enabled: isLocalEndpoint(baseUrl) };
	}
	const keys = Object.keys(collected).sort();
	if (!keys.some((key) => collected[key].enabled)) return null;
	const queues: Record<string, { enabled: boolean }> = {};
	for (const key of keys) queues[key] = collected[key];
	return { schemaVersion: SCHEMA_VERSION, defaults: { queueTimeoutMs: STARTER_QUEUE_TIMEOUT_MS }, queues };
}

/**
 * Queue keys a Starter config would add to an existing file: self-hosted endpoints served by a
 * non-native provider that no existing entry addresses. Add-only — a key that is already present,
 * tuned or `enabled: false`, is left alone, and a cloud endpoint is never added, so the file's own
 * choices always win and a deliberate passthrough stays a deliberate passthrough.
 */
function reconcileStarterKeys(
	providers: Record<string, any>,
	queues: Record<string, QueueEntry>,
	isNativeOwned: (name: string) => boolean,
): string[] {
	const keys = Object.keys(queues ?? {});
	const added = new Set<string>();
	for (const [name, provider] of Object.entries(providers ?? {})) {
		const baseUrl = provider?.baseUrl;
		if (!baseUrl || isNativeOwned(name) || !isLocalEndpoint(baseUrl)) continue;
		// An endpoint an entry already addresses — its own key, a provider-name key naming it, or a
		// Group's match — is queued (or disabled on purpose); adding the baseUrl key would split it
		// into a second queue and take the Slot away from the user's entry.
		const covered = keys.some((key) => membersOf(key, queues[key]).some((member) => keyAddressesEndpoint(member, providers, baseUrl)));
		if (!covered) added.add(baseUrl);
	}
	return [...added].sort();
}

type ReadResult = { ok: true; text: string | null } | { ok: false };
type WriteResult = "written" | "raced" | "unwritable" | "unreadable";

/** Read the config as text; a missing file is `null`, any other read failure is `unreadable`. */
function readText(path: string): ReadResult {
	try {
		return { ok: true, text: readFileSync(path, "utf8") };
	} catch (error) {
		return (error as NodeJS.ErrnoException)?.code === "ENOENT" ? { ok: true, text: null } : { ok: false };
	}
}

/** Parse through the schema; null means the file exists but must be left to the user. */
function parseConfig(text: string): Record<string, any> | null {
	try {
		const parsed = JSON.parse(text);
		return validateConfig(parsed).length ? null : parsed;
	} catch {
		return null;
	}
}

/**
 * Atomic write: tmp file, then rename. The guard re-reads the content the write is based on
 * (`expected`, null for a create) immediately before the rename, so a racing session_start or a
 * user edit wins and this write is dropped rather than clobbering it.
 */
function writeAtomically(configPath: string, expected: string | null, data: unknown): WriteResult {
	const tmp = `${configPath}.tmp-${process.pid}`;
	try {
		writeFileSync(tmp, `${JSON.stringify(data, null, 2)}\n`);
		const guard = readText(configPath);
		if (!guard.ok) return "unreadable";
		if (guard.text !== expected) return "raced";
		renameSync(tmp, configPath);
		return "written";
	} catch {
		return "unwritable";
	} finally {
		try {
			unlinkSync(tmp);
		} catch {}
	}
}

/** The result of a write that did not land: a lost race is benign, a read/write error is not. */
function writeFailure(write: Exclude<WriteResult, "written">): StarterResult {
	return write === "raced" ? { status: "untouched", reason: "raced" } : { status: "failed", reason: write };
}

/** The adopted config after a write; null when the required re-read is not a valid config. */
function adopt(configPath: string): Record<string, any> | null {
	const reread = readText(configPath);
	return reread.ok && reread.text !== null ? parseConfig(reread.text) : null;
}

/**
 * Create or add-only reconcile the Starter config at `configPath` (ADR 0001).
 *
 * `providers` is the already-read models.json catalog; `isNativeOwned` comes from pi's model
 * registry. Side effects: at most one atomic write. `created` and `reconciled` carry the validated
 * re-read config, so the caller queues the new keys in the same session.
 */
export function ensureStarterConfig({
	configPath,
	providers,
	isNativeOwned,
}: {
	configPath: string;
	providers: Record<string, any>;
	isNativeOwned: (name: string) => boolean;
}): StarterResult {
	const current = readText(configPath);
	if (!current.ok) return { status: "failed", reason: "unreadable" };

	if (current.text === null) {
		const starter = buildStarterConfig(providers, isNativeOwned);
		if (!starter) return { status: "untouched", reason: "nothing-to-queue" };
		const write = writeAtomically(configPath, null, starter);
		if (write !== "written") return writeFailure(write);
		const config = adopt(configPath);
		return config ? { status: "created", config } : { status: "failed", reason: "unstable" };
	}

	const parsed = parseConfig(current.text);
	// An invalid file is already reported by the config-rejected notify at session_start; writing
	// into it would bury that report, so reconcile leaves the file to the user.
	if (!parsed) return { status: "untouched", reason: "rejected-config" };
	const present = (parsed.queues ?? {}) as Record<string, QueueEntry>;
	const added = reconcileStarterKeys(providers, present, isNativeOwned);
	if (!added.length) return { status: "untouched", reason: "nothing-to-add" };
	// Add-only: existing entries and the `defaults` block are carried over untouched.
	const reconciled = {
		...parsed,
		queues: { ...present, ...Object.fromEntries(added.map((key) => [key, { enabled: true }])) },
	};
	const write = writeAtomically(configPath, current.text, reconciled);
	if (write !== "written") return writeFailure(write);
	const config = adopt(configPath);
	return config ? { status: "reconciled", added, config } : { status: "failed", reason: "unstable" };
}
