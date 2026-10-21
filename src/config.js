// Versioned schema for ~/.pi/agent/llm-queue.json. validateConfig returns clear, path-named
// errors ([] = usable); loadValidConfig reads a file through it and falls back to defaults on
// any failure. Plain JS on purpose: the daemon imports it, and Node's built-in type stripping
// is refused for files under node_modules, so a .ts import would kill an installed daemon.
import { readFileSync } from "node:fs";

/**
 * The config schema version this build writes. A file declaring a higher version is refused:
 * validateConfig names it and loadValidConfig falls back to DEFAULTS. A lower or absent version
 * is read as it stands.
 */
export const SCHEMA_VERSION = 1;

/**
 * The policy defaults table: the one place these values are written. resolveQueuePolicy applies
 * it as the cascade's last rung, and the test harnesses read it. Not members: the lease client's
 * 10000 ms fallback and the daemon's idle-exit startup floor — fallbacks of other layers, not
 * policy defaults.
 * @type {Record<string, any>}
 */
export const DEFAULTS = {
	maxStreams: 1,
	leaseTtlMs: 30000,
	agingAfterSlots: 3,
	agingAfterMs: 60000,
	policy: "priority-aging",
	idleYieldMs: 30000,
	// Tools that park the Run on another agent or the user: the client pauses the Run's keyed Beat
	// while one is in flight, so a parked Holder idle-yields the Slot to the agent it waits on
	// (#57, ADR 0003). Compute tools (bash, read, edit, …) keep beating: a busy Holder is never idle.
	blockingTools: ["subagent", "subagent_wait", "bg_wait", "ask_user", "contact_supervisor", "intercom"],
};

/**
 * The one policy cascade: queue entry field → defaults field → DEFAULTS. The daemon's adapter and
 * the client's beat derivation both call this, so they agree on every Policy default by
 * construction, not by comment (#98; the drift that lost a beating Holder its Slot, #81, was two
 * hand-written cascades disagreeing). The resolver invents no values: it applies only the
 * documented fallbacks in DEFAULTS, plus the Passthrough rule (`enabled: false` → maxStreams
 * Infinity). Returns config vocabulary (`leaseTtlMs`); the daemon adapter maps it to the
 * Scheduler's `ttlMs` itself.
 * @param {Record<string, any>} entry A queue entry ({} when none addresses the key)
 * @param {Record<string, any>} defaults The config's `defaults` block
 * @returns {{ maxStreams: number, leaseTtlMs: number, agingAfterSlots: number, agingAfterMs: number, policy: string, idleYieldMs: number }}
 */
export function resolveQueuePolicy(entry, defaults) {
	const pick = (/** @type {string} */ field) => entry[field] ?? defaults[field] ?? DEFAULTS[field];
	return {
		maxStreams: entry.enabled === false ? Infinity : pick("maxStreams"),
		leaseTtlMs: pick("leaseTtlMs"),
		agingAfterSlots: pick("agingAfterSlots"),
		agingAfterMs: pick("agingAfterMs"),
		policy: pick("policy"),
		idleYieldMs: pick("idleYieldMs"),
	};
}

/**
 * @typedef {object} FieldSpec
 * @property {"posInt"|"nonNegInt"|"posNum"|"nonNegNum"|"bool"|"strings"|"enum"|"path"} kind
 * @property {string[]} [values]
 * @property {number} [min] Accepted floor for a number field (inclusive).
 * @property {boolean} [defaultsOnly] Valid in `defaults` only; a per-queue entry with it is rejected.
 * @property {boolean} [queuesOnly] Valid in a queue entry only; `defaults` never reads it.
 */

/** @type {Record<string, FieldSpec>} */
const FIELDS = {
	maxStreams: { kind: "posInt" },
	policy: { kind: "enum", values: ["priority-aging", "fifo"] },
	// Floor: the client heartbeats at max(250, leaseTtlMs/3), and the sweep runs every second.
	// A shorter TTL means a heartbeating session loses its slot to a waiter over and over.
	leaseTtlMs: { kind: "posNum", min: 1000 },
	agingAfterSlots: { kind: "nonNegInt" },
	agingAfterMs: { kind: "nonNegNum" },
	enabled: { kind: "bool", queuesOnly: true },
	match: { kind: "strings", queuesOnly: true },
	idleYieldMs: { kind: "nonNegNum" },
	// #57: tools that park the run on another agent or the user. The client pauses its run-scoped
	// keyed beat while one is in flight, so a parked Holder idle-yields. Client-side: defaults only.
	blockingTools: { kind: "strings", defaultsOnly: true },
	failOpen: { kind: "bool", defaultsOnly: true },
	queueTimeoutMs: { kind: "posNum", defaultsOnly: true },
	daemonIdleExitMs: { kind: "nonNegNum", defaultsOnly: true },
	// The daemon's event log path (#47): a daemon-wide file, never a per-queue one.
	logFile: { kind: "path", defaultsOnly: true },
};

const DESC = {
	posInt: "a positive integer",
	nonNegInt: "an integer >= 0",
	posNum: "a number > 0",
	nonNegNum: "a number >= 0",
	bool: "true or false",
	strings: "an array of strings",
	path: "a non-empty string path",
};

const REJECT = "the whole config is rejected and defaults are used";

/** @param {unknown} v */
const isObj = (v) => typeof v === "object" && v !== null && !Array.isArray(v);

/**
 * @param {string} path
 * @param {unknown} value
 * @param {FieldSpec} spec
 * @returns {string | null}
 */
function checkValue(path, value, spec) {
	const bad = (/** @type {string} */ desc) => `${path} must be ${desc} (got ${JSON.stringify(value)})`;
	switch (spec.kind) {
		case "posInt":
			return typeof value === "number" && Number.isInteger(value) && value >= 1 ? null : bad(DESC.posInt);
		case "nonNegInt":
			return typeof value === "number" && Number.isInteger(value) && value >= 0 ? null : bad(DESC.nonNegInt);
		case "posNum": {
			const floor = spec.min ?? 0;
			const desc = spec.min === undefined ? DESC.posNum : `a number >= ${spec.min}`;
			return typeof value === "number" && isFinite(value) && value >= floor && value > 0 ? null : bad(desc);
		}
		case "nonNegNum":
			return typeof value === "number" && isFinite(value) && value >= 0 ? null : bad(DESC.nonNegNum);
		case "bool":
			return typeof value === "boolean" ? null : bad(DESC.bool);
		case "strings":
			return Array.isArray(value) && value.every((m) => typeof m === "string") ? null : bad(DESC.strings);
		case "path":
			return typeof value === "string" && value.trim() ? null : bad(DESC.path);
		case "enum": {
			// An enum field always carries its values; a spec without them is a broken schema.
			const values = /** @type {string[]} */ (spec.values);
			return typeof value === "string" && values.includes(value) ? null : bad(`one of ${values.map((v) => `"${v}"`).join(" or ")}`);
		}
	}
}

/**
 * @param {string} path
 * @param {Record<string, any>} obj
 * @param {boolean} inQueue True when checking a queue entry, false when checking `defaults`
 * @returns {string[]}
 */
function checkFields(path, obj, inQueue) {
	const known = Object.keys(FIELDS).filter((f) => (inQueue ? !FIELDS[f].defaultsOnly : !FIELDS[f].queuesOnly));
	const errors = [];
	for (const [field, value] of Object.entries(obj)) {
		const spec = FIELDS[field];
		if (!spec) {
			errors.push(`${path}.${field} is an unknown field (known: ${known.join(", ")})`);
			continue;
		}
		if (inQueue && spec.defaultsOnly) {
			errors.push(`${path}.${field} is only valid in defaults (${REJECT})`);
			continue;
		}
		if (!inQueue && spec.queuesOnly) {
			errors.push(`${path}.${field} is only valid in a queue entry (${REJECT})`);
			continue;
		}
		const error = checkValue(`${path}.${field}`, value, spec);
		if (error) errors.push(error);
	}
	return errors;
}

/** @param {unknown} raw @returns {string[]} */
export function validateConfig(raw) {
	if (!isObj(raw)) return ["config must be a JSON object"];
	const errors = [];
	const { schemaVersion, defaults, queues, ...rest } = /** @type {Record<string, any>} */ (raw);
	for (const field of Object.keys(rest)) errors.push(`${field} is an unknown top-level field (known: schemaVersion, defaults, queues)`);
	if (schemaVersion !== undefined) {
		if (!Number.isInteger(schemaVersion) || schemaVersion < 1) errors.push(`schemaVersion must be a positive integer (got ${JSON.stringify(schemaVersion)})`);
		else if (schemaVersion > SCHEMA_VERSION)
			errors.push(`schemaVersion ${schemaVersion} is newer than this pi-llm-queue build supports (max ${SCHEMA_VERSION}) — upgrade pi-llm-queue`);
	}
	if (defaults !== undefined) {
		if (!isObj(defaults)) errors.push("defaults must be an object");
		else errors.push(...checkFields("defaults", defaults, false));
	}
	if (queues !== undefined) {
		if (!isObj(queues)) errors.push("queues must be an object");
		else
			for (const [key, entry] of Object.entries(queues)) {
				if (!isObj(entry)) errors.push(`queues.${key} must be an object`);
				else errors.push(...checkFields(`queues.${key}`, entry, true));
			}
	}
	return errors;
}

/**
 * Read a JSON file as an object; anything unreadable (missing file, bad JSON) is an empty one.
 * models.json is optional for pi, and a config/catalog read must never kill a detached daemon.
 * @param {string} path @returns {Record<string, any>}
 */
export function readJson(path) {
	try {
		return JSON.parse(readFileSync(path, "utf8")) ?? {};
	} catch {
		return {};
	}
}

/**
 * @typedef {object} LoadedConfig
 * @property {Record<string, any>} config
 * @property {string[]} errors
 * @property {boolean} invalidJson True when the file exists but is not valid JSON — a half-written file, a silent no-op on reload.
 */

// Read a config file through the schema. A missing file is all defaults (no errors); any
// failure falls back to defaults and reports the errors for the caller to surface.
/** @param {string} path @returns {LoadedConfig} */
export function loadValidConfig(path) {
	let raw;
	try {
		raw = readFileSync(path, "utf8");
	} catch {
		return { config: {}, errors: [], invalidJson: false };
	}
	let parsed;
	try {
		parsed = JSON.parse(raw);
	} catch (error) {
		return { config: {}, errors: [`config is not valid JSON: ${error instanceof Error ? error.message : String(error)}`], invalidJson: true };
	}
	const errors = validateConfig(parsed);
	return errors.length ? { config: {}, errors, invalidJson: false } : { config: parsed, errors, invalidJson: false };
}
