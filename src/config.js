// Versioned schema for ~/.pi/agent/llm-queue.json. validateConfig returns clear, path-named
// errors ([] = usable); loadValidConfig reads a file through it and falls back to defaults on
// any failure. Plain JS on purpose: the daemon imports it, and Node's built-in type stripping
// is refused for files under node_modules, so a .ts import would kill an installed daemon.
import { readFileSync } from "node:fs";

export const SCHEMA_VERSION = 1;

/**
 * @typedef {object} FieldSpec
 * @property {"posInt"|"nonNegInt"|"posNum"|"nonNegNum"|"bool"|"strings"|"enum"} kind
 * @property {string[]} [values]
 * @property {boolean} [defaultsOnly] Valid in `defaults` only; a per-queue entry with it is rejected.
 * @property {boolean} [queuesOnly] Valid in a queue entry only; `defaults` never reads it.
 */

/** @type {Record<string, FieldSpec>} */
const FIELDS = {
	maxStreams: { kind: "posInt" },
	policy: { kind: "enum", values: ["priority-aging", "fifo"] },
	leaseTtlMs: { kind: "posNum" },
	idleQuantumMs: { kind: "nonNegNum" },
	monopolyRequests: { kind: "posInt" },
	monopolyMs: { kind: "nonNegNum" },
	agingAfterSlots: { kind: "nonNegInt" },
	agingAfterMs: { kind: "nonNegNum" },
	enabled: { kind: "bool", queuesOnly: true },
	match: { kind: "strings", queuesOnly: true },
	blockSubagentLaunch: { kind: "bool" },
	failOpen: { kind: "bool", defaultsOnly: true },
	queueTimeoutMs: { kind: "posNum", defaultsOnly: true },
	daemonIdleExitMs: { kind: "nonNegNum", defaultsOnly: true },
};

const DESC = {
	posInt: "a positive integer",
	nonNegInt: "an integer >= 0",
	posNum: "a number > 0",
	nonNegNum: "a number >= 0",
	bool: "true or false",
	strings: "an array of strings",
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
	const bad = (desc) => `${path} must be ${desc} (got ${JSON.stringify(value)})`;
	switch (spec.kind) {
		case "posInt":
			return Number.isInteger(value) && value >= 1 ? null : bad(DESC.posInt);
		case "nonNegInt":
			return Number.isInteger(value) && value >= 0 ? null : bad(DESC.nonNegInt);
		case "posNum":
			return typeof value === "number" && isFinite(value) && value > 0 ? null : bad(DESC.posNum);
		case "nonNegNum":
			return typeof value === "number" && isFinite(value) && value >= 0 ? null : bad(DESC.nonNegNum);
		case "bool":
			return typeof value === "boolean" ? null : bad(DESC.bool);
		case "strings":
			return Array.isArray(value) && value.every((m) => typeof m === "string") ? null : bad(DESC.strings);
		case "enum":
			return typeof value === "string" && spec.values.includes(value) ? null : bad(`one of ${spec.values.map((v) => `"${v}"`).join(" or ")}`);
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
	const { schemaVersion, defaults, queues, ...rest } = raw;
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
		return { config: {}, errors: [`config is not valid JSON: ${error.message}`], invalidJson: true };
	}
	const errors = validateConfig(parsed);
	return errors.length ? { config: {}, errors, invalidJson: false } : { config: parsed, errors, invalidJson: false };
}
