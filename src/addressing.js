// Queue addressing: the rule deciding which Queue a request addresses, in both directions
// (ADR 0005). Direct: a model or an endpoint → { queueKey, entry }. Reverse: the queues → the
// providers to wrap. Rules only, never state: the Scheduler owns the reload's state migration
// (holders, waiters, switch counts are Scheduler state, #103), and asks this module's rule for
// each live queue's remap target.
//
// Plain JS on purpose: the daemon imports it, and Node's built-in type stripping is refused for
// files under node_modules, so a .ts import would kill an installed daemon (the config.js reason).
import { readJson } from "./config.js";

/**
 * @typedef {object} Address
 * @property {string} queueKey
 * @property {Record<string, any> | undefined} entry The raw config entry owning the key, undefined
 *   when nothing owns it. Policy resolution (entry field → defaults field → fallback) stays with
 *   the caller: this module returns the entry, never a resolved policy.
 */

/**
 * Providers a queue key addresses: a provider named directly, or every provider serving that endpoint.
 * @param {string} key The queue key.
 * @param {Record<string, any>} providers The provider catalog.
 * @returns {string[]} The provider names the key addresses.
 */
export function matchingProviders(key, providers) {
	if (providers[key]) return [key];
	return Object.keys(providers).filter((name) => providers[name]?.baseUrl === key);
}

/**
 * A queue entry's members: its own key plus its `match` list — the names that address this queue.
 * @param {string} key The queue key.
 * @param {Record<string, any> | undefined} entry The queue's config entry.
 * @returns {string[]} The member names.
 */
export function membersOf(key, entry) {
	return [key, ...(entry?.match ?? [])];
}

/**
 * True when a queue key addresses this endpoint: it names the endpoint, a provider serving it, or a provider name on it.
 * @param {string} key The queue key.
 * @param {Record<string, any>} providers The provider catalog.
 * @param {string} baseUrl The endpoint.
 * @returns {boolean} Whether the key addresses the endpoint.
 */
export function keyAddressesEndpoint(key, providers, baseUrl) {
	return matchingProviders(key, providers).some((name) => providers[name]?.baseUrl === baseUrl);
}

/**
 * The group rule: a group whose `match` names the provider exactly owns it (split), else a group
 * naming the endpoint (directly, or through a member provider serving it), else the endpoint
 * itself. A caller naming no provider (the endpoint form) never reaches the first branch.
 * @param {Record<string, any>} queues
 * @param {Record<string, any>} providers models.json provider catalog
 * @param {{ provider?: string, baseUrl: string }} model
 * @returns {string}
 */
function groupKeyFor(queues, providers, model) {
	const active = Object.entries(queues ?? {}).filter(([, entry]) => entry?.enabled !== false);
	for (const [name, entry] of active) if (entry?.match?.includes(model.provider)) return name;
	for (const [name, entry] of active) if (entry?.match?.some((/** @type {string} */ m) => m === model.baseUrl || providers[m]?.baseUrl === model.baseUrl)) return name;
	return model.baseUrl;
}

/**
 * The entry owning a key: the key's own entry, else the endpoint's entry under a provider name
 * (a baseUrl key wins over a provider-name key naming the same endpoint), else nothing.
 * @param {Record<string, any>} queues
 * @param {Record<string, any>} providers
 * @param {string} key
 * @returns {Record<string, any> | undefined}
 */
function owningEntry(queues, providers, key) {
	const all = queues ?? {};
	if (all[key]) return all[key];
	return Object.entries(all).find(([name]) => !name.includes("://") && providers[name]?.baseUrl === key)?.[1];
}

/**
 * Address a model's queue — the extension's form, where the provider-name branch is live: a group
 * naming the model's provider splits one endpoint into per-provider slot sets.
 * @param {{ provider?: string, baseUrl: string }} model
 * @param {Record<string, any>} queues
 * @param {Record<string, any>} providers
 * @returns {Address}
 */
export function addressModel(model, queues, providers) {
	const queueKey = groupKeyFor(queues, providers, model);
	return { queueKey, entry: owningEntry(queues, providers, queueKey) };
}

/**
 * Address the queue owning an endpoint — the daemon's form: a socket client names an endpoint and
 * no provider, so the provider-name branch is unreachable here by construction and a raw client
 * lands in the endpoint-level group (the documented split limitation).
 * @param {string} baseUrl
 * @param {Record<string, any>} queues
 * @param {Record<string, any>} providers
 * @returns {Address}
 */
export function addressEndpoint(baseUrl, queues, providers) {
	const queueKey = groupKeyFor(queues, providers, { baseUrl });
	return { queueKey, entry: owningEntry(queues, providers, queueKey) };
}

/**
 * The provider catalog from a models.json path: the `.providers` object, so callers stop repeating
 * the unwrap. Each process keeps its own reporting around it (the daemon's unreadable-catalog
 * warning is the daemon's, not this module's).
 * @param {string} path
 * @returns {Record<string, any>}
 */
export function readProviders(path) {
	return readJson(path).providers ?? {};
}

/**
 * The providers to wrap: queue keys naming a provider directly, keys naming an endpoint a provider
 * serves (providers on one endpoint share one queue), and a group's `match` members the same way.
 * Keys whose entry is `enabled: false` are skipped.
 * @param {Record<string, any>} queues
 * @param {Record<string, any>} providers
 * @returns {string[]}
 */
export function queuedProviders(queues, providers) {
	const names = new Set();
	for (const [key, entry] of Object.entries(queues)) {
		if (entry?.enabled === false) continue;
		for (const member of membersOf(key, entry)) for (const provider of matchingProviders(member, providers)) names.add(provider);
	}
	return [...names];
}
