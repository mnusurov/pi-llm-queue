// The Lease's terms (GLOSSARY "Lease terms", ADR 0009): the TTL and the idle-yield window a
// Lease is held under. This module owns the terms' shape, the refresh period they imply, their
// wire encoding and the growth rule — nothing else derives them.
//
// Plain JS on purpose (the src/config.js reason): the daemon imports it and Node's built-in type
// stripping is refused for files under node_modules, so a .ts import would kill an installed daemon.
//
// The module does not own the config: it never reads a config file, never knows the config schema
// and never resolves a Queue's rules. The caller owns its copy of the config and hands the module
// terms; the module turns terms into a period.

/**
 * The terms a Lease is held under: the TTL the daemon enforces for it and the idle-yield window it
 * is measured against. The Grant carries them on the wire, and the client refreshes the Lease on
 * them — never on its own copy of the config, which may be stale.
 *
 * @typedef {object} LeaseTerms
 * @property {number} leaseTtlMs The TTL the Lease is held under, in ms.
 * @property {number} idleYieldMs The idle-yield window the Lease is held under, in ms (0 disables the yield).
 */

/**
 * The terms built from the two values that define them. The one place the terms' shape is written:
 * every other module holds terms it got from here.
 *
 * @param {number} leaseTtlMs The TTL, in ms.
 * @param {number} idleYieldMs The idle-yield window, in ms (0 disables the yield).
 * @returns {LeaseTerms} The terms.
 */
export const termsFor = (leaseTtlMs, idleYieldMs) => ({ leaseTtlMs, idleYieldMs });

/**
 * The refresh period a Lease held under these terms is refreshed at: a third of the TTL (floor
 * 250 ms) and at most half the idle-yield window, so a live Holder is never yielded between two
 * refreshes. The floor stays under the TTL floor (1000 ms), or a sweep landing between two
 * refreshes evicts a healthy Holder.
 *
 * A refresh period is a behaviour constant, not a config default: it is derived from the terms,
 * wherever those terms came from (ADR 0009).
 *
 * @param {LeaseTerms} terms The terms a Lease is held under.
 * @returns {number} The period, in ms.
 */
export const periodForTerms = (terms) => {
	const ttl = Math.max(250, Math.floor(terms.leaseTtlMs / 3));
	// idleYieldMs 0 disables the feature, so it imposes no bound.
	const idle = terms.idleYieldMs > 0 ? Math.max(1, Math.floor(terms.idleYieldMs / 2)) : Infinity;
	return Math.min(ttl, idle);
};

/**
 * The terms as the wire carries them: the same two fields, under their wire names. The daemon
 * writes the terms through this, the client reads them through its counterpart.
 *
 * @param {LeaseTerms} terms The terms.
 * @returns {{ leaseTtlMs: number, idleYieldMs: number }} The wire fields.
 */
export const termsToWire = (terms) => termsFor(terms.leaseTtlMs, terms.idleYieldMs);

/**
 * The terms a wire message carried, or null when it carried none. A daemon that sends no terms
 * (an older one) leaves the client on its own policy: the terms are never guessed.
 *
 * @param {unknown} msg The wire message.
 * @returns {LeaseTerms | null} The terms, or null when the message carries none.
 */
export const termsFromWire = (msg) => {
	if (typeof msg !== "object" || msg === null) return null;
	const leaseTtlMs = Number((/** @type {Record<string, unknown>} */ (msg).leaseTtlMs));
	const idleYieldMs = Number((/** @type {Record<string, unknown>} */ (msg).idleYieldMs));
	// Both fields, or none.
	if (!Number.isFinite(leaseTtlMs) || leaseTtlMs <= 0 || !Number.isFinite(idleYieldMs) || idleYieldMs < 0) return null;
	return termsFor(leaseTtlMs, idleYieldMs);
};

/**
 * The terms a Lease held under `granted` grows to when the Queue's current terms are `current`:
 * the TTL term is the longer of the two — a policy edit never shortens a Lease that is already
 * held (ADR 0002) — and the idle-yield window is the one the Lease was granted under, for as long
 * as it is held. A changed window applies at the next Grant: a window the daemon lengthened under
 * a live Holder would leave that Holder's client refreshing slower than the window allows, and the
 * Sweep would Yield the Slot to a Waiter while the Holder still streams.
 *
 * @param {LeaseTerms} granted The terms the Lease was granted under.
 * @param {LeaseTerms} current The Queue's current terms.
 * @returns {LeaseTerms} The terms the Lease is held under from now on.
 */
export const growTerms = (granted, current) =>
	termsFor(Math.max(granted.leaseTtlMs, current.leaseTtlMs), granted.idleYieldMs);
