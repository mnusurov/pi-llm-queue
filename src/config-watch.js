// The config watch, in one module: both processes watch the same file the same way, and both keep
// the watch alive. Plain JS on purpose (the src/config.js reason): the daemon imports it, and Node's
// built-in type stripping is refused for files under node_modules, so a .ts import would kill an
// installed daemon.
import { watch } from "node:fs";
import { basename, dirname } from "node:path";

/**
 * Watch a config file for edits and keep the watch alive.
 *
 * The watch is armed on the file's *directory*: an atomic-save editor replaces the file, and a file
 * watcher goes dead with it. Edits are debounced, so a save that writes the file twice is one edit.
 *
 * The watch is also kept alive, which is the point of the module (#105): a watch is armed once per
 * process, and a watch that dies stays dead — a directory that is renamed away delivers nothing and
 * reports no failure at all. So a failed watch is reported with context and re-armed, and a watch
 * that died silently is replaced on the next re-arm tick. A stale config then degrades the
 * caller's provider and queue sets — an annoyance the operator can see — instead of silently
 * deciding a Lease's life.
 *
 * @param {string} configPath The config file to watch.
 * @param {object} options
 * @param {() => void} options.onChange Called (debounced) when the file may have changed; the caller
 *   reads and validates the file itself, because the two processes report a bad config differently.
 * @param {(error: Error) => void} options.onError Called when the watch fails; the caller logs it
 *   with its own context.
 * @param {(dir: string, listener: (event: string, file: string | null) => void) => any} [options.watchFn]
 *   The watcher factory; the filesystem boundary a test replaces.
 * @param {number} [options.debounceMs] The debounce window.
 * @param {number} [options.rearmMs] How often the watch is replaced, so a watch that died silently
 *   is not left in place.
 * @returns {{ close: () => void }} A handle that stops the watch.
 */
export function watchConfigFile(configPath, { onChange, onError, watchFn = watch, debounceMs = 250, rearmMs = 30000 }) {
	const dir = dirname(configPath);
	const name = basename(configPath);
	/** @type {import("node:fs").FSWatcher | null} */
	let watcher = null;
	/** @type {NodeJS.Timeout | null} */
	let debounce = null;
	/** @type {NodeJS.Timeout | null} */
	let rearm = null;
	let closed = false;

	const stop = () => {
		if (!watcher) return;
		try {
			watcher.close();
		} catch {}
		watcher = null;
	};

	// Arm the watch, and report a failure to arm it: a watch that never started is a config that
	// never reloads, and the operator has to see it.
	const arm = () => {
		if (closed) return;
		try {
			watcher = watchFn(dir, (_event, file) => {
				if (file && file !== name) return; // another file in the same directory
				clearTimeout(debounce ?? undefined);
				debounce = setTimeout(() => {
					debounce = null;
					onChange();
				}, debounceMs);
				debounce.unref?.();
			});
		} catch (error) {
			onError(error instanceof Error ? error : new Error(String(error)));
			return;
		}
		// The watch is armed now: a factory that hands back nothing is a broken watch, and the
		// failure surfaces where it happens.
		const armed = /** @type {import("node:fs").FSWatcher} */ (watcher);
		armed.on("error", (error) => {
			onError(error);
			// The watch is broken: replace it, so the next edit is not missed.
			stop();
			arm();
		});
		armed.unref?.();
	};

	arm();
	// The watchdog: a watch can die without an error (a renamed-away directory delivers nothing), so
	// it is replaced on a tick whether or not it looks healthy.
	rearm = setInterval(() => {
		stop();
		arm();
	}, rearmMs);
	rearm.unref?.();

	return {
		close: () => {
			closed = true;
			clearTimeout(debounce ?? undefined);
			clearInterval(rearm ?? undefined);
			stop();
		},
	};
}
