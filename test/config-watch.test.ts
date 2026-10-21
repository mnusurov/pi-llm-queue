// The config watch (#105): a failed watch is reported with context and re-armed, and a watch that
// died without an error is replaced rather than left silent. The filesystem is the mocked boundary:
// the watcher factory is injected, so the watch's own lifecycle is driven without a real file.
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { describe, it } from "node:test";
import { watchConfigFile } from "../src/config-watch.js";
import { pollUntil } from "./harness.ts";

const CONFIG_PATH = "/tmp/pi-llm-queue-watch/llm-queue.json";

class FakeWatcher extends EventEmitter {
	closed = false;
	unref() {}
	close() {
		this.closed = true;
	}
}

/** The injected watcher factory, and the watchers it handed out. */
const fakeWatch = () => {
	const watchers: FakeWatcher[] = [];
	const watchFn = (_dir: string, listener: (event: string, file: string | null) => void) => {
		const watcher = new FakeWatcher();
		// fs.watch delivers "change" and "rename" with the file name beside the event type.
		watcher.on("change", listener);
		watcher.on("rename", listener);
		watchers.push(watcher);
		return watcher;
	};
	return { watchers, watchFn };
};

describe("the config watch", () => {
	it("reports a failed watch with its context and re-arms it", () => {
		const { watchers, watchFn } = fakeWatch();
		const errors: Error[] = [];
		const handle = watchConfigFile(CONFIG_PATH, {
			onChange: () => {},
			onError: (error) => errors.push(error),
			watchFn,
			rearmMs: 100000, // the watchdog is off here: this case is the error path
		});
		assert.equal(watchers.length, 1, "the watch is armed once");

		const failure = new Error("ENOSPC: no space left on device");
		watchers[0].emit("error", failure);

		assert.deepEqual(errors, [failure], "the failure is reported with its context");
		assert.equal(watchers.length, 2, "the failed watch is re-armed");
		assert.equal(watchers[0].closed, true, "the broken watch is closed");

		handle.close();
	});

	it("replaces a watch that died without an error", async () => {
		const { watchers, watchFn } = fakeWatch();
		const handle = watchConfigFile(CONFIG_PATH, { onChange: () => {}, onError: () => {}, watchFn, rearmMs: 20 });

		assert.equal(watchers.length, 1, "the watch is armed once");
		await pollUntil(() => watchers.length > 1, 1000, "waiting for the watch to be replaced");
		assert.equal(watchers[0].closed, true, "the replaced watch is closed");

		handle.close();
	});

	it("reports a watch that could not be armed at all", () => {
		const errors: Error[] = [];
		const failure = new Error("ENOENT: no such file or directory");
		const handle = watchConfigFile(CONFIG_PATH, {
			onChange: () => {},
			onError: (error) => errors.push(error),
			watchFn: () => {
				throw failure;
			},
			rearmMs: 100000,
		});

		assert.deepEqual(errors, [failure], "a watch that never started is reported");
		handle.close();
	});

	it("debounces a burst of edits into one change", async () => {
		const { watchers, watchFn } = fakeWatch();
		let changes = 0;
		const handle = watchConfigFile(CONFIG_PATH, { onChange: () => changes++, onError: () => {}, watchFn, debounceMs: 20, rearmMs: 100000 });

		watchers[0].emit("change", "llm-queue.json");
		watchers[0].emit("change", "llm-queue.json");
		watchers[0].emit("change", "models.json"); // another file in the same directory
		await pollUntil(() => changes > 0, 1000, "waiting for the debounced change");

		assert.equal(changes, 1, "the burst is one change, and another file is none");
		handle.close();
	});
});
