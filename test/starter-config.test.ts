// The Starter config transaction at its own seam (issue #94): ensureStarterConfig owns reading,
// deciding, the pre-rename guard, the atomic write and the post-write adoption read. These tests
// pin every ADR-0001 clause without dispatching a pi event.
import assert from "node:assert/strict";
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { afterEach, describe, it } from "node:test";
import { join } from "node:path";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { ensureStarterConfig } from "../src/starter-config.ts";

const LOCAL_URL = "http://192.168.1.50:8000/v1";
const LOCAL_URL_2 = "http://10.0.0.5:8080/v1";
const LOCAL_URL_3 = "http://172.20.0.9:9000/v1";
const CLOUD_URL = "https://api.openai.com/v1";
const NATIVE_URL = "http://127.0.0.1:8080/v1";

const PROVIDERS = {
	strata: { baseUrl: LOCAL_URL },
	extra: { baseUrl: LOCAL_URL },
	openai: { baseUrl: CLOUD_URL },
	native: { baseUrl: NATIVE_URL },
};
const nativeOwned = (name: string): boolean => name === "native";

const temps: string[] = [];
const tempDir = () => {
	const dir = mkdtempSync(join(tmpdir(), "llm-queue-starter-"));
	temps.push(dir);
	return dir;
};
const configPathIn = (dir: string) => join(dir, "llm-queue.json");
const ensure = (configPath: string, providers: Record<string, any> = PROVIDERS, isNativeOwned: (name: string) => boolean = nativeOwned) =>
	ensureStarterConfig({ configPath, providers, isNativeOwned });

afterEach(() => {
	for (const dir of temps.splice(0)) rmSync(dir, { recursive: true, force: true });
});

describe("missing config", () => {
	it("creates the documented Starter shape and returns the adopted config", () => {
		const path = configPathIn(tempDir());

		const result = ensure(path);

		assert.equal(result.status, "created");
		const expected = {
			schemaVersion: 1,
			defaults: { queueTimeoutMs: 120000 },
			queues: { [LOCAL_URL]: { enabled: true }, [CLOUD_URL]: { enabled: false } },
		};
		assert.deepEqual(result.status === "created" && result.config, expected, "the result carries the validated re-read");
		assert.deepEqual(JSON.parse(readFileSync(path, "utf8")), expected, "the file has one endpoint-keyed entry per baseUrl");
		assert.deepEqual(Object.keys(expected.queues), [...Object.keys(expected.queues)].sort(), "keys are sorted");
	});

	it("writes no file when there is no self-hosted non-native endpoint", () => {
		const path = configPathIn(tempDir());

		const result = ensure(path, { openai: { baseUrl: CLOUD_URL }, native: { baseUrl: NATIVE_URL } });

		assert.deepEqual(result, { status: "untouched", reason: "nothing-to-queue" });
		assert.throws(() => readFileSync(path, "utf8"), /ENOENT/);
	});
});

describe("reconcile of an existing config", () => {
	const existing = {
		schemaVersion: 1,
		defaults: { queueTimeoutMs: 5000, maxStreams: 2, leaseTtlMs: 9000 },
		queues: {
			[LOCAL_URL_2]: { enabled: false },
			[LOCAL_URL_3]: { maxStreams: 4, policy: "fifo", leaseTtlMs: 5000, idleYieldMs: 0, match: ["tuned"] },
		},
	};
	const providers = { ...PROVIDERS, second: { baseUrl: LOCAL_URL_2 }, tuned: { baseUrl: LOCAL_URL_3 } };

	it("adds only missing self-hosted keys, leaving every existing entry and defaults untouched", () => {
		const path = configPathIn(tempDir());
		writeFileSync(path, JSON.stringify(existing));

		const result = ensure(path, providers);

		assert.equal(result.status, "reconciled");
		assert.deepEqual(result.status === "reconciled" && result.added, [LOCAL_URL], "the result lists exactly the added key");
		assert.deepEqual(
			result.status === "reconciled" && result.config.queues,
			{ [LOCAL_URL_2]: { enabled: false }, [LOCAL_URL_3]: existing.queues[LOCAL_URL_3], [LOCAL_URL]: { enabled: true } },
			"the result carries the validated re-read config",
		);
		const written = JSON.parse(readFileSync(path, "utf8"));
		assert.deepEqual(written.defaults, existing.defaults, "the defaults block is never modified");
		assert.deepEqual(written.queues[LOCAL_URL_3], existing.queues[LOCAL_URL_3], "a tuned entry keeps every field");
		assert.deepEqual(written.queues[LOCAL_URL_2], { enabled: false }, "an explicit enabled: false survives");
		assert.deepEqual(written.queues[LOCAL_URL], { enabled: true }, "the unwrapped self-hosted endpoint gains its key");
		assert.equal(CLOUD_URL in written.queues, false, "a cloud endpoint is never added");
		assert.equal(NATIVE_URL in written.queues, false, "a native-owned endpoint is never added");
	});

	it("keeps each existing entry's exact serialized text", () => {
		const tuned = existing.queues[LOCAL_URL_3];
		const path = configPathIn(tempDir());
		writeFileSync(path, JSON.stringify({ schemaVersion: 1, queues: { [LOCAL_URL_3]: tuned } }));

		ensure(path, { ...PROVIDERS, tuned: { baseUrl: LOCAL_URL_3 } });

		const written = readFileSync(path, "utf8");
		const entryText = JSON.stringify(tuned, null, 2)
			.split("\n")
			.map((line, at) => (at ? `    ${line}` : line))
			.join("\n");
		assert.ok(written.includes(`"${LOCAL_URL_3}": ${entryText}`), `the tuned entry's text survives verbatim: ${written}`);
		assert.equal(
			written,
			`${JSON.stringify({ schemaVersion: 1, queues: { [LOCAL_URL_3]: tuned, [LOCAL_URL]: { enabled: true } } }, null, 2)}\n`,
			"the write uses the generator's 2-space layout",
		);
	});

	it("does not add an endpoint its own key already addresses", () => {
		const path = configPathIn(tempDir());
		const text = JSON.stringify({ schemaVersion: 1, queues: { [LOCAL_URL]: { enabled: false } } });
		writeFileSync(path, text);

		const result = ensure(path);

		assert.deepEqual(result, { status: "untouched", reason: "nothing-to-add" });
		assert.equal(readFileSync(path, "utf8"), text, "an explicit enabled: false stays the disable path");
	});

	it("does not add an endpoint a provider-name key already addresses", () => {
		const path = configPathIn(tempDir());
		const text = JSON.stringify({ schemaVersion: 1, queues: { strata: { maxStreams: 3 } } });
		writeFileSync(path, text);

		const result = ensure(path);

		assert.deepEqual(result, { status: "untouched", reason: "nothing-to-add" });
		assert.equal(readFileSync(path, "utf8"), text);
	});

	it("does not add an endpoint a Group's match already addresses", () => {
		const path = configPathIn(tempDir());
		const text = JSON.stringify({ schemaVersion: 1, queues: { local: { match: ["extra"], maxStreams: 3 } } });
		writeFileSync(path, text);

		const result = ensure(path);

		assert.deepEqual(result, { status: "untouched", reason: "nothing-to-add" });
		assert.equal(readFileSync(path, "utf8"), text, "no baseUrl key is split beside the Group");
	});

	it("leaves a schema-rejected config untouched", () => {
		const path = configPathIn(tempDir());
		const text = JSON.stringify({ schemaVersion: 1, queues: { [LOCAL_URL]: { maxStreams: 0 } } });
		writeFileSync(path, text);

		const result = ensure(path);

		assert.deepEqual(result, { status: "untouched", reason: "rejected-config" });
		assert.equal(readFileSync(path, "utf8"), text);
	});

	it("leaves an invalid-JSON config untouched", () => {
		const path = configPathIn(tempDir());
		writeFileSync(path, "{ half-written");

		const result = ensure(path);

		assert.deepEqual(result, { status: "untouched", reason: "rejected-config" });
		assert.equal(readFileSync(path, "utf8"), "{ half-written");
	});
});

describe("endpoint classification through the Starter shape", () => {
	const urls = {
		loopback4: "http://127.0.0.1:8080/v1",
		loopback6: "http://[::1]:8080/v1",
		localhost: "http://localhost:8080/v1",
		rfc1918: "http://172.16.0.1:8000/v1",
		linkLocal: "http://169.254.7.7:8000/v1",
		mdns: "http://gpu.local:8000/v1",
		lan: "http://llama.lan:8000/v1",
		public: "https://8.8.8.8/v1",
		nearRfc1918: "https://172.15.0.1:8000/v1",
		nearLinkLocal: "https://169.253.1.1/v1",
		nip: "http://10.0.0.1.nip.io:8080/v1",
		localhostSuffix: "http://127.0.0.1.example.com:8080/v1",
		hostname: "https://api.openai.com/v1",
	};
	const providers = Object.fromEntries(Object.entries(urls).map(([name, baseUrl]) => [name, { baseUrl }]));

	it("enables loopback, RFC1918, link-local and local domains, and passes the rest through", () => {
		const path = configPathIn(tempDir());

		const result = ensure(path, providers, () => false);

		assert.equal(result.status, "created");
		const queues = result.status === "created" ? result.config.queues : {};
		const enabled = Object.fromEntries(Object.entries(queues).map(([key, entry]: [string, any]) => [key, entry.enabled]));
		assert.deepEqual(enabled, {
			[urls.loopback4]: true,
			[urls.loopback6]: true,
			[urls.localhost]: true,
			[urls.rfc1918]: true,
			[urls.linkLocal]: true,
			[urls.mdns]: true,
			[urls.lan]: true,
			[urls.public]: false,
			[urls.nearRfc1918]: false,
			[urls.nearLinkLocal]: false,
			[urls.nip]: false,
			[urls.localhostSuffix]: false,
			[urls.hostname]: false,
		});
	});

	it("omits a provider with no baseUrl from the file entirely", () => {
		const path = configPathIn(tempDir());

		const result = ensure(path, { anthropic: { api: "anthropic" }, invalid: { baseUrl: "not a url" } }, () => false);

		assert.deepEqual(result, { status: "untouched", reason: "nothing-to-queue" });
		assert.throws(() => readFileSync(path, "utf8"), /ENOENT/, "no key, so the provider stays unwrapped");
	});
});

describe("transaction failures", () => {
	it("reports an existing but unreadable config as failed: unreadable", () => {
		const path = tempDir();

		const result = ensure(path);

		assert.deepEqual(result, { status: "failed", reason: "unreadable" });
	});

	it("reports a failed atomic write as failed: unwritable", () => {
		const path = join(tempDir(), "missing-parent", "llm-queue.json");

		const result = ensure(path);

		assert.deepEqual(result, { status: "failed", reason: "unwritable" });
	});

	it("does not overwrite a file another writer changed after it was read", () => {
		const path = configPathIn(tempDir());
		const original = JSON.stringify({ schemaVersion: 1, queues: {} });
		writeFileSync(path, original);
		const real = fs.readFileSync;
		let reads = 0;
		(fs as any).readFileSync = (file: any, options: any) => {
			reads++;
			return reads === 2 ? JSON.stringify({ schemaVersion: 1, queues: { other: { enabled: true } } }) : real(file, options as any);
		};
		syncBuiltinESMExports();

		try {
			const result = ensure(path);
			assert.deepEqual(result, { status: "untouched", reason: "raced" });
			assert.equal(readFileSync(path, "utf8"), original, "the other writer's file wins");
		} finally {
			(fs as any).readFileSync = real;
			syncBuiltinESMExports();
		}
	});

	it("does not rename when the pre-rename guard cannot read the file", () => {
		const path = configPathIn(tempDir());
		const original = JSON.stringify({ schemaVersion: 1, queues: {} });
		writeFileSync(path, original);
		const real = fs.readFileSync;
		let reads = 0;
		(fs as any).readFileSync = (file: any, options: any) => {
			reads++;
			if (reads === 2) throw Object.assign(new Error("EACCES"), { code: "EACCES" });
			return real(file, options as any);
		};
		syncBuiltinESMExports();

		try {
			const result = ensure(path);
			assert.deepEqual(result, { status: "failed", reason: "unreadable" }, "a guard read failure is not a benign race");
			assert.equal(readFileSync(path, "utf8"), original, "the file is never overwritten on an unverifiable guard");
		} finally {
			(fs as any).readFileSync = real;
			syncBuiltinESMExports();
		}
	});

	it("reports a post-write re-read that is not valid as failed: unstable", () => {
		const path = configPathIn(tempDir());
		writeFileSync(path, JSON.stringify({ schemaVersion: 1, queues: {} }));
		const real = fs.readFileSync;
		let reads = 0;
		(fs as any).readFileSync = (file: any, options: any) => {
			reads++;
			return reads === 3 ? "{ not a config" : real(file, options as any);
		};
		syncBuiltinESMExports();

		try {
			const result = ensure(path);
			assert.deepEqual(result, { status: "failed", reason: "unstable" });
			assert.ok(readFileSync(path, "utf8").includes(LOCAL_URL), "the write completed before the failed re-read");
		} finally {
			(fs as any).readFileSync = real;
			syncBuiltinESMExports();
		}
	});
});

it("exposes only the Starter transaction", async () => {
	const module = await import("../src/starter-config.ts");

	assert.deepEqual(Object.keys(module), ["ensureStarterConfig"]);
});
