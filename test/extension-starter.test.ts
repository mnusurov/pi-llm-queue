// Fresh install: a missing llm-queue.json means an inert queue. The first session_start must
// generate a Starter config from models.json and make the queue live in that same session.
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const STUB_API = "stub-starter-api";
const LOCAL_URL = "http://192.168.1.50:8000/v1";
const LOCAL_URL_2 = "http://10.0.0.5:8080/v1";
const LOCAL_URL_3 = "http://172.20.0.9:9000/v1";
const CLOUD_URL = "https://api.openai.com/v1";
const NATIVE_URL = "http://127.0.0.1:8080/v1";

const BASE_PROVIDERS = {
	strata: { baseUrl: LOCAL_URL, api: STUB_API, models: [{ id: "m" }] },
	extra: { baseUrl: LOCAL_URL, api: STUB_API, models: [{ id: "m" }] },
	openai: { baseUrl: CLOUD_URL, api: "openai-completions" },
	native: { baseUrl: NATIVE_URL, api: STUB_API },
};

// Point the extension's module-level paths at a throwaway agent dir with NO config file.
const agentDir = mkdtempSync(join(tmpdir(), "llm-queue-starter-"));
process.env.PI_CODING_AGENT_DIR = agentDir;
const writeModels = (providers: Record<string, any>) => writeFileSync(join(agentDir, "models.json"), JSON.stringify({ providers }));
writeModels(BASE_PROVIDERS);
const configPath = join(agentDir, "llm-queue.json");

const { default: extension } = await import("../src/extension.ts");

function makePi() {
	const handlers: Record<string, ((e: any, ctx: any) => void)[]> = {};
	const registered: Record<string, any> = {};
	return {
		handlers,
		registered,
		on: (event: string, handler: (e: any, ctx: any) => void) => (handlers[event] ??= []).push(handler),
		registerProvider: (name: string, config: any) => (registered[name] = config),
		unregisterProvider: (name: string) => delete registered[name],
		registerCommand: () => {},
	};
}

const makeCtx = (notifies: string[]) => ({
	sessionManager: { getSessionId: () => "STARTER" },
	model: { api: STUB_API, baseUrl: LOCAL_URL, provider: "strata", id: "m" },
	signal: undefined,
	// The built-in llama-style provider: native-owned endpoints must stay out of the starter.
	modelRegistry: { getRegisteredNativeProvider: (name: string) => (name === "native" ? { name } : undefined) },
	ui: { notify: (message: string) => notifies.push(message), setStatus: () => {} },
});

const startSession = (pi: any, notifies: string[]) => pi.handlers["session_start"].forEach((h: any) => h({}, makeCtx(notifies)));

describe("starter config on first launch", () => {
	it("creates the config once, notifies, and queues the self-hosted providers in the same session", () => {
		const pi = makePi();
		const notifies: string[] = [];
		extension(pi as any);
		assert.equal(existsSync(configPath), false, "importing the extension must not write the config");

		startSession(pi, notifies);

		const written = JSON.parse(readFileSync(configPath, "utf8"));
		assert.deepEqual(written, {
			schemaVersion: 1,
			// The parent queue-wait cap is the only defaults field a fresh install gets: a stuck queue
			// surfaces as a visible warning instead of a silent wait behind a long Child generation.
			defaults: { queueTimeoutMs: 120000 },
			queues: { [LOCAL_URL]: { enabled: true }, [CLOUD_URL]: { enabled: false } },
		});
		assert.equal(notifies.filter((m) => m.includes(configPath)).length, 1, "one notify naming the created file");
		assert.ok(notifies[0].includes("1 self-hosted"), `notify names the enabled endpoint count: ${notifies[0]}`);
		assert.deepEqual(Object.keys(pi.registered).sort(), ["extra", "strata"], "providers on the self-hosted endpoint are wrapped");
		assert.equal(pi.registered["native"], undefined, "native-owned endpoint is excluded");
		assert.equal(pi.registered["openai"], undefined, "cloud endpoint stays passthrough");
	});

	it("leaves an existing config untouched and stays quiet", () => {
		const handWritten = JSON.stringify({ schemaVersion: 1, queues: { strata: { maxStreams: 4 } } });
		writeFileSync(configPath, handWritten);
		const pi = makePi();
		const notifies: string[] = [];

		extension(pi as any);
		startSession(pi, notifies);

		assert.equal(readFileSync(configPath, "utf8"), handWritten, "a hand-tuned config is never overwritten");
		assert.deepEqual(notifies, [], "an established setup gets no starter notify");
	});
});

describe("reconcile of an existing config on session start", () => {
	/** One session_start against a hand-written config: the file it left behind and the notifies. */
	const sessionAgainst = (existing: string, providers: Record<string, any> = BASE_PROVIDERS) => {
		writeModels(providers);
		writeFileSync(configPath, existing);
		const pi = makePi();
		const notifies: string[] = [];
		extension(pi as any);
		startSession(pi, notifies);
		return { pi, notifies, added: notifies.filter((m) => m.includes("added")), written: readFileSync(configPath, "utf8") };
	};

	it("adds a self-hosted endpoint that has no Queue key, and wraps it in the same session", () => {
		const { pi, added, written } = sessionAgainst(JSON.stringify({ schemaVersion: 1, queues: { [CLOUD_URL]: { enabled: false } } }));

		assert.deepEqual(JSON.parse(written), {
			schemaVersion: 1,
			// Add-only: a file the user authored gains the missing Queue key and no `defaults` block.
			queues: { [CLOUD_URL]: { enabled: false }, [LOCAL_URL]: { enabled: true } },
		});
		assert.deepEqual(Object.keys(pi.registered).sort(), ["extra", "strata"], "the reconciled endpoint is wrapped without a pi restart");
		assert.equal(added.length, 1, `one notify naming the added endpoint: ${JSON.stringify(added)}`);
		assert.ok(added[0].includes(LOCAL_URL) && added[0].includes(configPath), `the notify names the endpoint and the file: ${added[0]}`);
	});

	it("adds only the missing self-hosted key, leaving tuned entries, an explicit false, defaults, cloud and native alone", () => {
		const existing = {
			schemaVersion: 1,
			defaults: { queueTimeoutMs: 5000, maxStreams: 2, leaseTtlMs: 9000 },
			queues: {
				[LOCAL_URL_2]: { enabled: false },
				[LOCAL_URL_3]: { maxStreams: 4, policy: "fifo", leaseTtlMs: 5000, idleYieldMs: 0, match: ["tuned"] },
			},
		};
		const { pi, added, written } = sessionAgainst(JSON.stringify(existing), {
			...BASE_PROVIDERS,
			second: { baseUrl: LOCAL_URL_2, api: STUB_API },
			tuned: { baseUrl: LOCAL_URL_3, api: STUB_API },
		});

		const after = JSON.parse(written);
		assert.deepEqual(after.defaults, existing.defaults, "the defaults block is never modified by reconcile");
		assert.deepEqual(after.queues[LOCAL_URL_3], existing.queues[LOCAL_URL_3], "a tuned entry keeps every field the user set");
		assert.deepEqual(after.queues[LOCAL_URL_2], { enabled: false }, "an explicit enabled: false survives every restart");
		assert.deepEqual(after.queues[LOCAL_URL], { enabled: true }, "the unwrapped self-hosted endpoint gains its Queue key");
		assert.equal(CLOUD_URL in after.queues, false, "a cloud endpoint is never added by reconcile");
		assert.equal(NATIVE_URL in after.queues, false, "a native-owned provider's endpoint is never added");
		assert.equal(added.length, 1, `only the added endpoint is notified: ${JSON.stringify(added)}`);
		assert.ok(
			added[0].includes(LOCAL_URL) && !added[0].includes(LOCAL_URL_2) && !added[0].includes(LOCAL_URL_3),
			`the notify names the endpoint it added and nothing that already existed: ${added[0]}`,
		);
		assert.deepEqual(Object.keys(pi.registered).sort(), ["extra", "strata", "tuned"], "the added endpoint is wrapped; the disabled one is not");
	});

	// #46 box 4: "byte-for-byte unchanged for those keys". The reconcile write is
	// JSON.stringify(data, null, 2), so what survives is the entry's exact serialized text; the
	// file's layout is the generator's. Pinned so the ADR cannot over-claim the file's bytes.
	it("keeps the tuned entry's exact serialized text, in the generator's 2-space layout", () => {
		const tuned = { maxStreams: 4, policy: "fifo", leaseTtlMs: 5000, idleYieldMs: 0, match: ["tuned"] };
		const { written } = sessionAgainst(JSON.stringify({ schemaVersion: 1, queues: { [LOCAL_URL_3]: tuned } }), {
			...BASE_PROVIDERS,
			tuned: { baseUrl: LOCAL_URL_3, api: STUB_API },
		});

		// The entry as the file spells it: key, then the value's own lines at the file's indent.
		const entryText = JSON.stringify(tuned, null, 2)
			.split("\n")
			.map((line, at) => (at ? `    ${line}` : line))
			.join("\n");
		assert.ok(written.includes(`"${LOCAL_URL_3}": ${entryText}`), `the tuned entry's text survives the write verbatim: ${written}`);
		assert.equal(
			written,
			`${JSON.stringify({ schemaVersion: 1, queues: { [LOCAL_URL_3]: tuned, [LOCAL_URL]: { enabled: true } } }, null, 2)}\n`,
			"a compact user file is reflowed to the generator's 2-space shape: the entry's bytes survive, the file's layout is the generator's",
		);
	});

	it("adds nothing when a Group's match already addresses the endpoint", () => {
		const existing = { schemaVersion: 1, queues: { local: { match: ["extra"], maxStreams: 3 } } };
		const { written, added } = sessionAgainst(JSON.stringify(existing));

		assert.deepEqual(JSON.parse(written), existing, "the endpoint is queued under the Group: no baseUrl key beside it");
		assert.deepEqual(added, [], "nothing added, nothing notified");
	});

	it("adds nothing on a second session start: reconcile is idempotent", () => {
		const { written } = sessionAgainst(JSON.stringify({ schemaVersion: 1, queues: {} }));
		const second = sessionAgainst(written);

		assert.equal(second.written, written, "the reconciled file is stable across restarts");
		assert.deepEqual(second.added, [], "a reconciled file gets no second notify");
	});

	it("leaves a config the schema rejects untouched", () => {
		const broken = JSON.stringify({ schemaVersion: 1, queues: { [LOCAL_URL]: { maxStreams: 0 } } });
		const { written, added } = sessionAgainst(broken);

		assert.equal(written, broken, "reconcile does not write into a rejected config");
		assert.deepEqual(added, [], "no reconcile notify on top of the config-rejected report");
	});
});
