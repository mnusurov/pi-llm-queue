// Fresh install: a missing llm-queue.json means an inert queue. The first session_start must
// generate a Starter config from models.json and make the queue live in that same session.
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { closeAllClients, disposeAllExtensions, startExtension } from "./extension-harness.ts";

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

const LOCAL_MODEL = { api: STUB_API, baseUrl: LOCAL_URL, provider: "strata", id: "m" };
// The built-in llama-style provider: native-owned endpoints must stay out of the starter.
const nativeOwned = (name: string) => (name === "native" ? { name } : undefined);

const startExt = (options: { config?: Record<string, any> | null; configText?: string; models?: Record<string, any>; nativeOwned?: (name: string) => unknown } = {}) =>
	startExtension({ daemon: false, nativeOwned, models: BASE_PROVIDERS, ...options });

const messages = (session: ReturnType<ReturnType<typeof startExtension>["startSession"]>) => session.notifications().map((n) => n.message);

after(() => {
	closeAllClients();
	disposeAllExtensions();
});

describe("starter config on first launch", () => {
	it("creates the config once, notifies, and queues the self-hosted providers in the same session", () => {
		const ext = startExt({ config: null });
		assert.equal(ext.readConfig(), null, "importing the extension must not write the config");

		const session = ext.startSession("STARTER", { model: LOCAL_MODEL });
		const notes = messages(session);

		const written = JSON.parse(ext.readConfig()!);
		assert.deepEqual(written, {
			schemaVersion: 1,
			// The parent queue-wait cap is the only defaults field a fresh install gets: a stuck queue
			// surfaces as a visible warning instead of a silent wait behind a long Child generation.
			defaults: { queueTimeoutMs: 120000 },
			queues: { [LOCAL_URL]: { enabled: true }, [CLOUD_URL]: { enabled: false } },
		});
		assert.equal(notes.filter((message) => message.includes(ext.configPath)).length, 1, "one notify naming the created file");
		assert.ok(notes[0].includes("1 self-hosted"), `notify names the enabled endpoint count: ${notes[0]}`);
		assert.deepEqual(Object.keys(ext.providers()).sort(), ["extra", "strata"], "providers on the self-hosted endpoint are wrapped");
		assert.equal(ext.providers()["native"], undefined, "native-owned endpoint is excluded");
		assert.equal(ext.providers()["openai"], undefined, "cloud endpoint stays passthrough");
	});

	it("leaves an existing config untouched and stays quiet", () => {
		const handWritten = JSON.stringify({ schemaVersion: 1, queues: { strata: { maxStreams: 4 } } });
		const ext = startExt({ configText: handWritten });

		const session = ext.startSession("STARTER", { model: LOCAL_MODEL });

		assert.equal(ext.readConfig(), handWritten, "a hand-tuned config is never overwritten");
		assert.deepEqual(messages(session), [], "an established setup gets no starter notify");
	});
});

describe("reconcile of an existing config on session start", () => {
	/** One session_start against a hand-written config: the file it left behind and the notifies. */
	const sessionAgainst = (existing: string, providers: Record<string, any> = BASE_PROVIDERS) => {
		const ext = startExt({ configText: existing, models: providers });
		const session = ext.startSession("STARTER", { model: LOCAL_MODEL });
		const notes = messages(session);
		return { ext, session, notes, added: notes.filter((message) => message.includes("added")), written: ext.readConfig()! };
	};

	it("adds a self-hosted endpoint that has no Queue key, and wraps it in the same session", () => {
		const { ext, added, written } = sessionAgainst(JSON.stringify({ schemaVersion: 1, queues: { [CLOUD_URL]: { enabled: false } } }));

		assert.deepEqual(JSON.parse(written), {
			schemaVersion: 1,
			// Add-only: a file the user authored gains the missing Queue key and no `defaults` block.
			queues: { [CLOUD_URL]: { enabled: false }, [LOCAL_URL]: { enabled: true } },
		});
		assert.deepEqual(Object.keys(ext.providers()).sort(), ["extra", "strata"], "the reconciled endpoint is wrapped without a pi restart");
		assert.equal(added.length, 1, `one notify naming the added endpoint: ${JSON.stringify(added)}`);
		assert.ok(added[0].includes(LOCAL_URL) && added[0].includes(ext.configPath), `the notify names the endpoint and the file: ${added[0]}`);
	});

	// #46 box 2: "`/queue` shows the queue". A queue exists at the daemon only after its first
	// acquire, so the reconciled key has to be driven once before status can name it.
	it("queues the added key at the daemon: one acquire makes it visible in status", async () => {
		const { ext } = sessionAgainst(JSON.stringify({ schemaVersion: 1, queues: { [CLOUD_URL]: { enabled: false } } }));
		await ext.serve();
		const client = await ext.connect();

		client.send({ op: "acquire", queueKey: LOCAL_URL, sessionKey: "S", role: "parent", activity: true });
		assert.deepEqual(await client.next(3000), { op: "granted", queueKey: LOCAL_URL, sessionKey: "S" }, "the added key is a queue the daemon grants");
		client.send({ op: "status" });
		const status = await client.next(3000);
		assert.deepEqual(
			status.queues.map((q: any) => q.queueKey),
			[LOCAL_URL],
			"from its first acquire on, the reconciled endpoint is a queue a reader sees — no more silent passthrough",
		);

		client.send({ op: "release", queueKey: LOCAL_URL, sessionKey: "S" });
		client.close();
	});

	it("names a self-hosted endpoint once when several providers serve it", () => {
		const { added, written } = sessionAgainst(JSON.stringify({ schemaVersion: 1, queues: {} }));

		assert.equal(added.length, 1, `one notify for the shared endpoint: ${JSON.stringify(added)}`);
		assert.equal(added[0].split(LOCAL_URL).length - 1, 1, `the notify names the endpoint exactly once: ${added[0]}`);
		assert.deepEqual(JSON.parse(written).queues, { [LOCAL_URL]: { enabled: true } }, "the shared endpoint is one queue key");
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
		const { ext, added, written } = sessionAgainst(JSON.stringify(existing), {
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
		assert.deepEqual(Object.keys(ext.providers()).sort(), ["extra", "strata", "tuned"], "the added endpoint is wrapped; the disabled one is not");
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
