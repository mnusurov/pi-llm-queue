// The Starter module owns the transaction (test/starter-config.test.ts). These two cases keep the
// session_start wiring covered: creation is adopted in the same session, and a reconcile names what
// it added. They drive pi events, not the module directly.
import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { closeAllClients, disposeAllExtensions, startExtension } from "./extension-harness.ts";

const STUB_API = "stub-starter-api";
const LOCAL_URL = "http://192.168.1.50:8000/v1";
const CLOUD_URL = "https://api.openai.com/v1";
const NATIVE_URL = "http://127.0.0.1:8080/v1";

const BASE_PROVIDERS = {
	strata: { baseUrl: LOCAL_URL, api: STUB_API, models: [{ id: "m" }] },
	extra: { baseUrl: LOCAL_URL, api: STUB_API, models: [{ id: "m" }] },
	openai: { baseUrl: CLOUD_URL, api: "openai-completions" },
	native: { baseUrl: NATIVE_URL, api: STUB_API },
};

const LOCAL_MODEL = { api: STUB_API, baseUrl: LOCAL_URL, provider: "strata", id: "m" };
const nativeOwned = (name: string) => (name === "native" ? { name } : undefined);

const startExt = (options: { config?: Record<string, any> | null; configText?: string } = {}) =>
	startExtension({ daemon: false, nativeOwned, models: BASE_PROVIDERS, ...options });

const messages = (session: ReturnType<ReturnType<typeof startExtension>["startSession"]>) => session.notifications().map((n) => n.message);

after(() => {
	closeAllClients();
	disposeAllExtensions();
});

describe("starter config at the session_start seam", () => {
	it("creates the config, notifies, and queues the self-hosted providers in the same session", () => {
		const ext = startExt({ config: null });
		assert.equal(ext.readConfig(), null, "importing the extension must not write the config");

		const session = ext.startSession("STARTER", { model: LOCAL_MODEL });
		const notes = messages(session);

		const written = JSON.parse(ext.readConfig()!);
		assert.deepEqual(written, {
			schemaVersion: 1,
			defaults: { queueTimeoutMs: 120000 },
			queues: { [LOCAL_URL]: { enabled: true }, [CLOUD_URL]: { enabled: false } },
		});
		assert.equal(notes.filter((message) => message.includes(ext.configPath)).length, 1, "one notify naming the created file");
		assert.ok(notes[0].includes("1 self-hosted"), `notify names the enabled endpoint count: ${notes[0]}`);
		assert.deepEqual(Object.keys(ext.providers()).sort(), ["extra", "strata"], "providers on the self-hosted endpoint are wrapped");
		assert.equal(ext.providers()["native"], undefined, "native-owned endpoint is excluded");
		assert.equal(ext.providers()["openai"], undefined, "cloud endpoint stays passthrough");
	});

	it("reconciles an existing config, notifies the added key, and wraps it in the same session", () => {
		const ext = startExt({ configText: JSON.stringify({ schemaVersion: 1, queues: { [CLOUD_URL]: { enabled: false } } }) });

		const session = ext.startSession("STARTER", { model: LOCAL_MODEL });
		const added = messages(session).filter((message) => message.includes("added"));

		assert.deepEqual(JSON.parse(ext.readConfig()!), {
			schemaVersion: 1,
			queues: { [CLOUD_URL]: { enabled: false }, [LOCAL_URL]: { enabled: true } },
		});
		assert.deepEqual(Object.keys(ext.providers()).sort(), ["extra", "strata"], "the reconciled endpoint is wrapped without a restart");
		assert.equal(added.length, 1, `one notify naming the added endpoint: ${JSON.stringify(added)}`);
		assert.ok(added[0].includes(LOCAL_URL) && added[0].includes(ext.configPath), `the notify names the endpoint and the file: ${added[0]}`);
	});
});
