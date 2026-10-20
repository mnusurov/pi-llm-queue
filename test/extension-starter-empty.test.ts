// Nothing to queue: a machine with no self-hosted, non-native endpoint must stay untouched —
// no file, no notify, no wrapped provider.
import assert from "node:assert/strict";
import { after, it } from "node:test";
import { closeAllClients, disposeAllExtensions, startExtension } from "./extension-harness.ts";

const MODELS = { openai: { baseUrl: "https://api.openai.com/v1" }, native: { baseUrl: "http://127.0.0.1:8080/v1" } };
const nativeOwned = (name: string) => (name === "native" ? { name } : undefined);

after(() => {
	closeAllClients();
	disposeAllExtensions();
});

it("creates no config when every endpoint is cloud or native-owned", () => {
	const ext = startExtension({ config: null, models: MODELS, daemon: false, nativeOwned });
	const session = ext.startSession("EMPTY", { model: { api: "openai-completions", baseUrl: MODELS.openai.baseUrl, provider: "openai", id: "m" } });

	assert.equal(ext.readConfig(), null, "no file is created");
	assert.deepEqual(session.notifications(), [], "nothing is notified");
	assert.deepEqual(ext.providers(), {});
});
