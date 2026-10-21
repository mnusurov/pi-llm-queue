// The release-tag policy: a tag is the only thing that starts a publish, and a version that
// reached npm cannot be taken back. The rule that guards it lives in the script the workflow
// calls, so it is driven here through that script's own seam — the command line — the way the
// workflow drives it.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const script = fileURLToPath(new URL("../scripts/check-release-version.mjs", import.meta.url));
const manifest = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));

/** The script's answer to a tag: its exit code and what it said. */
const check = (tag: string) =>
	spawnSync(process.execPath, [script, tag], { encoding: "utf8" });

describe("the release-tag check", () => {
	it("accepts a stable tag that names the package version", () => {
		const result = check(`v${manifest.version}`);
		assert.equal(result.status, 0);
		assert.match(result.stdout, /matches the package version/);
	});

	it("refuses a tag that names another version", () => {
		const result = check("v99.99.99");
		assert.equal(result.status, 1);
		assert.match(result.stderr, /v99\.99\.99/);
		assert.match(result.stderr, /does not match the package version/);
	});

	it("refuses a prerelease tag", () => {
		const result = check("v0.2.0-beta.1");
		assert.equal(result.status, 1);
		assert.match(result.stderr, /stable/);
	});

	it("refuses a tag without the version prefix", () => {
		const result = check("0.1.2");
		assert.equal(result.status, 1);
		assert.match(result.stderr, /stable/);
	});
});
