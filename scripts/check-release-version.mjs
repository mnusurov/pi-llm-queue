#!/usr/bin/env node
// The release-tag check: a tag is the only thing that starts a publish, and a published version
// cannot be taken back. So the tag must name a stable version, and that version must be the one
// package.json declares — otherwise the workflow publishes code under a version it does not claim.
//
// Why a script and not a shell comparison in the workflow: the rule lives in one place, and the
// workflow calls the rule instead of restating it.
// Usage: node scripts/check-release-version.mjs [tag]
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { repoRoot } from "./lib/repo.mjs";

const STABLE_TAG = /^v(\d+\.\d+\.\d+)$/;

/**
 * The reason a tag must not be published, or null when the tag may publish.
 *
 * @param {string} tag The git tag that triggered the release.
 * @param {string} version The version package.json declares.
 * @returns {string | null} The refusal, or null when the tag is publishable.
 */
const releaseTagProblem = (tag, version) => {
	const match = STABLE_TAG.exec(tag);
	if (!match) {
		return `tag ${tag} is not a stable release tag — this workflow publishes only vMAJOR.MINOR.PATCH`;
	}
	if (match[1] !== version) {
		return `tag ${tag} does not match the package version ${version}`;
	}
	return null;
};

const manifest = JSON.parse(readFileSync(join(repoRoot, "package.json"), "utf8"));
const tag = process.argv[2] ?? process.env.GITHUB_REF_NAME ?? "";
const problem = releaseTagProblem(tag, manifest.version);
if (problem) {
	console.error(`Refusing to publish: ${problem}`);
	process.exit(1);
}
console.log(`OK: ${tag} matches the package version ${manifest.version}.`);
