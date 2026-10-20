#!/usr/bin/env node
// The public-export contract check (CODING_STANDARDS.md, "Comments"): every export a caller can
// use carries a JSDoc contract — purpose, inputs, failure modes. Handled by review it was missed
// twice in one day (#92, #94) and re-found per refactor, so it is checked instead.
//
// Why a hand-rolled scan and not ESLint: the package is dependency-free at runtime, and the only
// question here is "does an exported declaration have a JSDoc block above it". ESLint would bring
// three dev dependencies, a flat config and an update treadmill for one question.
//
// The check answers presence, not quality: whether a contract says the right thing stays the
// reviewer's call. Scope: src/ — the package publishes src/, and test/ is not public API.
// Usage: node scripts/check-jsdoc.mjs
import { readFileSync, readdirSync } from "node:fs";
import { join, relative } from "node:path";

const repo = decodeURIComponent(new URL("..", import.meta.url).pathname);
const src = join(repo, "src");

const sources = [];
const walk = (dir) => {
	for (const entry of readdirSync(dir, { withFileTypes: true })) {
		const path = join(dir, entry.name);
		if (entry.isDirectory()) walk(path);
		else if (/\.(ts|js)$/.test(entry.name)) sources.push(path);
	}
};
walk(src);

// `export { x } from "./y"` re-exports: the contract lives at the declaration site, not here.
const KINDS = "function|class|interface|type|enum|const|let|var";
const REEXPORT = /^export\s+(?:type\s+)?\{/;
const DECLARATION = new RegExp(`^export\\s+(default\\s+)?(?:async\\s+)?(?:${KINDS})\\b`);
const NAME = new RegExp(`^export\\s+(?:async\\s+)?(?:${KINDS})\\s+([A-Za-z_$][\\w$]*)`);

/** The JSDoc block directly above a declaration, or null when there is none. */
const jsdocAbove = (lines, declIndex) => {
	let end = declIndex - 1;
	while (end >= 0 && lines[end].trim() === "") end--;
	if (end < 0 || !lines[end].trim().endsWith("*/")) return null;
	let start = end;
	while (start >= 0) {
		const trimmed = lines[start].trim();
		if (trimmed.startsWith("/**")) {
			const body = lines
				.slice(start, end + 1)
				.join("\n")
				.replace(/^\s*\/\*\*+/, "")
				.replace(/\*+\/\s*$/, "")
				.replace(/^\s*\* ?/gm, "")
				.trim();
			return body.length > 0 ? body : null;
		}
		if (trimmed.startsWith("/*")) return null;
		start--;
	}
	return null;
};

const findings = [];
for (const path of sources) {
	const lines = readFileSync(path, "utf8").split("\n");
	for (let i = 0; i < lines.length; i++) {
		const line = lines[i];
		if (!line.startsWith("export")) continue;
		if (REEXPORT.test(line)) continue;
		const declaration = DECLARATION.exec(line);
		if (!declaration) continue;
		const name = declaration[1] ? "default export" : (NAME.exec(line)?.[1] ?? "export");
		if (jsdocAbove(lines, i)) continue;
		findings.push(`${relative(repo, path)}:${i + 1}: exported \`${name}\` has no JSDoc contract`);
	}
}

for (const finding of findings) console.error(finding);
if (findings.length > 0) {
	console.error(`\n${findings.length} export(s) without a JSDoc contract.`);
	process.exit(1);
}
console.log(`OK: every export in ${relative(repo, src)} carries a JSDoc contract.`);
