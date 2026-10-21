#!/usr/bin/env node
// The TypeScript suppression check (CODING_STANDARDS.md, "Language and imports"): `@ts-ignore` is
// forbidden — it silences a check forever — and every `@ts-expect-error` must say why the
// suppression is needed, in a comment beside it.
//
// Why a script and not review: a suppression without a reason is invisible in a diff that carries
// a hundred other lines, and a forbidden one is a silent hole in the type check the daemon-side
// JavaScript now lives under.
//
// Scope: the whole repository — the rule says "anywhere in the repository". The checker names the
// forbidden markers in its own source, so it skips itself.
// Usage: node scripts/check-suppressions.mjs
import { readFileSync } from "node:fs";
import { relative } from "node:path";
import { repoRoot, walk } from "./lib/repo.mjs";

// The checker names the forbidden markers in its own source, so it skips itself.
const SELF = "check-suppressions.mjs";
const sources = walk(repoRoot, (name) => /\.(ts|js|mjs)$/.test(name)).filter((path) => !path.endsWith(SELF));

const IGNORE = "@ts-ignore";
const EXPECT = "@ts-expect-error";

const findings = [];
for (const path of sources) {
	const lines = readFileSync(path, "utf8").split("\n");
	for (let i = 0; i < lines.length; i++) {
		const line = lines[i];
		if (!line.includes("@ts-")) continue;
		const where = `${relative(repoRoot, path)}:${i + 1}`;
		if (line.includes(IGNORE)) {
			findings.push(`${where}: \`${IGNORE}\` is forbidden — fix the type or use \`${EXPECT}\` with a reason`);
			continue;
		}
		if (!line.includes(EXPECT)) continue;
		// The reason: a comment on the same line after the suppression, or a comment line right above it.
		const trailing = line.slice(line.indexOf(EXPECT) + EXPECT.length).match(/\/\/\s*(.*)$/);
		const above = i > 0 ? lines[i - 1].trim() : "";
		const reason = trailing?.[1]?.trim() || (above.startsWith("//") ? above.slice(2).trim() : "");
		if (!reason) findings.push(`${where}: \`@ts-expect-error\` carries no reason — say why the suppression is needed in a comment`);
	}
}

for (const finding of findings) console.error(finding);
if (findings.length > 0) {
	console.error(`\n${findings.length} suppression problem(s).`);
	process.exit(1);
}
console.log("OK: no `@ts-ignore`, and every `@ts-expect-error` says why.");
