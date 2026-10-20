#!/usr/bin/env node
// The doc-rot check: the repo's own links, paths and ADR references must resolve. A doc that
// points at a file that no longer exists is worse than no doc — the reader follows it and finds
// nothing.
//
// Three claims are checked, all of them mechanical:
//
//   1. Markdown links — the target file must exist, and an anchor must name a heading that does.
//   2. ADR references — "ADR 0004" must name a record in docs/adr/, and a record's filename must
//      exist.
//   3. Backticked paths — a path written in backticks must exist.
//
// Illustrations are not claims: fenced code blocks and blockquotes are skipped, and a path is
// only a claim when it looks like one (a `~` path, a `<placeholder>` and a bare ratio are not).
//
// The rules come from the rot-audit skill's recipes, made executable. Usage: node scripts/check-docs.mjs
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { repoRoot, walk } from "./lib/repo.mjs";

const adrDir = join(repoRoot, "docs", "adr");

// A backticked path is only checked when it could be a repo path: it has a slash, and either it
// names a file (a dot) or it starts at a directory the repo actually has.
const ROOTS = new Set(["src", "test", "docs", "scripts", ".github"]);

const markdown = walk(repoRoot, (name) => name.endsWith(".md"));

/** The lines that make claims: fenced code blocks and blockquotes are illustrations. */
const claimLines = (text) => {
	const lines = [];
	let inFence = false;
	text.split("\n").forEach((line, i) => {
		if (/^\s*```/.test(line)) {
			inFence = !inFence;
			return;
		}
		if (inFence) return;
		if (/^\s*>/.test(line)) return;
		lines.push([i + 1, line]);
	});
	return lines;
};

/** GitHub's heading slug: lowercase, punctuation dropped, spaces to hyphens. */
const slug = (heading) =>
	heading
		.trim()
		.toLowerCase()
		.replace(/[^\p{L}\p{N}\s_-]/gu, "")
		.replace(/\s+/g, "-");

const headings = (path) => {
	const found = new Set();
	let inFence = false;
	for (const line of readFileSync(path, "utf8").split("\n")) {
		if (/^\s*```/.test(line)) {
			inFence = !inFence;
			continue;
		}
		if (inFence) continue;
		const heading = /^(#{1,6})\s+(.*)$/.exec(line);
		if (heading) found.add(slug(heading[2]));
	}
	return found;
};

/** Where a path could live: beside the file that names it, or at a repo root. */
const candidates = (from, path) => [
	resolve(dirname(from), path),
	resolve(repoRoot, path),
];

const existsSomewhere = (from, path) => candidates(from, path).some((candidate) => existsSync(candidate));

const findings = [];
const report = (file, line, message) => findings.push(`${relative(repoRoot, file)}:${line}: ${message}`);

for (const file of markdown) {
	const text = readFileSync(file, "utf8");

	for (const [line, content] of claimLines(text)) {
		// 1. Markdown links.
		for (const match of content.matchAll(/\[[^\]]*\]\(([^)\s]+)\)/g)) {
			const target = match[1];
			if (/^(https?:|mailto:)/.test(target)) continue;
			const [path, anchor] = target.split("#");
			const resolved = path ? candidates(file, path).find((candidate) => existsSync(candidate)) : file;
			if (!resolved) {
				report(file, line, `link target \`${target}\` does not exist`);
				continue;
			}
			if (anchor && !headings(resolved).has(anchor.toLowerCase())) {
				report(file, line, `link anchor \`#${anchor}\` is not a heading in ${relative(repoRoot, resolved)}`);
			}
		}

		// 2. ADR references: "ADR 0004" and the record's own filename.
		for (const match of content.matchAll(/\bADR[- ]?(\d{1,4})\b/gi)) {
			const number = match[1].padStart(4, "0");
			const records = readdirSync(adrDir).filter((name) => name.startsWith(`${number}-`));
			if (records.length === 0) report(file, line, `ADR ${number} has no record in docs/adr/`);
		}
		for (const match of content.matchAll(/\b(\d{4}-[a-z0-9-]+\.md)\b/g)) {
			if (!existsSomewhere(file, match[1])) {
				report(file, line, `record \`${match[1]}\` does not exist`);
			}
		}

		// 3. Backticked paths.
		for (const match of content.matchAll(/`([^`\s]+)`/g)) {
			const token = match[1];
			if (!token.includes("/")) continue;
			if (token.startsWith("~") || token.startsWith("/")) continue;
			if (/[<>]/.test(token)) continue;
			const head = token.split("/")[0];
			if (!token.includes(".") && !ROOTS.has(head)) continue;
			if (!existsSomewhere(file, token)) {
				report(file, line, `path \`${token}\` does not exist`);
			}
		}
	}
}

for (const finding of findings) console.error(finding);
if (findings.length > 0) {
	console.error(`\n${findings.length} broken claim(s) in the docs.`);
	process.exit(1);
}
console.log(`OK: every link, ADR reference and path in the docs resolves.`);
