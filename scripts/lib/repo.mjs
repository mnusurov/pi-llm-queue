// Shared plumbing for the repo's check scripts: the repo root, and a walk that skips the
// dependency tree. Two scripts need both; neither needs more.
import { readdirSync } from "node:fs";
import { join } from "node:path";

export const repoRoot = decodeURIComponent(new URL("../..", import.meta.url).pathname);

/** Every file under `dir` that `accept` takes, skipping node_modules and .git. */
export const walk = (dir, accept) => {
	const found = [];
	for (const entry of readdirSync(dir, { withFileTypes: true })) {
		if (entry.name === "node_modules" || entry.name === ".git") continue;
		const path = join(dir, entry.name);
		if (entry.isDirectory()) found.push(...walk(path, accept));
		else if (accept(entry.name)) found.push(path);
	}
	return found;
};
