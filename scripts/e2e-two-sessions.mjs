#!/usr/bin/env node
// Human-run tracer bullet (#2): two pi sessions against one stub endpoint must be strictly
// serialised by the daemon. Runs the real extension, real daemon autostart and real pi sessions.
// Not part of `make test`: it spawns pi processes. Usage: node scripts/e2e-two-sessions.mjs
import { spawn, spawnSync } from "node:child_process";
import { createConnection } from "node:net";
import { createServer } from "node:http";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const repo = decodeURIComponent(new URL("..", import.meta.url).pathname);
const agentDir = mkdtempSync(join(tmpdir(), "llm-queue-e2e-"));

let inflight = 0;
let overlaps = 0;
let requests = 0;

const server = createServer((req, res) => {
	if (req.url.startsWith("/v1/models")) {
		res.writeHead(200, { "content-type": "application/json" });
		res.end(JSON.stringify({ data: [] }));
		return;
	}
	let body = "";
	req.on("data", (chunk) => (body += chunk));
	req.on("end", () => {
		requests++;
		inflight++;
		if (inflight > 1) overlaps++;
		const chunk = (delta, finish_reason) =>
			`data: ${JSON.stringify({
				id: `stub-${requests}`,
				object: "chat.completion.chunk",
				created: Math.floor(Date.now() / 1000),
				model: "stub-model",
				choices: [{ index: 0, delta, finish_reason }],
			})}\n\n`;
		res.writeHead(200, { "content-type": "text/event-stream" });
		res.write(chunk({ role: "assistant", content: "pong" }, null));
		res.write(chunk({}, "stop"));
		res.write("data: [DONE]\n\n");
		inflight--;
		res.end();
	});
});

await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const baseUrl = `http://127.0.0.1:${server.address().port}/v1`;

writeFileSync(
	join(agentDir, "models.json"),
	JSON.stringify({
		providers: {
			stub: {
				baseUrl,
				api: "openai-completions",
				apiKey: "stub",
				models: [
					{
						id: "stub-model",
						name: "Stub",
						input: ["text"],
						reasoning: false,
						cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
						contextWindow: 8192,
						maxTokens: 256,
					},
				],
			},
		},
	}),
);
writeFileSync(
	join(agentDir, "llm-queue.json"),
	JSON.stringify({ defaults: { maxStreams: 1 }, queues: { stub: { maxStreams: 1 } } }),
);

function runPi(label) {
	return new Promise((resolve) => {
		const child = spawn(
			"pi",
			["--print", "--no-tools", "--model", "stub/stub-model", "-ne", "-e", join(repo, "src/extension.ts"), "Reply with exactly: pong"],
			{
				cwd: agentDir,
				env: { ...process.env, PI_CODING_AGENT_DIR: agentDir, PI_OFFLINE: "1", PI_SKIP_VERSION_CHECK: "1" },
				stdio: ["ignore", "pipe", "pipe"],
			},
		);
		let out = "";
		child.stdout.on("data", (chunk) => (out += chunk));
		child.stderr.on("data", (chunk) => (out += chunk));
		child.on("close", (code) => resolve({ label, code, out }));
	});
}

const results = await Promise.all([runPi("A"), runPi("B")]);
const alive = await new Promise((resolve) => {
	const sock = createConnection(join(agentDir, "llm-queue.sock"));
	sock.once("connect", () => {
		sock.destroy();
		resolve(true);
	});
	sock.once("error", () => resolve(false));
});
if (alive) spawnSync("pkill", ["-f", join(agentDir, "llm-queue.sock")]);
server.close();

const ok = requests === 2 && overlaps === 0 && alive && results.every((r) => r.code === 0 && r.out.includes("pong"));
for (const r of results) console.log(`${r.label}: exit=${r.code} out=${JSON.stringify(r.out.trim().slice(0, 120))}`);
console.log(`requests=${requests} overlaps=${overlaps} daemon-autostart=${alive}`);
console.log(ok ? "PASS: two pi sessions were strictly serialised" : "FAIL: see counters above");
process.exit(ok ? 0 : 1);
