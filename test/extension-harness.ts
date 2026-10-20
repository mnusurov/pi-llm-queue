// The pi seam as a test surface (ADR 0006, #88): one agent dir per file, a pi stub that records
// handlers/providers/commands, and a session handle that owns the ExtensionContext. Tests dispatch
// events by name and take the wrapped stream; they never index a handler array or the registry.
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAssistantMessageEventStream, getApiProvider, registerApiProvider, unregisterApiProviders } from "@earendil-works/pi-ai/compat";
import { connectClient, sleep, waitForDaemon, type Client } from "./harness.ts";

export { closeAllClients, connectClient, exited, pollUntil, sleep, startDaemon, stopAllDaemons, waitForDaemon, QUEUE_KEY } from "./harness.ts";
export type { Client, Daemon } from "./harness.ts";

const AGENT_DIR = mkdtempSync(join(tmpdir(), "llm-queue-ext-"));
process.env.PI_CODING_AGENT_DIR = AGENT_DIR;

const DAEMON = decodeURIComponent(new URL("../src/daemon.js", import.meta.url).pathname);
const EMPTY_USAGE = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };

// The import-time read of PI_CODING_AGENT_DIR is the production contract (ADR 0006); setting it
// before this import is the whole isolation story. node --test gives each file its own process.
const { default: extension } = await import("../src/extension.ts");

interface ExtensionOptions {
	/** Written to llm-queue.json before registration. null or omitted = no config file. */
	config?: Record<string, any> | null;
	/** Exact file text, for suites that pin a hand-written config's bytes. */
	configText?: string;
	/** Provider catalog written to models.json; the map pi's readProviders unwraps. */
	models?: Record<string, any>;
	/** Applied around registration and restored on dispose. undefined deletes the variable. */
	env?: Record<string, string | undefined>;
	/** false leaves daemon autostart to the Extension (the fail-open and lifecycle suites). */
	daemon?: false | { sweepMs?: number };
	/** A provider is native-owned when this returns a truthy value. */
	nativeOwned?: (name: string) => unknown;
	/** Overrides the default one-event stub responder for every api in the models fixture. */
	stream?: (model: any, context: any, options: any) => any;
}

interface SessionOptions {
	model?: any;
	signal?: AbortSignal;
}

export interface Notification {
	message: string;
	level: string;
}

export type StatusEntry = [string, string | undefined];

const sessions = new Set<ExtensionHarness>();

function writeJson(path: string, value: unknown): void {
	writeFileSync(path, JSON.stringify(value));
}

function defaultModel(models: Record<string, any>): any {
	for (const [provider, entry] of Object.entries(models)) {
		if (!entry?.api) continue;
		return { api: entry.api, baseUrl: entry.baseUrl, provider, id: entry.models?.[0]?.id ?? "m" };
	}
	return { api: "stub", provider: "", id: "m" };
}

/** The default responder: one `start`, then `end`. The queue, not the stream, is under test. */
function stubStream(model: any) {
	const out = createAssistantMessageEventStream();
	out.push({
		type: "start",
		partial: { role: "assistant", content: [], api: model.api, provider: model.provider, model: model.id, usage: EMPTY_USAGE, stopReason: "stop", timestamp: Date.now() },
	} as any);
	out.end();
	return out;
}

/** The event's handlers, in registration order, awaited one at a time. */
async function dispatch(pi: PiStub, event: string, payload: any, ctx: any): Promise<void> {
	for (const handler of pi.handlers.get(event) ?? []) await handler(payload, ctx);
}

function dispatchSync(pi: PiStub, event: string, payload: any, ctx: any): void {
	for (const handler of pi.handlers.get(event) ?? []) handler(payload, ctx);
}

interface PiStub {
	handlers: Map<string, ((event: any, ctx: any) => unknown)[]>;
	providers: Map<string, any>;
	commands: Map<string, any>;
	on(event: string, handler: (event: any, ctx: any) => unknown): void;
	registerProvider(name: string, config: any): void;
	unregisterProvider(name: string): void;
	registerCommand(name: string, config: any): void;
}

function makePi(): PiStub {
	const pi: PiStub = {
		handlers: new Map(),
		providers: new Map(),
		commands: new Map(),
		on(event, handler) {
			const list = pi.handlers.get(event) ?? [];
			list.push(handler);
			pi.handlers.set(event, list);
		},
		registerProvider(name, config) {
			pi.providers.set(name, config);
		},
		unregisterProvider(name) {
			pi.providers.delete(name);
		},
		registerCommand(name, config) {
			pi.commands.set(name, config);
		},
	};
	return pi;
}

/** The first event a wrapped stream yields. */
export const firstEvent = (stream: AsyncIterable<any>) => (async () => { for await (const event of stream) return event; })();

/** "done" when the promise settles inside ms, "blocked" when it is still pending. */
export const race = async (promise: Promise<unknown>, ms: number): Promise<"done" | "blocked"> =>
	Promise.race([promise.then(() => "done" as const), sleep(ms).then(() => "blocked" as const)]);

/** One per `extension(pi)` call: an independent Extension instance on the shared agent dir. */
export class ExtensionHarness {
	readonly agentDir = AGENT_DIR;
	readonly configPath = join(AGENT_DIR, "llm-queue.json");
	readonly modelsPath = join(AGENT_DIR, "models.json");
	readonly socketPath = join(AGENT_DIR, "llm-queue.sock");
	private readonly pi = makePi();
	readonly nativeOwned: (name: string) => unknown;
	private readonly savedEnv = new Map<string, { previous: string | undefined; applied: string | undefined }>();
	private readonly apis: string[] = [];
	private readonly ownedDaemon: boolean;
	private readonly served: ChildProcess[] = [];
	private readyPromise: Promise<void> | null = null;
	private disposed = false;

	constructor(options: ExtensionOptions = {}) {
		this.nativeOwned = options.nativeOwned ?? (() => undefined);
		const models = options.models ?? {};
		writeJson(this.modelsPath, { providers: models });
		if (options.configText !== undefined) writeFileSync(this.configPath, options.configText);
		else if (options.config === null || options.config === undefined) rmSync(this.configPath, { force: true });
		else writeJson(this.configPath, options.config);

		for (const [name, value] of Object.entries(options.env ?? {})) {
			this.savedEnv.set(name, { previous: process.env[name], applied: value });
			if (value === undefined) delete process.env[name];
			else process.env[name] = value;
		}
		extension(this.pi as any);

		for (const api of new Set(Object.values(models).map((entry) => entry?.api).filter(Boolean) as string[])) {
			const pool = apiPools.get(api);
			if (pool) {
				pool.refs++;
			} else if (!getApiProvider(api)) {
				apiPools.set(api, { refs: 1, source: `extension-harness-${apiSeq++}` });
				const source = apiPools.get(api)!.source;
				const responder = options.stream ?? stubStream;
				registerApiProvider({ api, stream: undefined as any, streamSimple: (model: any, context: any, opts: any) => responder(model, context, opts) } as any, source);
			} else {
				continue; // a builtin or externally registered api: never register over it
			}
			this.apis.push(api);
		}

		const daemon = options.daemon;
		this.ownedDaemon = daemon !== false;
		if (this.ownedDaemon) {
			sharedDaemons.refs++;
			sharedDaemons.proc ??= this.spawnDaemon(typeof daemon === "object" ? daemon.sweepMs : undefined);
		}
		this.model = defaultModel(models);
		sessions.add(this);
	}

	readonly model: any;

	/** Wait for the harness-spawned daemon, so the Extension connects to this process, not a racer. */
	ready(): Promise<void> {
		this.readyPromise ??= this.ownedDaemon ? waitForDaemon(this.socketPath) : Promise.resolve();
		return this.readyPromise;
	}

	/** The per-session handle. Dispatches session_start and owns the ctx every later event sees. */
	startSession(sessionId: string, options: SessionOptions = {}): SessionHandle {
		const handle = new SessionHandle(this, this.pi, sessionId, options);
		dispatchSync(this.pi, "session_start", {}, handle.ctx);
		return handle;
	}

	/** What got registered, read-only: a copy, so a test cannot mutate the registry. */
	providers(): Readonly<Record<string, any>> {
		return Object.fromEntries(this.pi.providers);
	}

	/** Raw socket client on this Extension's daemon, for acquire/release/status driving. */
	connect(): Promise<Client> {
		return connectClient(this.socketPath);
	}

	/** The daemon's status reply, or one queue's entry when a key is given. */
	async status(queueKey?: string): Promise<any> {
		const client = await this.connect();
		client.send({ op: "status" });
		const reply = await client.next(2000);
		client.close();
		return queueKey === undefined ? reply : reply.queues.find((queue: any) => queue.queueKey === queueKey);
	}

	/** A daemon serving the agent dir's current config, for suites that write it first. */
	async serve(sweepMs?: number): Promise<ChildProcess> {
		const proc = this.spawnDaemon(sweepMs);
		this.served.push(proc);
		await waitForDaemon(this.socketPath);
		return proc;
	}

	writeConfig(config: Record<string, any>): void {
		writeJson(this.configPath, config);
	}

	readConfig(): string | null {
		try {
			return readFileSync(this.configPath, "utf8");
		} catch {
			return null;
		}
	}

	/** The daemon this Extension spawned, when `daemon` was not false. */
	get daemon(): ChildProcess | null {
		return this.ownedDaemon ? sharedDaemons.proc : null;
	}

	/** Tracked children, sockets, api providers and env: one call per Extension instance. */
	dispose(): void {
		if (this.disposed) return;
		this.disposed = true;
		sessions.delete(this);
		for (const proc of this.served) proc.kill();
		for (const api of this.apis) {
			const pool = apiPools.get(api);
			if (!pool) continue;
			if (--pool.refs <= 0) {
				unregisterApiProviders(pool.source);
				apiPools.delete(api);
			}
		}
		// Restore only a value this instance still owns: a sibling disposed out of order must not
		// clobber the environment it set after this one.
		for (const [name, { previous, applied }] of this.savedEnv) {
			if (process.env[name] !== applied) continue;
			if (previous === undefined) delete process.env[name];
			else process.env[name] = previous;
		}
		if (this.ownedDaemon && --sharedDaemons.refs <= 0) {
			sharedDaemons.proc?.kill();
			sharedDaemons.proc = null;
		}
		if (sharedDaemons.refs <= 0) killAutostarted(this.socketPath);
	}

	private spawnDaemon(sweepMs?: number): ChildProcess {
		const args = [DAEMON, "--socket", this.socketPath, "--config", this.configPath, "--models-json", this.modelsPath];
		if (sweepMs) args.push("--sweep-ms", String(sweepMs));
		const proc = spawn(process.execPath, args, { stdio: "ignore" });
		proc.unref();
		return proc;
	}
}

const sharedDaemons: { proc: ChildProcess | null; refs: number } = { proc: null, refs: 0 };
const apiPools = new Map<string, { refs: number; source: string }>();
let apiSeq = 0;

/** The Extension autostarts detached and keeps no handle (LeaseClient), so match its socket. */
function killAutostarted(socketPath: string): void {
	spawnSync("pkill", ["-f", socketPath]);
}

/** The only entry: one per `extension(pi)` call. */
export function startExtension(options: ExtensionOptions = {}): ExtensionHarness {
	return new ExtensionHarness(options);
}

/** A suite's one cleanup hook: every live Extension disposes through its own handle. */
export function disposeAllExtensions(): void {
	for (const ext of [...sessions]) ext.dispose();
}

/** The per-session entry: events arrive with this handle's ctx, streams with its session id. */
export class SessionHandle {
	readonly sessionId: string;
	readonly ctx: any;
	private readonly ext: ExtensionHarness;
	private readonly pi: PiStub;
	private readonly notes: Notification[] = [];
	private readonly statusLog: StatusEntry[] = [];

	constructor(ext: ExtensionHarness, pi: PiStub, sessionId: string, options: SessionOptions) {
		this.ext = ext;
		this.sessionId = sessionId;
		this.pi = pi;
		this.ctx = {
			sessionManager: { getSessionId: () => sessionId },
			model: options.model ?? ext.model,
			signal: options.signal,
			modelRegistry: { getRegisteredNativeProvider: ext.nativeOwned },
			ui: {
				notify: (message: string, level: string) => this.notes.push({ message, level }),
				setStatus: (key: string, text: string | undefined) => this.statusLog.push([key, text]),
			},
		};
	}

	/** The event ctx's model; assign to simulate a run that switched models. */
	get model(): any {
		return this.ctx.model;
	}

	set model(value: any) {
		this.ctx.model = value;
	}

	/** Every handler for the event, in order, once the harness's daemon is listening. */
	async dispatch(event: string, payload: Record<string, any> = {}): Promise<void> {
		await this.ext.ready();
		await dispatch(this.pi, event, payload, this.ctx);
	}

	/** The wrapped streamSimple pi would call, tagged with this session's id. */
	stream(model: any = this.ctx.model, options: Record<string, any> = {}): any {
		const provider = this.pi.providers.get(model.provider);
		if (!provider) throw new Error(`pi-llm-queue: no provider registered as "${model.provider}"`);
		return provider.streamSimple(model, {}, { sessionId: this.sessionId, ...options });
	}

	notifications(): Notification[] {
		return [...this.notes];
	}

	statuses(): StatusEntry[] {
		return [...this.statusLog];
	}

	async command(name: string, args = ""): Promise<void> {
		const command = this.pi.commands.get(name);
		if (!command) throw new Error(`pi-llm-queue: no command registered as "${name}"`);
		await command.handler(args, this.ctx);
	}
}
