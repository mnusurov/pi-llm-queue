/**
 * Config for pi-llm-queue: `~/.pi/agent/llm-queue.json`.
 *
 * Queue identity is the endpoint baseUrl, so providers pointing at one GPU
 * share one queue automatically. Config keys accept either a baseUrl or a
 * provider name (resolved to that provider's baseUrl); baseUrl entries win
 * when both are present.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

export interface QueueSettings {
	maxStreams: number;
	policy: "priority-aging" | "fifo";
	agingAfterSlots: number;
	agingAfterMs: number;
	/** null = default (true when maxStreams === 1). */
	blockSubagentLaunch: boolean | null;
	/** null = unlimited wait for children, 0 for parents is "no wait". */
	queueTimeoutMs: number | null;
	enabled: boolean;
	failOpen: boolean;
	daemonIdleExitMs: number;
	leaseTtlMs: number;
}

export interface QueueFileShape {
	defaults?: Partial<QueueSettings>;
	queues?: Record<string, Partial<QueueSettings>>;
}

export interface ResolvedConfig {
	defaults: QueueSettings;
	queues: Record<string, Partial<QueueSettings>>;
}

export const DEFAULT_SETTINGS: QueueSettings = {
	maxStreams: 1,
	policy: "priority-aging",
	agingAfterSlots: 3,
	agingAfterMs: 60_000,
	blockSubagentLaunch: null,
	queueTimeoutMs: null,
	enabled: true,
	failOpen: true,
	daemonIdleExitMs: 60_000,
	leaseTtlMs: 30_000,
};

export function agentDir(): string {
	return process.env.PI_AGENT_DIR || path.join(os.homedir(), ".pi", "agent");
}

export function defaultConfigPath(): string {
	return process.env.PI_QUEUE_CONFIG || path.join(agentDir(), "llm-queue.json");
}

export function defaultSocketPath(): string {
	return process.env.PI_QUEUE_SOCKET || path.join(agentDir(), "llm-queue.sock");
}

export function defaultLockPath(): string {
	return process.env.PI_QUEUE_LOCK || path.join(agentDir(), "llm-queue.lock");
}

export function normalizeBaseUrl(url: string | undefined): string {
	if (!url) return "";
	const trimmed = String(url).trim().replace(/\/+$/, "");
	if (!trimmed) return "";
	const match = /^([a-z][a-z0-9+.-]*):\/\/([^/]*)(.*)$/i.exec(trimmed);
	if (!match) return trimmed.toLowerCase();
	const [, scheme, authority, rest] = match;
	return `${scheme.toLowerCase()}://${authority.toLowerCase()}${rest}`.toLowerCase();
}

function num(value: unknown, fallback: number): number {
	const parsed = Number(value);
	return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function int(value: unknown, fallback: number): number {
	const parsed = Number(value);
	return Number.isFinite(parsed) && parsed >= 0 ? Math.floor(parsed) : fallback;
}

function normalizeEntry(entry: Partial<QueueSettings> | undefined): Partial<QueueSettings> {
	if (!entry || typeof entry !== "object") return {};
	const out: Partial<QueueSettings> = {};
	if (entry.maxStreams !== undefined) out.maxStreams = Math.max(1, Math.floor(num(entry.maxStreams, 1)));
	if (entry.policy === "fifo" || entry.policy === "priority-aging") out.policy = entry.policy;
	if (entry.agingAfterSlots !== undefined) out.agingAfterSlots = Math.max(1, Math.floor(num(entry.agingAfterSlots, 1)));
	if (entry.agingAfterMs !== undefined) out.agingAfterMs = num(entry.agingAfterMs, 1);
	if (typeof entry.blockSubagentLaunch === "boolean") out.blockSubagentLaunch = entry.blockSubagentLaunch;
	if (entry.queueTimeoutMs !== undefined) out.queueTimeoutMs = entry.queueTimeoutMs === null ? null : int(entry.queueTimeoutMs, 0);
	if (typeof entry.enabled === "boolean") out.enabled = entry.enabled;
	if (typeof entry.failOpen === "boolean") out.failOpen = entry.failOpen;
	if (entry.daemonIdleExitMs !== undefined) out.daemonIdleExitMs = num(entry.daemonIdleExitMs, 1);
	if (entry.leaseTtlMs !== undefined) out.leaseTtlMs = num(entry.leaseTtlMs, 1);
	return out;
}

export function resolveConfig(raw: QueueFileShape | null | undefined): ResolvedConfig {
	const defaults: QueueSettings = { ...DEFAULT_SETTINGS, ...(normalizeEntry(raw?.defaults) as Partial<QueueSettings>) };
	const queues: Record<string, Partial<QueueSettings>> = {};
	for (const [key, entry] of Object.entries(raw?.queues ?? {})) {
		const normalizedKey = normalizeBaseUrl(key) || key;
		queues[normalizedKey] = { ...queues[normalizedKey], ...normalizeEntry(entry) };
	}
	return { defaults, queues };
}

export function loadConfig(configPath: string = defaultConfigPath()): { config: ResolvedConfig; error?: string } {
	try {
		const raw = JSON.parse(fs.readFileSync(configPath, "utf8")) as QueueFileShape;
		return { config: resolveConfig(raw) };
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
			return { config: resolveConfig({}), error: `llm-queue.json: ${(error as Error).message}` };
		}
		return { config: resolveConfig({}) };
	}
}

function findEntry(config: ResolvedConfig, provider: string, baseUrl: string): { key: string; entry: Partial<QueueSettings> } | null {
	const byBaseUrl = Object.prototype.hasOwnProperty.call(config.queues, baseUrl) ? baseUrl : undefined;
	const lowerProvider = provider.toLowerCase();
	const byProvider = Object.keys(config.queues).find((key) => !/^https?:\/\//i.test(key) && key.toLowerCase() === lowerProvider);
	const key = byBaseUrl ?? byProvider;
	return key ? { key, entry: config.queues[key] } : null;
}

/**
 * Queue key for a provider request. `null` means passthrough: the request is
 * not queued (provider explicitly disabled, or no endpoint to key on).
 */
export function resolveQueueKey(config: ResolvedConfig, provider: string, baseUrl: string | undefined): string | null {
	const key = normalizeBaseUrl(baseUrl);
	if (!key) return null;
	const found = findEntry(config, provider, key);
	if (found?.entry.enabled === false) return null;
	return key;
}

export function resolveQueueSettings(config: ResolvedConfig, provider: string, baseUrl: string | undefined): QueueSettings | null {
	const key = resolveQueueKey(config, provider, baseUrl);
	if (!key) return null;
	const entry = findEntry(config, provider, key)?.entry ?? {};
	const settings: QueueSettings = { ...config.defaults, ...entry };
	if (settings.blockSubagentLaunch === null) settings.blockSubagentLaunch = settings.maxStreams === 1;
	return settings;
}
