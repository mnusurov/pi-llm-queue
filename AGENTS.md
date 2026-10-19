# pi-llm-queue

Pi extension + local daemon that serialises LLM access to protect KV-cache
warmth on constrained local model servers.

## Agent skills

### Issue tracker

GitHub Issues via `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Canonical five-role defaults. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `GLOSSARY.md` + `docs/adr/` at root. See `docs/agents/domain.md`.

### LLM queue

All LLM access goes through pi-llm-queue: one model slot, serialized. When delegating, run implementers one at a time and set child `timeoutMs` to include queue wait — a long queue wait is normal, not a hang.
