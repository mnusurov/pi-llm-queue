.PHONY: test typecheck check e2e

typecheck:
	npx tsc --noEmit

# The guardrail: typecheck plus the public-export contract check.
check: typecheck
	node scripts/check-jsdoc.mjs

test: check
	node --test test/*.test.ts

# Human-run tracer bullet: spawns real pi sessions against a stub endpoint.
e2e:
	node scripts/e2e-two-sessions.mjs
