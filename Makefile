.PHONY: test typecheck e2e

typecheck:
	npx tsc --noEmit

test: typecheck
	node --test test/*.test.ts

# Human-run tracer bullet: spawns real pi sessions against a stub endpoint.
e2e:
	node scripts/e2e-two-sessions.mjs
