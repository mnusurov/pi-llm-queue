.PHONY: test typecheck check e2e

typecheck:
	npm run typecheck

# The gate: typecheck, the public-export contract check, the doc check. The list lives in
# package.json — this target only names it.
check:
	npm run check

test: typecheck
	npm test

# Human-run tracer bullet: spawns real pi sessions against a stub endpoint.
e2e:
	node scripts/e2e-two-sessions.mjs
