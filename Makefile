.PHONY: test typecheck

typecheck:
	npx tsc --noEmit

test: typecheck
	node --test test/*.test.ts
