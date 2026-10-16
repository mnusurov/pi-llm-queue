.PHONY: setup typecheck test test-socket clean

setup:
	npm install

typecheck:
	./node_modules/.bin/tsc -p tsconfig.json

# Full suite: typecheck, then every test file. Tests are stub/socket only —
# they never touch a real model endpoint.
test: typecheck
	node --test --test-timeout=30000 "test/*.test.ts"

# Socket seam only (fast loop while implementing scheduler behaviour).
test-socket:
	node --test --test-timeout=30000 test/serialisation.test.ts test/lease-ttl.test.ts

clean:
	rm -rf node_modules/.cache
