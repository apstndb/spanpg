# Thin wrapper over mise tasks (see mise.toml).
.PHONY: check test test-race test-integration lint fmt vet build

check:
	mise run check

test:
	mise run test

test-race:
	mise run test-race

# Nested module: PostgreSQL TypeAnnotation probes (Docker or real Spanner; see integration/pgtypeannotation/README.md).
test-integration:
	mise run test-integration

lint:
	mise run lint

fmt:
	mise run fmt

vet:
	mise run vet

build:
	mise run build
