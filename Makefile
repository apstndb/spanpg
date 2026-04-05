.PHONY: test test-integration vet

test:
	go test ./...

# Nested module: PostgreSQL TypeAnnotation probes (Docker or real Spanner; see integration/pgtypeannotation/README.md).
test-integration:
	cd integration/pgtypeannotation && go test -count=1 ./...

vet:
	go vet ./...
