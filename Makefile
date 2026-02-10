.PHONY: proto proto-local build test clean migrate-up migrate-down

# --- Proto generation ---

# Generate using buf (recommended — requires buf CLI: https://buf.build/docs/installation)
proto:
	buf generate

# Generate using local protoc (alternative if buf is not installed)
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc, protoc-gen-grpc-gateway, protoc-gen-openapiv2
proto-local:
	@mkdir -p gen/go gen/openapiv2
	protoc \
		--proto_path=proto \
		--go_out=gen/go --go_opt=paths=source_relative \
		--go-grpc_out=gen/go --go-grpc_opt=paths=source_relative \
		--grpc-gateway_out=gen/go --grpc-gateway_opt=paths=source_relative \
		--openapiv2_out=gen/openapiv2 \
		proto/perpledger/events/v1/events.proto \
		proto/perpledger/ingest/v1/ingest.proto \
		proto/perpledger/query/v1/query.proto \
		proto/perpledger/admin/v1/admin.proto

# --- Build ---

build:
	go build -o bin/perpledger ./cmd/perpledger

# --- Test ---

test:
	go test ./... -v -race -count=1

# --- Database migrations ---

# Requires POSTGRES_URL env var
migrate-up:
	go run ./cmd/migrate up

migrate-down:
	go run ./cmd/migrate down

# --- Clean ---

clean:
	rm -rf bin/ gen/
