.PHONY: proto proto-local build test test-unit test-integration test-coverage \
       clean migrate-up migrate-down lint docker-build docker-up docker-down \
       docker-test-up docker-test-down setup-nats dev

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

# --- Test (per doc §17) ---

test:
	go test ./... -v -race -count=1

test-unit:
	go test ./internal/... -v -race -count=1 -short

test-integration:
	INTEGRATION_TEST=1 go test ./... -v -race -count=1 -run Integration

test-coverage:
	go test ./internal/... -race -coverprofile=coverage.out -covermode=atomic
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# --- Lint ---

lint:
	golangci-lint run ./...

# --- Database migrations ---

migrate-up:
	go run ./cmd/migrate up

migrate-down:
	go run ./cmd/migrate down

# --- Docker (per doc §19) ---

docker-build:
	docker build -t perpledger:dev .

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-test-up:
	docker compose -f docker-compose.test.yml up -d
	@echo "Waiting for services..."
	@sleep 3

docker-test-down:
	docker compose -f docker-compose.test.yml down -v

# --- NATS setup (per doc §19) ---

setup-nats:
	bash deploy/scripts/setup-nats.sh

# --- Developer workflow (per doc §19) ---

dev: docker-up
	@echo "Waiting for Postgres and NATS..."
	@sleep 3
	go run ./cmd/migrate up
	bash deploy/scripts/setup-nats.sh
	@echo "Dependencies ready. Run: source .env.dev && go run ./cmd/perpledger"

# --- Clean ---

clean:
	rm -rf bin/ gen/ coverage.out coverage.html
