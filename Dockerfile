# Per doc §19: Multi-stage Dockerfile for PerpLedger
# Stage 1: Builder
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /app/perpledger ./cmd/perpledger

# Stage 2: Runtime
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

RUN addgroup -S perp && adduser -S perp -G perp

WORKDIR /app

COPY --from=builder /app/perpledger .
COPY --from=builder /app/migrations ./migrations

USER perp

EXPOSE 8080 9090 9091

HEALTHCHECK --interval=5s --timeout=3s --start-period=30s --retries=3 \
    CMD wget -qO- http://localhost:8080/healthz || exit 1

ENTRYPOINT ["./perpledger"]
