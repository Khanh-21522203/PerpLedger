#!/bin/bash
# Per doc §19: NATS stream and consumer setup for local development
# Usage: ./deploy/scripts/setup-nats.sh
# Requires: nats CLI (https://github.com/nats-io/natscli)

set -euo pipefail

NATS_URL="${PERP_NATS_URL:-nats://localhost:4222}"

echo "Setting up NATS streams at ${NATS_URL}..."

# Inbound events stream
nats stream add PERP_EVENTS \
  --server="${NATS_URL}" \
  --subjects="perp.events.>" \
  --storage=file \
  --retention=limits \
  --max-msgs=-1 \
  --max-bytes=10737418240 \
  --max-age=168h \
  --max-msg-size=1048576 \
  --discard=old \
  --dupe-window=2m \
  --replicas=1 \
  --no-deny-delete \
  --no-deny-purge \
  2>/dev/null || nats stream update PERP_EVENTS \
    --server="${NATS_URL}" \
    --subjects="perp.events.>" \
    2>/dev/null || true

echo "  ✓ PERP_EVENTS stream"

# Durable consumer for PerpLedger
nats consumer add PERP_EVENTS perp-ledger \
  --server="${NATS_URL}" \
  --filter="" \
  --ack=explicit \
  --deliver=all \
  --max-deliver=5 \
  --ack-wait=30s \
  --replay=instant \
  --pull \
  --max-pending=1000 \
  2>/dev/null || true

echo "  ✓ perp-ledger consumer"

# Outbound stream (processed events)
nats stream add PERP_PROCESSED \
  --server="${NATS_URL}" \
  --subjects="perp.processed.>" \
  --storage=file \
  --retention=limits \
  --max-msgs=-1 \
  --max-bytes=10737418240 \
  --max-age=168h \
  --max-msg-size=1048576 \
  --discard=old \
  --dupe-window=2m \
  --replicas=1 \
  --no-deny-delete \
  --no-deny-purge \
  2>/dev/null || nats stream update PERP_PROCESSED \
    --server="${NATS_URL}" \
    --subjects="perp.processed.>" \
    2>/dev/null || true

echo "  ✓ PERP_PROCESSED stream"

echo "NATS setup complete."
