#!/bin/sh
set -eu
CONNECT_URL="${CONNECT_URL:-http://debezium:8083}"
until curl -fsS "$CONNECT_URL/" >/dev/null; do sleep 2; done
curl -fsS -X PUT "$CONNECT_URL/connectors/quickticket-postgres/config" \
  -H 'Content-Type: application/json' \
  --data @/config/postgres-connector.json
