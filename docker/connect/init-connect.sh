#!/bin/sh
# Register file-source and file-sink connectors after Connect REST is up.
set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
CONFIGS_DIR="${CONFIGS_DIR:-/connectors}"

echo "==> waiting for Kafka Connect at ${CONNECT_URL}"
for i in $(seq 1 60); do
  if curl -sf "${CONNECT_URL}/" >/dev/null 2>&1; then
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "Kafka Connect not ready" >&2
    exit 1
  fi
  sleep 2
done

register() {
  file="$1"
  name=$(basename "$file" .json)
  echo "==> connector ${name}"
  code=$(curl -s -o /tmp/connect-out.txt -w "%{http_code}" \
    -X POST \
    -H "Content-Type: application/json" \
    --data-binary "@${file}" \
    "${CONNECT_URL}/connectors")
  if [ "$code" = "201" ] || [ "$code" = "409" ]; then
    echo "    ok (HTTP ${code})"
    return 0
  fi
  echo "    failed HTTP ${code}:" >&2
  cat /tmp/connect-out.txt >&2
  return 1
}

register "${CONFIGS_DIR}/file-source.json"
register "${CONFIGS_DIR}/file-sink.json"

echo "==> connectors"
curl -sf "${CONNECT_URL}/connectors?expand=status" | head -c 2000 || true
echo ""
echo "==> done"
echo "    pipeline: /data/events.json -> topic connect.events -> /data/sink.json"
echo "    TUI: key 7 / :connect  (kafka_connect.url = http://localhost:8083)"
