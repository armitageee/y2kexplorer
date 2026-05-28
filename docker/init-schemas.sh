#!/bin/sh
# Регистрирует демо-схемы в Schema Registry (Confluent-compatible REST API).
set -eu

SR="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
HDR="Content-Type: application/vnd.schemaregistry.v1+json"
SCHEMAS_DIR="${SCHEMAS_DIR:-/schemas}"

echo "==> waiting for schema registry at ${SR}"
for i in $(seq 1 60); do
  if curl -sf "${SR}/subjects" >/dev/null 2>&1; then
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "schema registry not ready" >&2
    exit 1
  fi
  sleep 2
done

register_schema() {
  subject="$1"
  file="$2"
  echo "    ${subject}"
  curl -sf -X POST "${SR}/subjects/${subject}/versions" \
    -H "${HDR}" \
    --data-binary "@${file}"
  echo ""
}

echo "==> registering schemas"
register_schema "orders-value" "${SCHEMAS_DIR}/orders-value.json"
register_schema "users.events-value" "${SCHEMAS_DIR}/users.events-value.json"
register_schema "payments.retry-value" "${SCHEMAS_DIR}/payments.retry-value.json"

echo "==> subjects"
curl -sf "${SR}/subjects"
echo ""
echo "==> done — Schema Registry: ${SR}"
