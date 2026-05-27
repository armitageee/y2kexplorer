#!/usr/bin/env bash
# Создаёт демо-топики, заливает сообщения и sample ACL (SASL/PLAIN + StandardAuthorizer).
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
COMMAND_CONFIG="${KAFKA_COMMAND_CONFIG:-}"
BIN="/opt/kafka/bin"

cmd_config_args=()
if [[ -n "${COMMAND_CONFIG}" && -f "${COMMAND_CONFIG}" ]]; then
  cmd_config_args=(--command-config "${COMMAND_CONFIG}")
fi

echo "==> waiting for broker at ${BOOTSTRAP}"
for i in $(seq 1 60); do
  if "${BIN}/kafka-broker-api-versions.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    "${cmd_config_args[@]}" >/dev/null 2>&1; then
    break
  fi
  if [[ "${i}" -eq 60 ]]; then
    echo "Kafka not ready" >&2
    exit 1
  fi
  sleep 2
done

echo "==> creating topics"
TOPICS=(
  "orders:3"
  "users.events:3"
  "test.notifications:1"
  "payments.retry:2"
  "connect.events:1"
)

for spec in "${TOPICS[@]}"; do
  topic="${spec%%:*}"
  parts="${spec##*:}"
  "${BIN}/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    "${cmd_config_args[@]}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${parts}" \
    --replication-factor 1
  echo "    ${topic} (${parts} partitions)"
done

echo "==> kafka connect internal topics (cleanup.policy=compact)"
CONNECT_TOPICS=(
  "connect-configs:1"
  "connect-offsets:25"
  "connect-status:1"
)
for spec in "${CONNECT_TOPICS[@]}"; do
  topic="${spec%%:*}"
  parts="${spec##*:}"
  "${BIN}/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    "${cmd_config_args[@]}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${parts}" \
    --replication-factor 1 \
    --config cleanup.policy=compact
  "${BIN}/kafka-configs.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    "${cmd_config_args[@]}" \
    --alter \
    --entity-type topics \
    --entity-name "${topic}" \
    --add-config cleanup.policy=compact >/dev/null 2>&1 || true
  echo "    ${topic} (${parts} partitions, compact)"
done

echo "==> producing sample messages"
produce() {
  local topic="$1"
  local file="$2"
  local producer_args=(--bootstrap-server "${BOOTSTRAP}" --topic "${topic}")
  if [[ -n "${COMMAND_CONFIG}" && -f "${COMMAND_CONFIG}" ]]; then
    producer_args+=(--producer.config "${COMMAND_CONFIG}")
  fi
  "${BIN}/kafka-console-producer.sh" "${producer_args[@]}" < "${file}"
}

produce orders /sample-messages/orders.jsonl
produce users.events /sample-messages/users.events.jsonl
produce test.notifications /sample-messages/notifications.jsonl
produce payments.retry /sample-messages/payments.jsonl

echo "==> sample ACLs for User:app (Read+Describe on orders only)"
"${BIN}/kafka-acls.sh" \
  --bootstrap-server "${BOOTSTRAP}" \
  "${cmd_config_args[@]}" \
  --add \
  --allow-principal User:app \
  --operation Read \
  --operation Describe \
  --topic orders \
  --force

echo "==> topic list"
"${BIN}/kafka-topics.sh" \
  --bootstrap-server "${BOOTSTRAP}" \
  "${cmd_config_args[@]}" \
  --list

echo "==> ACL list (principal User:app)"
"${BIN}/kafka-acls.sh" \
  --bootstrap-server "${BOOTSTRAP}" \
  "${cmd_config_args[@]}" \
  --list \
  --principal User:app

echo "==> done"
echo "    y2k: cluster local, admin/admin-secret (SASL/PLAIN, ACL admin)"
echo "    test limited user: app/app-secret (read orders only)"
