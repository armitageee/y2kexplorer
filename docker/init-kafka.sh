#!/usr/bin/env bash
# Создаёт демо-топики и заливает тестовые сообщения.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
BIN="/opt/kafka/bin"

echo "==> waiting for broker at ${BOOTSTRAP}"
for i in $(seq 1 60); do
  if "${BIN}/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; then
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
)

for spec in "${TOPICS[@]}"; do
  topic="${spec%%:*}"
  parts="${spec##*:}"
  "${BIN}/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${parts}" \
    --replication-factor 1
  echo "    ${topic} (${parts} partitions)"
done

echo "==> producing sample messages"
produce() {
  local topic="$1"
  local file="$2"
  "${BIN}/kafka-console-producer.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic "${topic}" \
    < "${file}"
}

produce orders /sample-messages/orders.jsonl
produce users.events /sample-messages/users.events.jsonl
produce test.notifications /sample-messages/notifications.jsonl
produce payments.retry /sample-messages/payments.jsonl

echo "==> topic list"
"${BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list

echo "==> done — connect y2k to localhost:9092"
