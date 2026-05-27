#!/bin/sh
# Append one JSON line every ~2s — source for file-source connector.
set -eu

mkdir -p /data
: >/data/events.json 2>/dev/null || true

echo "events-generator: appending JSON lines to /data/events.json (every 2s)"
n=0
while true; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  n=$((n + 1))
  printf '{"id":%s,"ts":"%s","source":"events-generator","payload":"demo event %s"}\n' \
    "$n" "$ts" "$n" >>/data/events.json
  sleep 2
done
