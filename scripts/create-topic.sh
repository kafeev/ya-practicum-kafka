#!/usr/bin/env bash
# Создание топика topic-1: 3 партиции, коэффициент репликации 3.
set -euo pipefail

BOOTSTRAP="rc1a-6ibie76edoio2ab7.mdb.yandexcloud.net:9091"
CONFIG="config/client.properties"

kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CONFIG" \
  --create \
  --topic topic-1 \
  --partitions 12 \
  --replication-factor 3 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config segment.bytes=1073741824 \
  --config min.insync.replicas=2
