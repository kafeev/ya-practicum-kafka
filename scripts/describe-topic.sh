#!/usr/bin/env bash
# Описание топика topic-1 (вывод kafka-topics.sh --describe).
set -euo pipefail

BOOTSTRAP="rc1a-6ibie76edoio2ab7.mdb.yandexcloud.net:9091"
CONFIG="config/client.properties"

kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CONFIG" \
  --describe --topic topic-1
