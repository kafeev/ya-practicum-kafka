#!/bin/bash
set -euo pipefail

# ============================================================================
# Создание топиков topic-1, topic-2.
#
# Требования:
#   - кластер должен быть запущен:  docker compose up -d
#   - сертификаты сгенерированы:    ssl/certs/*
#
# Запуск:
#   docker compose exec kafka1 bash /etc/kafka/ssl-config/create-topics.sh
#
# (скрипт монтируется в kafka-контейнеры как /etc/kafka/ssl-config)
# ============================================================================

BOOTSTRAP=${BOOTSTRAP:-kafka1:9095,kafka2:9096,kafka3:9097}
ADMIN_CONFIG=${ADMIN_CONFIG:-/etc/kafka/ssl-config/admin.properties}
PARTITIONS=${PARTITIONS:-3}
REPLICATION=${REPLICATION:-3}

echo ">>> Создание топиков"
kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --create --if-not-exists --topic topic-1 --partitions "$PARTITIONS" --replication-factor "$REPLICATION"
kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --create --if-not-exists --topic topic-2 --partitions "$PARTITIONS" --replication-factor "$REPLICATION"

# Топики аналитической платформы маркетплейса
kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --create --if-not-exists --topic products --partitions "$PARTITIONS" --replication-factor "$REPLICATION"
kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --create --if-not-exists --topic client_requests --partitions "$PARTITIONS" --replication-factor "$REPLICATION"

echo ""
echo ">>> Топики:"
kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" --list