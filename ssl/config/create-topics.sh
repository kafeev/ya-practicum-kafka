#!/bin/bash
set -eu

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
  --create --if-not-exists --topic topic-1 --partitions "$PARTITIONS" --replication-factor "$REPLICATION" --config min.insync.replicas=2
kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --create --if-not-exists --topic topic-2 --partitions "$PARTITIONS" --replication-factor "$REPLICATION" --config min.insync.replicas=2

# Топики аналитической платформы маркетплейса
kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --create --if-not-exists --topic products --partitions "$PARTITIONS" --replication-factor "$REPLICATION" --config min.insync.replicas=2
kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --create --if-not-exists --topic client_requests --partitions "$PARTITIONS" --replication-factor "$REPLICATION" --config min.insync.replicas=2

# Топики фильтрации запрещённых товаров (Шаг 4, Faust)
kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --create --if-not-exists --topic forbidden-products --partitions "$PARTITIONS" --replication-factor "$REPLICATION" --config min.insync.replicas=2 --config cleanup.policy=compact
kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --create --if-not-exists --topic products-allowed --partitions "$PARTITIONS" --replication-factor "$REPLICATION" --config min.insync.replicas=2
kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --create --if-not-exists --topic products-rejected --partitions "$PARTITIONS" --replication-factor "$REPLICATION" --config min.insync.replicas=2

echo ""
echo ">>> Топики:"
kafka-topics --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" --list

# Шаг 3.1: Создание топиков в РЕЗЕРВНОМ кластере
# Аналитика (Spark) читает primary.client_requests и пишет recommendations
# именно в резервный кластер (KAFKA_BOOTSTRAP=backup1:9195,...).
BACKUP_BOOTSTRAP=backup1:9195
BACKUP_ADMIN=/etc/kafka/ssl-config/admin-backup.properties

# Проверяем, доступен ли резервный кластер (пробуем получить метаданные)
if kafka-topics --bootstrap-server "$BACKUP_BOOTSTRAP" --command-config "$BACKUP_ADMIN" --list >/dev/null 2>&1; then
  echo ">>> Создание топика recommendations в резервном кластере (backup)..."
  kafka-topics --bootstrap-server "$BACKUP_BOOTSTRAP" --command-config "$BACKUP_ADMIN" \
    --create --if-not-exists --topic recommendations \
    --partitions 3 --replication-factor 3 \
    --config min.insync.replicas=2

  # Топик для сырых данных (используется Spark для landing в HDFS, опционально)
  echo ">>> Создание топика raw_data в резервном кластере (backup)..."
  kafka-topics --bootstrap-server "$BACKUP_BOOTSTRAP" --command-config "$BACKUP_ADMIN" \
    --create --if-not-exists --topic raw_data \
    --partitions 3 --replication-factor 3 \
    --config min.insync.replicas=2

  echo ""
  echo ">>> Топики резервного кластера (backup):"
  kafka-topics --bootstrap-server "$BACKUP_BOOTSTRAP" --command-config "$BACKUP_ADMIN" --list
else
  echo ""
  echo ">>> Резервный кластер (backup1:9195) недоступен — пропускаем создание топиков."
  echo "    Запустите backup-брокеры: docker compose up -d backup1 backup2 backup3"
  echo "    Затем создайте топики вручную или перезапустите этот скрипт."
fi