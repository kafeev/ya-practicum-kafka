#!/bin/bash
set -eu
BOOTSTRAP=${BOOTSTRAP:-kafka1:9095,kafka2:9096,kafka3:9097}
ADMIN_CONFIG=${ADMIN_CONFIG:-/etc/kafka/ssl-config/admin.properties}

echo ">>> topic-1: продюсер (read, write) + консьюмер (read)"
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:producer \
  --operation Read --operation Write --operation Describe --topic topic-1
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:consumer \
  --operation Read --operation Describe --topic topic-1

echo ""
echo ">>> topic-2: продюсер может отправлять (write), консьюмер НЕ может читать (только Describe)"
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:producer \
  --operation Write --operation Describe --topic topic-2
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:consumer \
  --operation Describe --topic topic-2

echo ""
echo ">>> Группы: консьюмеру нужен доступ к consumer group"
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:consumer \
  --operation Read --operation Describe --group '*'

echo ""
echo ">>> products: продюсер пишет, консьюмер (product-sink) читает"
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:producer \
  --operation Read --operation Write --operation Describe --topic products
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:consumer \
  --operation Read --operation Describe --topic products

echo ">>> forbidden-filter (Faust): продюсер читает/пишет служебные топики фильтрации"
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:producer \
  --operation Read --operation Write --operation Describe --topic forbidden-products
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:producer \
  --operation Write --operation Describe --topic products-allowed
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:producer \
  --operation Write --operation Describe --topic products-rejected
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:consumer \
  --operation Read --operation Describe --topic products-allowed

echo ">>> forbidden-filter: внутренние топики Faust (assignor/reply) по префиксу forbidden-filter-"
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:producer \
  --operation All --topic forbidden-filter- --resource-pattern-type prefixed
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:producer \
  --operation Create --cluster

echo ">>> forbidden-filter: доступ к группе консьюмера (FindCoordinator требует Describe на группу)"
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:producer \
  --operation Read --operation Describe --group forbidden-filter
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:producer \
  --operation Read --operation Describe --group 'forbidden-filter-*'
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:producer \
  --operation Read --operation Describe --group forbidden-cli-list

echo ""
echo ">>> client_requests: продюсер (CLIENT API) пишет события запросов"
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" \
  --add --allow-principal User:producer \
  --operation Write --operation Describe --topic client_requests

echo ""
echo ">>> ACL:"
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" --list

# Шаг 3.2: ACL для аналитики (В РЕЗЕРВНОМ кластере, т.к. Spark работает с backup)
# ===== ACL для аналитики =====
BACKUP_BOOTSTRAP=backup1:9195
BACKUP_ADMIN=/etc/kafka/ssl-config/admin-backup.properties
echo ">>> Setting ACL for analytics on backup cluster..."

# Аналитика читает из резервного кластера primary.client_requests
kafka-acls --bootstrap-server "$BACKUP_BOOTSTRAP" \
  --command-config "$BACKUP_ADMIN" \
  --add --allow-principal User:analytics \
  --operation DESCRIBE --operation READ --topic "primary.client_requests" \
  --group "*"

# Аналитика пишет в recommendations (в резервном кластере)
kafka-acls --bootstrap-server "$BACKUP_BOOTSTRAP" \
  --command-config "$BACKUP_ADMIN" \
  --add --allow-principal User:analytics \
  --operation DESCRIBE --operation WRITE --topic recommendations

# Аналитика читает products (если нужно для обогащения)
kafka-acls --bootstrap-server "$BACKUP_BOOTSTRAP" \
  --command-config "$BACKUP_ADMIN" \
  --add --allow-principal User:analytics \
  --operation DESCRIBE --operation READ --topic "primary.products"

# Консьюмер может читать рекомендации
kafka-acls --bootstrap-server "$BACKUP_BOOTSTRAP" \
  --command-config "$BACKUP_ADMIN" \
  --add --allow-principal User:consumer \
  --operation DESCRIBE --operation READ --topic recommendations

echo "=== Current ACLs (backup) ==="
kafka-acls --bootstrap-server "$BACKUP_BOOTSTRAP" \
  --command-config "$BACKUP_ADMIN" \
  --list