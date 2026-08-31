#!/bin/bash
set -euo pipefail
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
echo ">>> ACL:"
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" --list