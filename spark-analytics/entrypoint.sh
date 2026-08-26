#!/bin/bash
set -e

export SPARK_HOME=/opt/spark
export PATH="$SPARK_HOME/bin:$PATH"

echo "Starting Spark Analytics Job..."

# Проверяем наличие сертификатов
if [ ! -d "/opt/bitnami/spark/ssl/certs" ]; then
    echo "ERROR: SSL certificates not found!"
    exit 1
fi

# Запускаем Spark job (jars уже в classpath образа)
exec spark-submit \
    --master local[*] \
    /opt/spark-apps/analytics.py