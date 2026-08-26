FROM spark:3.5.1

USER root

# Устанавливаем Python-зависимости
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Копируем скрипты
COPY analytics.py /opt/spark-apps/
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

# Домашняя директория для кэша ivy (spark-submit --packages)
RUN mkdir -p /home/spark && chown -R spark:spark /home/spark

# Предзагрузка spark-sql-kafka и зависимостей в Spark classpath (без --packages в рантайме)
RUN set -e; \
  BASE=https://repo1.maven.org/maven2; \
  J=/opt/spark/jars; \
  curl -sSL -o $J/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    $BASE/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar; \
  curl -sSL -o $J/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    $BASE/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar; \
  curl -sSL -o $J/kafka-clients-3.4.1.jar \
    $BASE/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar; \
  curl -sSL -o $J/lz4-java-1.8.0.jar \
    $BASE/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar; \
  curl -sSL -o $J/snappy-java-1.1.8.4.jar \
    $BASE/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar; \
  curl -sSL -o $J/zstd-jni-1.5.2-1.jar \
    $BASE/com/github/luben/zstd-jni/1.5.2-1/zstd-jni-1.5.2-1.jar; \
  curl -sSL -o $J/hadoop-client-runtime-3.3.4.jar \
    $BASE/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar; \
  curl -sSL -o $J/hadoop-client-api-3.3.4.jar \
    $BASE/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar; \
  curl -sSL -o $J/commons-logging-1.1.3.jar \
    $BASE/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar; \
  curl -sSL -o $J/jsr305-3.0.0.jar \
    $BASE/com/google/code/findbugs/jsr305/3.0.0/jsr305-3.0.0.jar; \
  curl -sSL -o $J/commons-pool2-2.11.1.jar \
    $BASE/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar; \
  ls -la $J | grep -E "kafka|hadoop-client|zstd|snappy|lz4|commons-logging|jsr305|commons-pool2"

USER spark

ENTRYPOINT ["/opt/entrypoint.sh"]
