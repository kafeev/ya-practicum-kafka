#!/bin/bash
set -euo pipefail

# ============================================================================
# Генерация сертификатов для SSL-кластера Kafka (3 брокера)
#
# Создаёт:
#   - самоподписанный CA (certificate authority)
#   - keystore + подписанный сертификат для каждого брокера (kafka1..kafka3)
#   - общий truststore (содержит только CA)
#   - client keystores для производителя/потребителя/админа (для ACL)
#   - client truststore
#
# Запуск (из корня проекта):
#   docker run --rm -v ${PWD}/ssl:/ssl -w /ssl confluentinc/cp-kafka:7.0.1 bash generate-certs.sh
# ============================================================================

PASSWORD=${PASSWORD:-kafka123}
VALIDITY_DAYS=${VALIDITY_DAYS:-365}
OUTPUT_DIR=${OUTPUT_DIR:-certs}
CA_CN=${CA_CN:-Kafka-CA}
ORG=${ORG:-Example}
LOCATION=${LOCATION:-Moscow}
STATE=${STATE:-MSK}
COUNTRY=${COUNTRY:-RU}

mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR"

echo ">>> 1/5 Генерация Certificate Authority"
openssl req -new -x509 \
  -keyout ca-key \
  -out ca-cert \
  -days "$VALIDITY_DAYS" \
  -passout "pass:${PASSWORD}" \
  -subj "/CN=${CA_CN}/OU=Security/O=${ORG}/L=${LOCATION}/ST=${STATE}/C=${COUNTRY}" 2>/dev/null

echo ">>> 2/5 Truststore с CA (общий для брокеров и клиентов)"
keytool -keystore kafka.truststore.jks -alias CARoot -import -file ca-cert \
  -storepass "$PASSWORD" -noprompt >/dev/null 2>&1

echo ">>> 3/5 Keystore и сертификаты брокеров"
for i in 1 2 3; do
  BROKER="kafka${i}"
  echo "    --- ${BROKER}"
  keytool -keystore "${BROKER}.keystore.jks" -alias "${BROKER}" -validity "$VALIDITY_DAYS" \
    -genkeypair -keyalg RSA -keysize 2048 \
    -storetype PKCS12 -storepass "$PASSWORD" -keypass "$PASSWORD" \
    -dname "CN=${BROKER},OU=Brokers,O=${ORG},L=${LOCATION},ST=${STATE},C=${COUNTRY}" >/dev/null 2>&1

  keytool -keystore "${BROKER}.keystore.jks" -alias "${BROKER}" -certreq \
    -file "${BROKER}.csr" -storepass "$PASSWORD" >/dev/null 2>&1

  # SAN нужно указывать при подписи (openssl x509 -req не копирует SAN из CSR)
  cat > "${BROKER}-san.cnf" <<EOF
subjectAltName=DNS:${BROKER},DNS:localhost,IP:127.0.0.1
EOF

  openssl x509 -req -CA ca-cert -CAkey ca-key -in "${BROKER}.csr" \
    -out "${BROKER}-cert-signed.crt" -days "$VALIDITY_DAYS" \
    -CAcreateserial -passin "pass:${PASSWORD}" \
    -extfile "${BROKER}-san.cnf" 2>/dev/null

  keytool -keystore "${BROKER}.keystore.jks" -alias CARoot -import -file ca-cert \
    -storepass "$PASSWORD" -noprompt >/dev/null 2>&1
  keytool -keystore "${BROKER}.keystore.jks" -alias "${BROKER}" -import \
    -file "${BROKER}-cert-signed.crt" -storepass "$PASSWORD" -noprompt >/dev/null 2>&1
  rm -f "${BROKER}.csr" "${BROKER}-cert-signed.crt" "${BROKER}-san.cnf"
done

echo ">>> 4/5 Client keystores (для ACL и авторизации)"
for CLIENT in producer consumer admin; do
  echo "    --- ${CLIENT}"
  keytool -keystore "client.${CLIENT}.keystore.jks" -alias "${CLIENT}" -validity "$VALIDITY_DAYS" \
    -genkeypair -keyalg RSA -keysize 2048 \
    -storetype PKCS12 -storepass "$PASSWORD" -keypass "$PASSWORD" \
    -dname "CN=${CLIENT},OU=Clients,O=${ORG},L=${LOCATION},ST=${STATE},C=${COUNTRY}" >/dev/null 2>&1

  keytool -keystore "client.${CLIENT}.keystore.jks" -alias "${CLIENT}" -certreq \
    -file "${CLIENT}.csr" -storepass "$PASSWORD" >/dev/null 2>&1

  openssl x509 -req -CA ca-cert -CAkey ca-key -in "${CLIENT}.csr" \
    -out "${CLIENT}-cert-signed.crt" -days "$VALIDITY_DAYS" \
    -CAcreateserial -passin "pass:${PASSWORD}" 2>/dev/null

  keytool -keystore "client.${CLIENT}.keystore.jks" -alias CARoot -import -file ca-cert \
    -storepass "$PASSWORD" -noprompt >/dev/null 2>&1
  keytool -keystore "client.${CLIENT}.keystore.jks" -alias "${CLIENT}" -import \
    -file "${CLIENT}-cert-signed.crt" -storepass "$PASSWORD" -noprompt >/dev/null 2>&1
  rm -f "${CLIENT}.csr" "${CLIENT}-cert-signed.crt"
done

echo ">>> 5/5 Client truststore"
cp kafka.truststore.jks client.truststore.jks

echo ">>> 6/6 PEM-экспорт клиентских сертификатов (для Python/kafka-python)"
for CLIENT in producer consumer admin; do
  # Открытый сертификат в PEM
  keytool -exportcert -keystore "client.${CLIENT}.keystore.jks" -alias "${CLIENT}" \
    -rfc -file "client.${CLIENT}-cert.pem" -storepass "$PASSWORD" >/dev/null 2>&1
  # Приватный ключ в PEM (keystore уже PKCS12)
  openssl pkcs12 -in "client.${CLIENT}.keystore.jks" -passin "pass:${PASSWORD}" \
    -passout "pass:${PASSWORD}" -nodes -nocerts \
    -out "client.${CLIENT}-key.pem" 2>/dev/null
done

# 7. Генерация сертификата для analytics (User:analytics)
echo "=== Generating analytics client certificate ==="
keytool -keystore client.analytics.keystore.jks -alias client-analytics -validity 365 \
    -genkey -storepass kafka123 -keypass kafka123 -dname "CN=analytics, OU=Kafka, O=Confluent, L=PaloAlto, ST=Ca, C=US" \
    -ext SAN=DNS:analytics

# Экспорт сертификата для подписи CA
keytool -keystore client.analytics.keystore.jks -alias client-analytics -certreq -file client.analytics.csr -storepass kafka123

# Подпись CA
openssl x509 -req -CA ca-cert -CAkey ca-key -in client.analytics.csr -out client.analytics-signed.crt -days 365 -CAcreateserial -passin pass:kafka123

# Импорт CA в клиентский keystore
keytool -keystore client.analytics.keystore.jks -alias CARoot -import -file ca-cert -storepass kafka123 -noprompt

# Импорт подписанного сертификата
keytool -keystore client.analytics.keystore.jks -alias client-analytics -import -file client.analytics-signed.crt -storepass kafka123

echo ""
echo "Готово! Файлы в ${OUTPUT_DIR}:"
ls -la
