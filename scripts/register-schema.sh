#!/usr/bin/env bash
# Регистрация Avro-схемы в Schema Registry (http://localhost:8081).
set -euo pipefail

SUBJECT="topic-1-value"
SCHEMA_FILE="schema.avsc"

PAYLOAD=$(jq -n --arg schema "$(cat "$SCHEMA_FILE")" '{schema: $schema}')

curl -X POST "http://localhost:8081/subjects/${SUBJECT}/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "$PAYLOAD"
