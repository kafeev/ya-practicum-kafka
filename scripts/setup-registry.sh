#!/usr/bin/env bash
set -euo pipefail

# Set before running:
export CLUSTER_ID=c9qshk2r61aaebr3vqho
export REGISTRY_PASSWORD=registry1
CLUSTER_ID="${CLUSTER_ID:?CLUSTER_ID is not set}"
REGISTRY_PASSWORD="${REGISTRY_PASSWORD:?REGISTRY_PASSWORD is not set}"

yc managed-kafka user create "$CLUSTER_ID" --name registry --password "$REGISTRY_PASSWORD"
yc managed-kafka topic create "$CLUSTER_ID" --name _schemas --partitions 1 --replication-factor 3 --cleanup-policy CLEANUP_POLICY_COMPACT
yc managed-kafka topic grant-permission "$CLUSTER_ID" --topic-name _schemas --user-name registry --role PRODUCER
yc managed-kafka topic grant-permission "$CLUSTER_ID" --topic-name _schemas --user-name registry --role CONSUMER
echo "Done: user registry and topic _schemas created"
