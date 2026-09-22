#!/bin/sh
set -eu

connect_url="${KAFKA_CONNECT_URL:-http://kafka-connect:8083}"
connector_name="${DEBEZIUM_CONNECTOR_NAME:-catalog-postgres-connector}"

echo "Registering Debezium connector ${connector_name}"
curl --fail --show-error --silent \
  --request PUT \
  --header "Content-Type: application/json" \
  --data @/config/postgres-connector.json \
  "${connect_url}/connectors/${connector_name}/config"
echo

curl --fail --show-error --silent \
  "${connect_url}/connectors/${connector_name}/status"
echo

