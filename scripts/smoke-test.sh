#!/bin/sh
set -eu

compose="docker compose"

$compose exec -T postgres pg_isready -U catalog -d catalog
$compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:29092 >/dev/null
connector_status="$(curl --fail --silent "http://localhost:${KAFKA_CONNECT_PORT:-8083}/connectors/catalog-postgres-connector/status")"
echo "$connector_status" | grep -q '"connector":{"state":"RUNNING"'
echo "$connector_status" | grep -q '"tasks":\[{"id":0,"state":"RUNNING"'
$compose exec -T redis redis-cli ping | grep PONG >/dev/null
curl --fail --silent "http://localhost:${ELASTICSEARCH_PORT:-9200}/_cluster/health?wait_for_status=yellow&timeout=5s" >/dev/null
curl --fail --silent "http://localhost:${PRODUCT_API_PORT:-8081}/healthz" >/dev/null
curl --fail --silent "http://localhost:${SEARCH_API_PORT:-8082}/healthz" >/dev/null

echo "Infrastructure smoke test passed"
