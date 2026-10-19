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
curl --fail --silent "http://localhost:${ELASTICSEARCH_PORT:-9200}/_alias/products" >/dev/null
curl --fail --silent "http://localhost:${ELASTICSEARCH_PORT:-9200}/_alias/products_cdc_targets" >/dev/null

product_name="SmokeProduct$(date +%s)"
product_response="$(curl --fail --silent \
  -X POST "http://localhost:${PRODUCT_API_PORT:-8081}/products" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"$product_name\",\"description\":\"CDC smoke test\",\"brand\":\"smoke\",\"category\":\"tests\",\"price\":1,\"stock\":1}")"
product_id="$(printf '%s' "$product_response" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
test -n "$product_id"

attempt=0
while [ "$attempt" -lt 30 ]; do
  search_response="$(curl --fail --silent "http://localhost:${SEARCH_API_PORT:-8082}/search?q=$product_name" || true)"
  if printf '%s' "$search_response" | grep -q "$product_id"; then
	  curl --fail --silent "http://localhost:${SEARCH_API_PORT:-8082}/products/$product_id" >/dev/null
    curl --fail --silent -X DELETE "http://localhost:${PRODUCT_API_PORT:-8081}/products/$product_id" >/dev/null
	  delete_attempt=0
	  while [ "$delete_attempt" -lt 30 ]; do
	    status="$(curl --silent --output /dev/null --write-out '%{http_code}' "http://localhost:${SEARCH_API_PORT:-8082}/products/$product_id")"
	    if [ "$status" = "404" ]; then
	      echo "End-to-end create/search/cache/delete smoke test passed"
	      exit 0
	    fi
	    delete_attempt=$((delete_attempt + 1))
	    sleep 1
	  done
	  echo "Product $product_id did not disappear from detail cache within 30 seconds" >&2
	  exit 1
  fi
  attempt=$((attempt + 1))
  sleep 1
done

echo "Product $product_id did not reach search within 30 seconds" >&2
exit 1
