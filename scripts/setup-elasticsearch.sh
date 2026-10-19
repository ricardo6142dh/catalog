#!/bin/sh
set -eu

base_url="${ELASTICSEARCH_URL:-http://elasticsearch:9200}"

if ! curl --fail --silent "$base_url/products_v1" >/dev/null 2>&1; then
  curl --fail --silent --show-error \
    -X PUT "$base_url/products_v1" \
    -H 'Content-Type: application/json' \
    --data-binary @/config/products-index.json >/dev/null
fi

if ! curl --fail --silent "$base_url/_alias/products" >/dev/null 2>&1; then
  curl --fail --silent --show-error \
    -X POST "$base_url/_aliases" \
    -H 'Content-Type: application/json' \
    -d '{"actions":[{"add":{"index":"products_v1","alias":"products","is_write_index":true}}]}' >/dev/null
fi

if ! curl --fail --silent "$base_url/_alias/products_cdc_targets" >/dev/null 2>&1; then
  curl --fail --silent --show-error \
    -X POST "$base_url/_aliases" \
    -H 'Content-Type: application/json' \
    -d '{"actions":[{"add":{"index":"products_v1","alias":"products_cdc_targets"}}]}' >/dev/null
fi
