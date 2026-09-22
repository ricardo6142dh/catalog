#!/bin/sh
set -eu

product_api_url="${PRODUCT_API_URL:-http://localhost:8081}"

create_product() {
  curl --fail --show-error --silent \
    --request POST \
    --header "Content-Type: application/json" \
    --data "$1" \
    "${product_api_url}/products"
  echo
}

create_product '{"name":"Apple iPhone 15","description":"Smartphone Apple","brand":"Apple","category":"phones","price":999.00,"stock":12}'
create_product '{"name":"Samsung Galaxy","description":"Smartphone Android","brand":"Samsung","category":"phones","price":699.00,"stock":20}'
create_product '{"name":"Notebook ThinkPad","description":"Laptop para trabalho","brand":"Lenovo","category":"computers","price":1299.00,"stock":7}'
create_product '{"name":"TV OLED","description":"Televisão 4K","brand":"LG","category":"televisions","price":1599.00,"stock":4}'
create_product '{"name":"Wireless Headphones","description":"Headphones bluetooth","brand":"Sony","category":"audio","price":249.00,"stock":15}'
create_product '{"name":"USB-C Charger","description":"Carregador rápido","brand":"Anker","category":"accessories","price":49.00,"stock":50}'
