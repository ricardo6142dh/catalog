.PHONY: up down reset logs config smoke kafka-topics debezium-status seed fmt test vet build check integration-test search reindex

up:
	docker compose up --build -d

down:
	docker compose down

reset:
	@echo "WARNING: this deletes all local PostgreSQL, Kafka, Redis, and Elasticsearch data."
	docker compose down --volumes

logs:
	docker compose logs --follow

config:
	docker compose config

smoke:
	sh ./scripts/smoke-test.sh

kafka-topics:
	docker compose exec kafka kafka-topics --bootstrap-server localhost:29092 --list

debezium-status:
	curl --fail --silent http://localhost:$${KAFKA_CONNECT_PORT:-8083}/connectors/catalog-postgres-connector/status

seed:
	sh ./scripts/seed-products.sh

test:
	go test ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

build:
	go build ./cmd/product-api ./cmd/search-api ./cmd/indexer ./cmd/reindex

check: fmt test vet build

integration-test:
	$(MAKE) smoke

search:
	curl --fail --silent "http://localhost:$${SEARCH_API_PORT:-8082}/search?q=iphone&page=1"

reindex:
	go run ./cmd/reindex
