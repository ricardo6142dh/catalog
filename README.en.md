# Catalog Search CDC

Executable laboratory for **System Design** studying how to keep product search fast and up-to-date without turning the transactional database into a search engine.

The project connects PostgreSQL, Debezium, Kafka, Elasticsearch and Redis. More important than the technology is the problem explored: data flows through different systems, processes fail mid-stream and every decision trades one guarantee for another.

## The Problem

A store starts small. The Product API saves products to PostgreSQL and queries from there. Works fine until search needs free text, relevance, autocomplete and combined filters.

PostgreSQL remains excellent for transactions, but we don't want heavy search queries competing with order creation and inventory updates. So we create a projection of products in Elasticsearch:

```text
writes ──> PostgreSQL          source of truth
                │
                └────────────> Elasticsearch   optimized view for search
```

The lab's central problem emerges: **how to keep this projection current despite failures, retries, duplicate messages and a full reindex?**

## Architecture

```text
                                   ┌───────────────┐
                                   │     Redis     │
                                   │ cache-aside   │
                                   └───────▲───────┘
                                           │
Client ──search──> Search API ─────────────┼──> Elasticsearch
                                           │       alias: products
                                           │
Client ──CRUD───> Product API ──> PostgreSQL
                                      │
                                      │ logical WAL
                                      ▼
                                  Debezium ──> Kafka ──> Indexer
                                                               │
                                                               └──> Elasticsearch
                                                                    alias: products_cdc_targets
```

Responsibilities:

- **PostgreSQL** is the source of truth (golden source).
- **Product API** performs CRUD only on PostgreSQL.
- **Debezium** reads the logical WAL and transforms changes into CDC events.
- **Kafka** stores these events and manages consumer position.
- **Indexer** converts Debezium events into Elasticsearch documents.
- **Elasticsearch** maintains the projection optimized for search.
- **Redis** reduces repeated reads using cache-aside.
- **Search API** reads Redis and Elasticsearch; never queries PostgreSQL.

This separation favors fast and scalable reads but introduces **eventual consistency**.

## The Story of a Change

Consider updating product `42` to price `$100`, version `15`.

1. Product API updates a row in PostgreSQL.
2. PostgreSQL confirms the transaction and writes the change to WAL.
3. Debezium reads the WAL and publishes the event to topic `catalog.public.products`.
4. Indexer receives the event and writes the document to Elasticsearch.
5. Only after Elasticsearch confirms, Indexer commits the offset to Kafka.
6. Search API now finds the new price.

There is a window between steps 1 and 6 where PostgreSQL already has the new data, but search still shows the old one. This is not accidental: it's the cost accepted for decoupling writes and search.

### Why doesn't the Product API publish directly to Kafka?

A dual write would create two possible outcomes:

```text
PostgreSQL confirmed + Kafka failed  = data saved, event lost
Kafka confirmed + PostgreSQL failed  = event for something that doesn't exist
```

There is no simple atomic transaction across both systems. CDC avoids this window: the Product API confirms only PostgreSQL, and Debezium captures the change already persisted in WAL.

Trade-off: less coupling and no dual writes in the application, at the cost of more infrastructure and eventual consistency.

## At-Least-Once Delivery

The offset commit is the receipt sent by the Indexer to Kafka:

> "This message is done; don't send it again."

That's why the correct order is:

```text
Kafka delivers event
        ↓
Indexer writes to Elasticsearch
        ↓
Elasticsearch confirms
        ↓
Indexer commits offset
```

### Never confirm before indexing

If the offset were committed first and Elasticsearch failed next, Kafka would consider the message completed. The product would remain absent from search until a full reindex. This would be **at-most-once**: the event may be processed once or never.

Committing later avoids loss but opens another window:

```text
Elasticsearch writes successfully
        ↓
Indexer crashes before commit
        ↓
Kafka delivers the same event again
```

Thus, Kafka delivery is **at-least-once**: before the commit, the same event may be delivered one or more times. Consumer must tolerate duplicates. This doesn't promise success in Elasticsearch: failures that exhaust retries are preserved in the DLQ for investigation and future reprocessing.

### Idempotence makes duplicates safe

An idempotent operation produces the same state when applied once or many times.

Poor event:

```text
add 5 to stock
```

If delivered twice, adds 10. The event here describes state:

```text
product 42 must have stock 15 at version 15
```

Applying it again keeps stock at 15. The Indexer also uses `external_gte`, external versioning in Elasticsearch:

```text
current document: version 15

arrives version 16 -> accepts
arrives version 15 -> safe duplicate
arrives version 14 -> rejects as stale
```

This way, retries don't repeat effects and late events don't overwrite new state.

### Why not exactly-once?

Kafka and Elasticsearch are independent systems. There is no single transaction that writes the document and commits the offset atomically. Kafka Transactions offer guarantees within the Kafka ecosystem, but don't include an HTTP write to Elasticsearch.

Real goal of the project is:

```text
at-least-once delivery + idempotent operation = correct final effect
```

For a charge, physical shipment or other action that can only produce effect once, the approach would change. Common options are idempotency key in the external service, `processed_events` table with `UNIQUE` constraint, Transactional Outbox or a state machine with reconciliation. Without cooperation from the destination, it's impossible to promise exactly one execution just by configuring Kafka.

## Retry and Dead-Letter Queue

Processing failures receive up to three attempts. If all fail, event is published to `catalog.public.products.dlq` before committing the original offset. This limit prevents a problematic message from blocking the partition forever, but requires monitoring and reprocessing DLQ after fixing the cause.

```text
event
  ├── indexed successfully ─────────────> commit
  ├── duplicate/stale ─────────────────> safe commit
  ├── temporary error ──────────────────> retry, no commit
  └── permanent error ──> DLQ confirms ─> commit
```

Confirming the original event before writing it to DLQ would also lose data. DLQ doesn't fix the event; it preserves evidence and allows investigation/reprocessing.

## Deletes and Stale Events

Physically deleting the document from Elasticsearch would allow a stale event to recreate it after internal version metadata expired. The project keeps a versioned tombstone:

```json
{
  "product_id": "42",
  "version": 20,
  "deleted": true
}
```

Search, suggestions and details hide tombstones. The version remains available to prevent resurrection by stale events. Cost: deleted documents still consume space and require a future safe compaction policy.

## Cache-Aside and Its Trade-Offs

Search API uses Redis as cache-aside:

```text
request
  ├── cache hit  ─> respond Redis
  └── cache miss ─> query Elasticsearch ─> write Redis ─> respond
```

Important decisions:

- search key is canonical and receives SHA-256 hash;
- results expire by TTL;
- product detail is invalidated by Indexer;
- Redis failure causes bypass to Elasticsearch (fail-open);
- empty result is not cached.

Not caching empty matters during CDC convergence. Right after creating a product, a search may occur before indexing and return empty. Storing that empty for two minutes would make the product seem absent even after indexing.

Search caches with non-empty results are not invalidated on every change. An update or deletion may remain visible until TTL expires. Invalidating all possible combinations of text, filters, sort and pagination would cost too much; project accepts this staleness window. Detail cache is simple to address by `product:<id>` and therefore receives explicit invalidation.

Fail-open prioritizes availability but may transfer sudden load to Elasticsearch when Redis goes down. In production, circuit breaker, rate limit and observability would be necessary.

## Reindex Without Stopping Writes

Mappings and analyzers cannot be freely altered in an existing index. Reindex creates a new versioned index and swaps an alias at the end.

Example:

```text
products ───────────────> products_v1

during reindex:
products_cdc_targets ───> products_v1
                     └──> products_v2

after validation:
products ───────────────> products_v2
```

`make reindex` executes:

1. acquires advisory lock on PostgreSQL to prevent simultaneous reindexes;
2. creates `products_v<timestamp>` with new mapping;
3. adds new index to `products_cdc_targets` write alias;
4. copies active products from PostgreSQL using `external_gte`;
5. validates count and version of each product;
6. waits for consumer group lag to reach zero in two consecutive reads;
7. validates again and atomically swaps `products` alias;
8. removes old indices from CDC targets.

Events keep arriving to old and new indices during backfill. Previous index is not deleted, allowing manual rollback. Cost: duplicate writes and extra disk usage during migration.

## Consistency and Guarantees

| Situation | Behavior |
|---|---|
| Freshly confirmed write | May take a few moments to appear in search |
| Duplicate event | Safe by idempotent operation and `external_gte` |
| Stale event | Does not overwrite new version |
| Elasticsearch down | Three attempts; then DLQ; offset commits only after DLQ |
| Permanently invalid event | Goes to DLQ before commit |
| Redis down | Search ignores cache and queries Elasticsearch |
| Elasticsearch down on cache miss | Search API returns `503` |
| Reindex fails before switch | Read alias remains on previous index |

## Prerequisites

- Docker with Compose v2;
- at least 4 GiB available for stack;
- `curl`;
- Go, only for commands run outside containers.

## How to Start and Test

```bash
cd ~/repo/catalog
cp .env.example .env       # optional; defaults already work
make up
docker compose ps
make smoke
```

First startup downloads images and may take a few minutes. One-shot services `kafka-init`, `debezium-setup` and `elasticsearch-setup` should exit with code zero; this means initialization is complete, not failure.

Smoke test creates a single product, waits for CDC propagation, tests search and cache, deletes product and confirms it disappeared. Passing this test demonstrates the complete path, not just isolated health checks.

To load example data:

```bash
make seed
make search
```

## APIs

```text
POST   http://localhost:8081/products
GET    http://localhost:8081/products/{id}
PUT    http://localhost:8081/products/{id}
DELETE http://localhost:8081/products/{id}

GET    http://localhost:8082/search
GET    http://localhost:8082/suggest
GET    http://localhost:8082/products/{id}
```

Create product:

```bash
curl -fsS -X POST http://localhost:8081/products \
  -H 'Content-Type: application/json' \
  -d '{"name":"Apple iPhone","description":"Smartphone","brand":"Apple","category":"phones","price":999,"stock":10}'
```

Search:

```bash
curl -fsS 'http://localhost:8082/search?q=iphone&brand=Apple&min_price=500&sort=price_asc&limit=10&page=1'
```

`/search` accepts `q`, `brand`, `category`, `min_price`, `max_price`, `sort=relevance|price_asc`, `limit` and `page`. `/suggest` offers autocomplete. `/healthz` exists on both APIs.

## Ports

| Port | Service | Role |
|---:|---|---|
| 5432 | PostgreSQL | source of truth |
| 9092 | Kafka | host access to broker |
| 8083 | Kafka Connect | Debezium REST |
| 8090 | Kafka UI | inspect topics and connector |
| 6379 | Redis | cache-aside |
| 9200 | Elasticsearch | search engine |
| 8081 | Product API | CRUD |
| 8082 | Search API | search, suggestions and details |

Ports can be changed by variables in `.env.example`. Internal communication uses Compose names and fixed ports.

## Useful Commands

```bash
make up               # start stack in background
make down             # stop containers and preserve volumes
make reset            # DESTRUCTIVE: removes containers and local volumes
make logs             # follow logs
make config           # validate/render Compose
make smoke            # test end-to-end flow
make kafka-topics     # list topics
make debezium-status  # show connector status
make check            # format, test, vet and compile binaries
make seed             # create example products
make reindex          # versioned rebuild and safe alias swap
```

## Experiments for Study

### 1. Observe Eventual Consistency

Create product and immediately query PostgreSQL and Search API. Repeat until you observe small delay between source and projection.

### 2. Simulate Redis Unavailability

```bash
docker compose stop redis
make search
docker compose start redis
```

Search should continue via Elasticsearch. Compare latency and logs.

### 3. Simulate Indexer Retry and DLQ

Stop Elasticsearch and alter a product. Indexer will make three attempts; logs show final error and DLQ send, not each attempt. Original offset only advances after DLQ write is confirmed. Starting Elasticsearch again does not replay event: product remains outdated until reprocessing DLQ or running reindex. Automated DLQ replay is proposed improvement, not current feature.

### 4. Inspect Events

Open Kafka UI at `http://localhost:8090` and examine:

- topic `catalog.public.products`;
- Debezium payload before/after;
- partition and offset;
- consumer group `catalog-indexer`;
- topic `catalog.public.products.dlq`.

### 5. Run Online Reindex

```bash
make reindex
curl -fsS 'http://localhost:9200/_cat/aliases?v'
curl -fsS 'http://localhost:9200/_cat/indices/products_v*?v'
```

Make updates while reindex runs and confirm new index ends with correct versions.

## Quick Diagnosis

If `make smoke` says product didn't reach search, check chain in order:

```bash
docker compose ps
make debezium-status
make kafka-topics
docker compose logs --tail=100 indexer
curl -fsS 'http://localhost:9200/_cat/aliases?v'
```

Useful questions:

1. Did transaction reach PostgreSQL?
2. Are Debezium connector and task `RUNNING`?
3. Did event appear in CDC topic?
4. Did Indexer consume or send to DLQ?
5. Does alias `products` point to compatible index?
6. Did Search API respond from Redis or Elasticsearch?

Restarting everything without locating broken step usually erases evidence. Great for superstition, bad for engineering.

## Manual Checks

```bash
# PostgreSQL and logical WAL
docker compose exec postgres pg_isready -U catalog -d catalog
docker compose exec postgres psql -U catalog -d catalog -Atc "SHOW wal_level"

# Kafka and Debezium
docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:29092
curl -fsS http://localhost:8083/connectors/catalog-postgres-connector/status

# Redis and Elasticsearch
docker compose exec redis redis-cli ping
curl -fsS 'http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=5s'
```

## Limits and Next Steps

Project favors didactic clarity, not production readiness. Natural improvements:

- Prometheus metrics for lag, retries, DLQ, cache hit and latency;
- alerts for stopped connector and lag growth;
- authentication, TLS and secrets management;
- multiple partitions and key ordering tests;
- safe compaction/removal policy for tombstones;
- circuit breaker and protection against avalanche after Redis failure;
- automated DLQ reprocessing;
- chaos and load tests.

JSON logs already include request, product, version, partition, offset and cache/DLQ errors.

## Local Security

Credentials are intentionally simple. Elasticsearch runs without authentication. Do not expose this stack on public network. Named volumes preserve data between restarts; `make reset` removes that data irreversibly.
