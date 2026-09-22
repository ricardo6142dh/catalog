# Catalog Search CDC — learning scaffold

Laboratório local de busca de catálogo. Infraestrutura do Milestone 1 está
validada. Product API funciona contra PostgreSQL. Search API e indexer compilam,
conectam às dependências e deixam exercícios centrais explícitos como TODOs.

## Arquitetura futura

```text
Product API -> PostgreSQL -> Debezium -> Kafka -> Indexer -> Elasticsearch
Search API  -> Redis (cache-aside) -> Elasticsearch
```

Product API escreve somente no PostgreSQL. Debezium registra mudanças no Kafka.
Search API e indexer ainda não completam fluxo de leitura/indexação: isso é parte
dos exercícios abaixo.

## Pré-requisitos

- Docker com Compose v2
- pelo menos 4 GiB disponíveis para stack local
- `curl`

## Início rápido

```bash
cd ~/repo/catalog
cp .env.example .env       # opcional; defaults funcionam sem arquivo
docker compose config
docker compose up --build
```

`debezium-setup` é container one-shot. Ele registra/atualiza connector via
`PUT /connectors/catalog-postgres-connector/config` e deve terminar com código
zero. Para subir em background e verificar tudo:

```bash
docker compose up --build -d
docker compose ps
make smoke
make debezium-status
make kafka-topics
```

## Portas

| Porta | Processo | Uso |
|---:|---|---|
| 5432 | PostgreSQL | golden source; database/user/password `catalog` |
| 9092 | Kafka | acesso do host |
| 8083 | Kafka Connect | REST API e estado do Debezium |
| 8090 | Kafka UI | inspeção de brokers, tópicos e connector |
| 6379 | Redis | cache futuro |
| 9200 | Elasticsearch | HTTP API do motor de busca |
| 8081 | Product API | CRUD da golden source |
| 8082 | Search API | endpoints scaffold de busca |

Portas podem ser trocadas pelas variáveis documentadas em `.env.example`.
Comunicação interna usa nomes Compose e portas internas fixas.

## Verificações manuais

```bash
# PostgreSQL e WAL lógico
docker compose exec postgres pg_isready -U catalog -d catalog
docker compose exec postgres psql -U catalog -d catalog -Atc "SHOW wal_level"

# Kafka KRaft
docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:29092

# Kafka Connect e connector
curl -fsS http://localhost:8083/connectors
curl -fsS http://localhost:8083/connectors/catalog-postgres-connector/status

# Redis
docker compose exec redis redis-cli ping

# Elasticsearch
curl -fsS 'http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=5s'
```

Registro manual/idempotente do connector:

```bash
curl -fsS -X PUT \
  -H 'Content-Type: application/json' \
  --data @debezium/postgres-connector.json \
  http://localhost:8083/connectors/catalog-postgres-connector/config
```

Tópico CDC esperado após primeira mudança capturada em `products`:

```text
catalog.public.products
```

## Comandos

```bash
make up               # sobe stack em background
make down             # para containers; preserva volumes
make reset            # DESTRUTIVO: remove containers e volumes locais
make logs             # acompanha logs
make config           # valida/renderiza Compose
make smoke            # testa cinco health endpoints/processos
make kafka-topics     # lista tópicos
make debezium-status  # mostra estado do connector
make check            # fmt, testes, vet e build dos três binários
make seed             # cria seis produtos pela Product API
```

## Dados e segurança local

Volumes nomeados preservam PostgreSQL, Kafka, Redis e Elasticsearch. Credenciais
são intencionalmente simples e adequadas somente ao ambiente local. Elasticsearch
roda sem autenticação. Não expor esta stack em rede pública.

## APIs disponíveis

```text
POST   http://localhost:8081/products
GET    http://localhost:8081/products/{id}
PUT    http://localhost:8081/products/{id}
DELETE http://localhost:8081/products/{id}

GET    http://localhost:8082/search
GET    http://localhost:8082/suggest
GET    http://localhost:8082/products/{id}
```

`/search` consulta diretamente o alias Elasticsearch `products`, sem Redis ou
outro cache. Aceita `q`, `brand`, `category`, `min_price`, `max_price`,
`sort=relevance|price_asc`, `limit` e `page`. `/suggest` permanece como exercício
e retorna `501 Not Implemented`. `/healthz` existe nas duas APIs.

Exemplo de busca:

```bash
curl -fsS 'http://localhost:8082/search?q=iphone&brand=Apple&min_price=500&sort=price_asc&limit=10&page=1'
```

Exemplo Product API:

```bash
curl -fsS -X POST http://localhost:8081/products \
  -H 'Content-Type: application/json' \
  -d '{"name":"Apple iPhone","description":"Smartphone","brand":"Apple","category":"phones","price":999,"stock":10}'
```

## Learning TODOs

Testes-guia usam `t.Skip`. Remova skip somente ao começar respectivo exercício.

| Arquivo | Função | Comportamento esperado | Teste relacionado |
|---|---|---|---|
| `internal/cache/key.go` | `BuildSearchCacheKey()` | normalizar caixa/valores, ordenar representação e incluir todo parâmetro que muda resposta | `go test ./internal/cache/...` |
| `internal/search/elasticsearch.go` | `IndexProduct()` | usar `product_id` como `_id` | `go test ./internal/search/... -run LearningIndexProduct` |
| `internal/indexer/handler.go` | `ApplyProductEvent()` | transformar evento, indexar/delete e impedir versão velha de vencer | `go test ./internal/indexer/... -run Learning` |
| `internal/search/elasticsearch.go` | `SwitchProductsAlias()` | trocar atomicamente `products` para novo índice | `go test ./internal/search/... -run LearningSwitchProductsAlias` |
| `internal/search/elasticsearch.go` | `CreateProductsIndex()` e futuro `cmd/reindex` | criar `products_vN`, backfill, validar e trocar alias sem bloquear writes | adicione testes em `internal/search` e `test/integration` |

TODO adicional de autocomplete, fora dos sete exercícios principais:
`ElasticsearchClient.SuggestProducts()`.

Para listar marcações:

```bash
grep -R -n 'TODO(ricardo)' internal cmd
```

## Escopo adiado

- escrita final do indexer;
- autocomplete real;
- cache-aside;
- índices versionados, aliases e reindex;
- Milestone 7: experimentos completos de resiliência.

## Desvios conscientes do desenho final

- índice versionado, mapping e aliases Elasticsearch permanecem exercícios;
- Search API consulta Elasticsearch diretamente; Redis/cache-aside permanece
  adiado para o Milestone 5;
- indexer encerra com erro ao receber primeiro evento, porque
  `ApplyProductEvent()` permanece intencionalmente não implementado.
