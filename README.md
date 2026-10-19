# Catalog Search CDC

Laboratório local e executável de busca de catálogo para estudar golden source,
CDC, Kafka, indexação idempotente, cache-aside e reindex sem bloquear escritas.

## Arquitetura

```text
Product API -> PostgreSQL -> Debezium -> Kafka -> Indexer -> Elasticsearch
Search API  -> Redis (cache-aside) -> Elasticsearch
```

Product API escreve somente no PostgreSQL. Debezium registra mudanças no Kafka.
Indexer aplica eventos em todos índices do alias `products_cdc_targets`, usando
versionamento externo para ignorar eventos antigos. Search API lê somente do
alias `products`; Redis falha aberto para Elasticsearch.

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
| 6379 | Redis | cache-aside de buscas e detalhes |
| 9200 | Elasticsearch | HTTP API do motor de busca |
| 8081 | Product API | CRUD da golden source |
| 8082 | Search API | busca, sugestões e detalhes |

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
make check            # fmt, testes, vet e build dos quatro binários
make seed             # cria seis produtos pela Product API
make reindex          # rebuild versionado e troca segura do alias
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

`/search` usa cache-aside Redis e consulta o alias Elasticsearch `products` no
miss. Aceita `q`, `brand`, `category`, `min_price`, `max_price`,
`sort=relevance|price_asc`, `limit` e `page`. `/suggest` oferece autocomplete.
`/products/{id}` também usa cache direto. Falha Redis não derruba busca.
`/healthz` existe nas duas APIs.

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

## Reindex

`make reindex` executa:

1. lock advisory no PostgreSQL;
2. criação de `products_v<timestamp>`;
3. inclusão do novo índice em `products_cdc_targets`;
4. backfill de produtos ativos com `external_gte`;
5. refresh, validação de contagem e versões por produto;
6. espera do consumer group atingir lag zero em duas leituras consecutivas;
7. nova validação de versões e troca atômica do alias `products`;
8. remoção dos índices antigos dos alvos CDC.

Índice anterior não é apagado e permanece disponível para rollback manual. Se o
processo falhar antes da troca, alias de leitura continua intacto.

Via Compose:

```bash
docker compose --profile tools run --rm reindex
```

## Garantias e limites

- PostgreSQL é fonte de verdade; Product API não publica Kafka.
- Offsets Kafka são confirmados somente depois de aplicar evento.
- Falhas recebem três tentativas limitadas; poison messages vão para
  `catalog.public.products.dlq` antes do commit do offset.
- `external_gte` torna duplicatas seguras e rejeita versão antiga.
- Cache de busca expira por TTL; cache direto é invalidado pelo indexer.
- Redis indisponível causa bypass, não indisponibilidade da busca.
- Elasticsearch indisponível em cache miss retorna `503`.
- Métricas Prometheus ficam como melhoria operacional; logs JSON já expõem
  request, produto, versão, partição, offset e erros de cache/DLQ.
