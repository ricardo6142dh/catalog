# Catalog Search CDC

Laboratório executável de **System Design** para estudar como manter uma busca de
produtos rápida e atualizada sem transformar o banco transacional em motor de
busca.

O projeto conecta PostgreSQL, Debezium, Kafka, Elasticsearch e Redis. Mais
importante que a tecnologia é o problema explorado: dados atravessam sistemas
diferentes, processos falham no meio do caminho e cada decisão troca uma garantia
por outra.

## O problema

Uma loja começa pequena. A Product API salva produtos no PostgreSQL e também os
consulta ali. Funciona bem até a busca precisar de texto livre, relevância,
autocomplete e filtros combinados.

PostgreSQL continua sendo ótimo para transações, mas não queremos que consultas
de busca pesadas disputem recursos com criação de pedidos e atualização de
estoque. Criamos então uma projeção dos produtos no Elasticsearch:

```text
escritas ──> PostgreSQL          fonte de verdade
                  │
                  └────────────> Elasticsearch   visão otimizada para busca
```

Surge o problema central do laboratório: **como manter essa projeção atualizada
mesmo com falhas, retries, mensagens duplicadas e uma reindexação completa?**

## Arquitetura

```text
                                   ┌───────────────┐
                                   │     Redis     │
                                   │ cache-aside   │
                                   └───────▲───────┘
                                           │
Cliente ──busca──> Search API ─────────────┼──> Elasticsearch
                                           │       alias: products
                                           │
Cliente ──CRUD───> Product API ──> PostgreSQL
                                      │
                                      │ WAL lógico
                                      ▼
                                  Debezium ──> Kafka ──> Indexer
                                                               │
                                                               └──> Elasticsearch
                                                                    alias: products_cdc_targets
```

Responsabilidades:

- **PostgreSQL** é a fonte de verdade (golden source).
- **Product API** faz CRUD somente no PostgreSQL.
- **Debezium** lê o WAL lógico e transforma mudanças em eventos CDC.
- **Kafka** armazena esses eventos e controla a posição do consumidor.
- **Indexer** converte eventos Debezium em documentos Elasticsearch.
- **Elasticsearch** mantém a projeção otimizada para busca.
- **Redis** reduz leituras repetidas usando cache-aside.
- **Search API** lê Redis e Elasticsearch; nunca consulta PostgreSQL.

Essa separação favorece leitura rápida e escalável, mas introduz
**consistência eventual**.

## A história de uma alteração

Considere a atualização do produto `42` para preço `R$ 100`, versão `15`.

1. Product API atualiza uma linha no PostgreSQL.
2. PostgreSQL confirma a transação e grava a mudança no WAL.
3. Debezium lê o WAL e publica o evento no tópico
   `catalog.public.products`.
4. Indexer recebe o evento e grava o documento no Elasticsearch.
5. Somente após o Elasticsearch confirmar, Indexer confirma o offset no Kafka.
6. Search API passa a encontrar o novo preço.

Existe um intervalo entre os passos 1 e 6 no qual o PostgreSQL já possui o dado
novo, mas a busca ainda mostra o anterior. Isso não é acidente: é o custo aceito
por desacoplar escrita e busca.

### Por que a Product API não publica diretamente no Kafka?

Uma escrita dupla criaria dois resultados possíveis:

```text
PostgreSQL confirmou + Kafka falhou  = dado salvo, evento perdido
Kafka confirmou + PostgreSQL falhou  = evento de algo que não existe
```

Não existe transação atômica simples entre os dois sistemas. CDC evita essa
janela: a Product API confirma apenas o PostgreSQL, e Debezium captura a mudança
já persistida no WAL.

Trade-off: menos acoplamento e nenhuma escrita dupla na aplicação, ao custo de
mais infraestrutura e consistência eventual.

## Entrega at-least-once

O commit de offset é o recibo enviado pelo Indexer ao Kafka:

> “Esta mensagem terminou; não preciso recebê-la novamente.”

Por isso, a ordem correta é:

```text
Kafka entrega evento
        ↓
Indexer grava no Elasticsearch
        ↓
Elasticsearch confirma
        ↓
Indexer faz commit do offset
```

### Nunca confirmar antes de indexar

Se o offset fosse confirmado primeiro e o Elasticsearch falhasse depois, o
Kafka consideraria a mensagem concluída. O produto ficaria ausente da busca até
uma reindexação completa. Isso seria **at-most-once**: o evento pode ser
processado uma vez ou nenhuma.

Confirmar depois evita perda, mas abre outra janela:

```text
Elasticsearch grava com sucesso
        ↓
Indexer cai antes do commit
        ↓
Kafka entrega o mesmo evento novamente
```

Logo, a entrega do Kafka é **at-least-once**: antes do commit, mesmo evento pode
ser entregue uma ou mais vezes. Consumidor precisa tolerar duplicatas. Isso não
promete sucesso no Elasticsearch: falhas que esgotam retries são preservadas na
DLQ para investigação e futuro reprocessamento.

### Idempotência torna duplicatas seguras

Uma operação idempotente produz o mesmo estado quando aplicada uma ou várias
vezes.

Evento inadequado:

```text
some 5 ao estoque
```

Se entregue duas vezes, soma 10. O evento usado aqui descreve estado:

```text
produto 42 deve ter estoque 15 na versão 15
```

Aplicá-lo novamente mantém o estoque em 15. O Indexer também usa
`external_gte`, versionamento externo do Elasticsearch:

```text
documento atual: versão 15

chega versão 16 -> aceita
chega versão 15 -> duplicata segura
chega versão 14 -> rejeita como antiga
```

Assim, retries não repetem efeitos e eventos atrasados não sobrescrevem estado
novo.

### Por que não exactly-once?

Kafka e Elasticsearch são sistemas independentes. Não há uma transação única
que grave o documento e confirme o offset atomicamente. Kafka Transactions
oferece garantias dentro do ecossistema Kafka, mas não inclui uma escrita HTTP
no Elasticsearch.

Objetivo real do projeto é:

```text
at-least-once delivery + operação idempotente = efeito final correto
```

Para uma cobrança, envio físico ou outra ação que só possa produzir efeito uma
vez, a abordagem mudaria. Opções comuns são chave de idempotência no serviço
externo, tabela `processed_events` com restrição `UNIQUE`, Transactional Outbox
ou uma máquina de estados com reconciliação. Sem cooperação do destino, não é
possível prometer exatamente uma execução apenas configurando o Kafka.

## Retry e dead-letter queue

Falhas de processamento recebem até três tentativas. Se todas falharem, evento é
publicado em `catalog.public.products.dlq` antes do commit do offset original.
Esse limite impede uma mensagem problemática de bloquear partição para sempre,
mas exige monitorar e reprocessar DLQ depois de corrigir causa.

```text
evento
  ├── indexado com sucesso ─────────────> commit
  ├── duplicado/antigo ─────────────────> commit seguro
  ├── erro temporário ──────────────────> retry, sem commit
  └── erro permanente ──> DLQ confirma ─> commit
```

Confirmar o evento original antes de gravá-lo na DLQ também perderia dados. DLQ
não corrige o evento; preserva evidência e permite investigação/reprocessamento.

## Deletes e eventos antigos

Excluir fisicamente o documento do Elasticsearch permitiria que um evento antigo
o recriasse depois que os metadados internos de versão expirassem. O projeto
mantém um tombstone versionado:

```json
{
  "product_id": "42",
  "version": 20,
  "deleted": true
}
```

Busca, sugestões e detalhes escondem tombstones. A versão permanece disponível
para impedir ressurreição por eventos antigos. Custo: documentos excluídos ainda
ocupam espaço e pedem política futura de compactação segura.

## Cache-aside e seus trade-offs

Search API usa Redis como cache-aside:

```text
requisição
  ├── cache hit  ─> responde Redis
  └── cache miss ─> consulta Elasticsearch ─> grava Redis ─> responde
```

Decisões importantes:

- chave de busca é canônica e recebe hash SHA-256;
- resultados expiram por TTL;
- detalhe de produto é invalidado pelo Indexer;
- falha do Redis faz bypass para Elasticsearch (fail-open);
- resultado vazio não é cacheado.

Não cachear vazio importa durante convergência CDC. Logo após criar um produto,
uma busca pode ocorrer antes da indexação e retornar vazia. Guardar esse vazio
por dois minutos faria o produto parecer ausente mesmo depois de indexado.

Caches de buscas com resultado não vazio não são invalidados a cada alteração.
Uma atualização ou exclusão pode permanecer visível até TTL expirar. Invalidar
todas combinações possíveis de texto, filtros, ordenação e paginação custaria
muito; projeto aceita essa janela de staleness. Cache de detalhe é simples de
endereçar por `product:<id>` e, por isso, recebe invalidação explícita.

Fail-open prioriza disponibilidade, mas pode transferir carga repentina para o
Elasticsearch quando Redis cai. Em produção, circuit breaker, rate limit e
observabilidade seriam necessários.

## Reindex sem parar escritas

Mappings e analyzers não podem ser alterados livremente em um índice existente.
Reindex cria índice versionado novo e troca um alias ao final.

Exemplo:

```text
products ───────────────> products_v1

durante reindex:
products_cdc_targets ───> products_v1
                     └──> products_v2

depois da validação:
products ───────────────> products_v2
```

`make reindex` executa:

1. adquire advisory lock no PostgreSQL para impedir dois reindexes simultâneos;
2. cria `products_v<timestamp>` com mapping novo;
3. adiciona índice novo ao alias de escrita `products_cdc_targets`;
4. copia produtos ativos do PostgreSQL usando `external_gte`;
5. valida contagem e versão de cada produto;
6. espera lag do consumer group chegar a zero em duas leituras consecutivas;
7. valida novamente e troca alias `products` atomicamente;
8. remove índices antigos dos alvos CDC.

Eventos continuam chegando aos índices antigo e novo durante o backfill. Índice
anterior não é apagado, permitindo rollback manual. Custo: escrita duplicada e
uso extra de disco durante migração.

## Consistência e garantias

| Situação | Comportamento |
|---|---|
| Escrita recém-confirmada | Pode demorar alguns instantes para aparecer na busca |
| Evento duplicado | Seguro por operação idempotente e `external_gte` |
| Evento antigo | Não sobrescreve versão nova |
| Elasticsearch fora | Três tentativas; depois DLQ; offset só é confirmado após DLQ |
| Evento inválido permanente | Vai para DLQ antes do commit |
| Redis fora | Busca ignora cache e consulta Elasticsearch |
| Elasticsearch fora em cache miss | Search API retorna `503` |
| Reindex falha antes do switch | Alias de leitura continua no índice anterior |

## Pré-requisitos

- Docker com Compose v2;
- pelo menos 4 GiB disponíveis para stack;
- `curl`;
- Go, apenas para comandos executados fora dos containers.

## Como subir e testar

```bash
cd ~/repo/catalog
cp .env.example .env       # opcional; valores padrão já funcionam
make up
docker compose ps
make smoke
```

Primeira subida baixa imagens e pode levar alguns minutos. Serviços one-shot
`kafka-init`, `debezium-setup` e `elasticsearch-setup` devem terminar com código
zero; isso significa inicialização concluída, não falha.

Smoke test cria produto único, espera propagação CDC, testa busca e cache, exclui
produto e confirma que ele desapareceu. Passar nesse teste demonstra caminho
completo, não apenas health checks isolados.

Para carregar dados de exemplo:

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

Criar produto:

```bash
curl -fsS -X POST http://localhost:8081/products \
  -H 'Content-Type: application/json' \
  -d '{"name":"Apple iPhone","description":"Smartphone","brand":"Apple","category":"phones","price":999,"stock":10}'
```

Buscar:

```bash
curl -fsS 'http://localhost:8082/search?q=iphone&brand=Apple&min_price=500&sort=price_asc&limit=10&page=1'
```

`/search` aceita `q`, `brand`, `category`, `min_price`, `max_price`,
`sort=relevance|price_asc`, `limit` e `page`. `/suggest` oferece autocomplete.
`/healthz` existe nas duas APIs.

## Portas

| Porta | Serviço | Papel |
|---:|---|---|
| 5432 | PostgreSQL | fonte de verdade |
| 9092 | Kafka | acesso do host ao broker |
| 8083 | Kafka Connect | REST do Debezium |
| 8090 | Kafka UI | inspeção de tópicos e connector |
| 6379 | Redis | cache-aside |
| 9200 | Elasticsearch | motor de busca |
| 8081 | Product API | CRUD |
| 8082 | Search API | busca, sugestões e detalhes |

Portas podem ser alteradas pelas variáveis em `.env.example`. Comunicação interna
usa nomes do Compose e portas fixas.

## Comandos úteis

```bash
make up               # sobe stack em background
make down             # para containers e preserva volumes
make reset            # DESTRUTIVO: remove containers e volumes locais
make logs             # acompanha logs
make config           # valida/renderiza Compose
make smoke            # testa fluxo ponta a ponta
make kafka-topics     # lista tópicos
make debezium-status  # mostra estado do connector
make check            # formata, testa, executa vet e compila binários
make seed             # cria produtos de exemplo
make reindex          # rebuild versionado e troca segura do alias
```

## Experimentos para estudo

### 1. Observar consistência eventual

Crie produto e consulte imediatamente PostgreSQL e Search API. Repita até
observar pequeno atraso entre fonte e projeção.

### 2. Simular indisponibilidade do Redis

```bash
docker compose stop redis
make search
docker compose start redis
```

Busca deve continuar via Elasticsearch. Compare latência e logs.

### 3. Simular retry e DLQ do Indexer

Pare Elasticsearch e altere um produto. Indexer fará três tentativas; logs
mostram erro final e envio à DLQ, não cada tentativa. Offset original só avança
depois que escrita na DLQ é confirmada. Subir
Elasticsearch novamente não reaplica evento: produto continuará desatualizado
até reprocessar DLQ ou executar reindex. Replay automatizado da DLQ é melhoria
proposta, não funcionalidade atual.

### 4. Inspecionar eventos

Abra Kafka UI em `http://localhost:8090` e examine:

- tópico `catalog.public.products`;
- payload Debezium antes/depois;
- partição e offset;
- consumer group `catalog-indexer`;
- tópico `catalog.public.products.dlq`.

### 5. Executar reindex online

```bash
make reindex
curl -fsS 'http://localhost:9200/_cat/aliases?v'
curl -fsS 'http://localhost:9200/_cat/indices/products_v*?v'
```

Faça atualizações enquanto reindex roda e confirme que índice novo termina com
versões corretas.

## Diagnóstico rápido

Se `make smoke` disser que produto não chegou à busca, cheque cadeia na ordem:

```bash
docker compose ps
make debezium-status
make kafka-topics
docker compose logs --tail=100 indexer
curl -fsS 'http://localhost:9200/_cat/aliases?v'
```

Perguntas úteis:

1. Transação chegou ao PostgreSQL?
2. Debezium connector e task estão `RUNNING`?
3. Evento apareceu no tópico CDC?
4. Indexer consumiu ou enviou para DLQ?
5. Alias `products` aponta para índice compatível?
6. Search API respondeu do Redis ou Elasticsearch?

Reiniciar tudo sem localizar etapa quebrada geralmente apaga evidência. Ótimo
para superstição, ruim para engenharia.

## Verificações manuais

```bash
# PostgreSQL e WAL lógico
docker compose exec postgres pg_isready -U catalog -d catalog
docker compose exec postgres psql -U catalog -d catalog -Atc "SHOW wal_level"

# Kafka e Debezium
docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:29092
curl -fsS http://localhost:8083/connectors/catalog-postgres-connector/status

# Redis e Elasticsearch
docker compose exec redis redis-cli ping
curl -fsS 'http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=5s'
```

## Limites e próximos passos

Projeto privilegia clareza didática, não prontidão de produção. Melhorias
naturais:

- métricas Prometheus para lag, retries, DLQ, cache hit e latência;
- alertas para connector parado e crescimento de lag;
- autenticação, TLS e gestão de secrets;
- múltiplas partições e teste de ordenação por chave;
- política de compactação/remoção segura de tombstones;
- circuit breaker e proteção contra avalanche após falha do Redis;
- procedimento automatizado para reprocessar DLQ;
- testes de caos e carga.

Logs JSON já incluem request, produto, versão, partição, offset e erros de
cache/DLQ.

## Segurança local

Credenciais são intencionalmente simples. Elasticsearch roda sem autenticação.
Não exponha esta stack em rede pública. Volumes nomeados preservam dados entre
reinícios; `make reset` remove esses dados de forma irreversível.
