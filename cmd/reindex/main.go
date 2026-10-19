package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/config"
	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/database"
	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/search"
	"github.com/segmentio/kafka-go"
)

const advisoryLockID int64 = 0x434154414c4f47

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("reindex failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	postgresTimeout, err := config.Duration("POSTGRES_CONNECT_TIMEOUT", 5*time.Second)
	if err != nil {
		return err
	}
	pool, err := database.Open(ctx, config.String("DATABASE_URL", "postgres://catalog:catalog@localhost:5432/catalog?sslmode=disable"), postgresTimeout)
	if err != nil {
		return err
	}
	defer pool.Close()
	elasticsearchTimeout, err := config.Duration("ELASTICSEARCH_TIMEOUT", 5*time.Second)
	if err != nil {
		return err
	}
	index, err := search.NewElasticsearch(config.String("ELASTICSEARCH_URL", "http://localhost:9200"), elasticsearchTimeout)
	if err != nil {
		return err
	}
	definition, err := loadDefinition(config.String("PRODUCTS_INDEX_DEFINITION", "search/products-index.json"))
	if err != nil {
		return err
	}
	catchupTimeout, err := config.Duration("REINDEX_CATCHUP_TIMEOUT", 60*time.Second)
	if err != nil {
		return err
	}
	brokers := splitNonEmpty(config.String("KAFKA_BROKERS", "localhost:9092"))
	barrier := func(ctx context.Context) error {
		return waitForConsumerCatchUp(ctx, brokers, config.String("KAFKA_TOPIC", "catalog.public.products"), config.String("KAFKA_GROUP_ID", "catalog-indexer"), catchupTimeout)
	}
	return rebuild(ctx, logger, pool, index, definition, barrier)
}

func rebuild(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool, index *search.ElasticsearchClient, definition any, catchUp func(context.Context) error) error {
	connection, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire reindex connection: %w", err)
	}
	defer connection.Release()
	if _, err := connection.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryLockID); err != nil {
		return fmt.Errorf("acquire reindex lock: %w", err)
	}
	defer connection.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", advisoryLockID) //nolint:errcheck

	oldTargets, err := index.AliasIndices(ctx, search.ProductsCDCTargetsAlias)
	if err != nil {
		return err
	}
	newIndex := "products_v" + time.Now().UTC().Format("20060102150405_000000000")
	if err := index.CreateProductsIndex(ctx, newIndex, definition); err != nil {
		return err
	}
	if err := index.AddAliasTarget(ctx, search.ProductsCDCTargetsAlias, newIndex); err != nil {
		return err
	}
	aliasSwitched := false
	defer func() {
		if !aliasSwitched {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := index.RemoveAliasTarget(cleanupCtx, search.ProductsCDCTargetsAlias, newIndex); err != nil {
				logger.Error("remove failed reindex target", "index", newIndex, "error", err)
			}
		}
	}()
	logger.Info("reindex target created", "index", newIndex)

	indexed, err := backfill(ctx, pool, index, newIndex)
	if err != nil {
		return err
	}
	if err := index.RefreshIndex(ctx, newIndex); err != nil {
		return err
	}
	var activeCount int64
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM products WHERE active").Scan(&activeCount); err != nil {
		return fmt.Errorf("count active products: %w", err)
	}
	indexCount, err := index.CountIndex(ctx, newIndex)
	if err != nil {
		return err
	}
	if indexCount != activeCount {
		return fmt.Errorf("reindex validation failed: postgres=%d Elasticsearch=%d", activeCount, indexCount)
	}
	if err := validateVersions(ctx, pool, index, newIndex); err != nil {
		return err
	}
	if err := catchUp(ctx); err != nil {
		return fmt.Errorf("wait for indexer catch-up: %w", err)
	}
	if err := validateVersions(ctx, pool, index, newIndex); err != nil {
		return fmt.Errorf("post-catch-up validation: %w", err)
	}
	if err := index.SwitchProductsAlias(ctx, newIndex); err != nil {
		return err
	}
	aliasSwitched = true
	for _, oldIndex := range oldTargets {
		if oldIndex != newIndex {
			if err := index.RemoveAliasTarget(ctx, search.ProductsCDCTargetsAlias, oldIndex); err != nil {
				return err
			}
		}
	}
	logger.Info("reindex completed", "index", newIndex, "backfilled", indexed, "validated", indexCount, "rollback_indices", oldTargets)
	return nil
}

func waitForConsumerCatchUp(ctx context.Context, brokers []string, topic, groupID string, timeout time.Duration) error {
	if len(brokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}
	barrierCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 5 * time.Second}
	stable := 0
	for {
		caughtUp, err := consumerCaughtUp(barrierCtx, client, topic, groupID)
		if err == nil && caughtUp {
			stable++
			if stable == 2 {
				return nil
			}
		} else {
			stable = 0
		}
		select {
		case <-barrierCtx.Done():
			if err != nil {
				return err
			}
			return fmt.Errorf("consumer group %q did not reach zero lag", groupID)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func consumerCaughtUp(ctx context.Context, client *kafka.Client, topic, groupID string) (bool, error) {
	metadata, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return false, err
	}
	partitions := make([]int, 0)
	for _, topicMetadata := range metadata.Topics {
		if topicMetadata.Name == topic {
			for _, partition := range topicMetadata.Partitions {
				partitions = append(partitions, partition.ID)
			}
		}
	}
	if len(partitions) == 0 {
		return false, fmt.Errorf("topic %q has no partitions", topic)
	}
	offsets, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{Addr: client.Addr, GroupID: groupID, Topics: map[string][]int{topic: partitions}})
	if err != nil {
		return false, err
	}
	requests := make([]kafka.OffsetRequest, 0, len(partitions))
	for _, partition := range partitions {
		requests = append(requests, kafka.LastOffsetOf(partition))
	}
	ends, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Addr: client.Addr, Topics: map[string][]kafka.OffsetRequest{topic: requests}})
	if err != nil {
		return false, err
	}
	committed := make(map[int]int64, len(partitions))
	for _, partition := range offsets.Topics[topic] {
		if partition.Error != nil {
			return false, partition.Error
		}
		committed[partition.Partition] = partition.CommittedOffset
	}
	for _, partition := range ends.Topics[topic] {
		if partition.Error != nil {
			return false, partition.Error
		}
		if committed[partition.Partition] < partition.LastOffset {
			return false, nil
		}
	}
	return true, nil
}

func splitNonEmpty(raw string) []string {
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func validateVersions(ctx context.Context, pool *pgxpool.Pool, index *search.ElasticsearchClient, target string) error {
	rows, err := pool.Query(ctx, "SELECT id::text, version FROM products WHERE active ORDER BY id")
	if err != nil {
		return fmt.Errorf("query products for validation: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var productID string
		var version int64
		if err := rows.Scan(&productID, &version); err != nil {
			return fmt.Errorf("scan product validation row: %w", err)
		}
		document, err := index.GetProductFrom(ctx, target, productID)
		if err != nil {
			return fmt.Errorf("validate product %s: %w", productID, err)
		}
		if document.Version != version {
			return fmt.Errorf("validate product %s: postgres version=%d Elasticsearch version=%d", productID, version, document.Version)
		}
	}
	return rows.Err()
}

func backfill(ctx context.Context, pool *pgxpool.Pool, index *search.ElasticsearchClient, target string) (int64, error) {
	rows, err := pool.Query(ctx, `
		SELECT id::text, name, description, brand, category, price::float8, stock, version, updated_at
		FROM products WHERE active ORDER BY id`)
	if err != nil {
		return 0, fmt.Errorf("query active products: %w", err)
	}
	defer rows.Close()
	var count int64
	for rows.Next() {
		var document search.ProductDocument
		if err := rows.Scan(&document.ProductID, &document.Name, &document.Description, &document.Brand, &document.Category, &document.Price, &document.Stock, &document.Version, &document.UpdatedAt); err != nil {
			return count, fmt.Errorf("scan backfill product: %w", err)
		}
		if err := index.IndexProductTo(ctx, target, document); err != nil && !errors.Is(err, search.ErrStaleVersion) {
			return count, fmt.Errorf("backfill product %s: %w", document.ProductID, err)
		}
		count++
	}
	return count, rows.Err()
}

func loadDefinition(path string) (any, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read index definition: %w", err)
	}
	var definition any
	if err := json.Unmarshal(payload, &definition); err != nil {
		return nil, fmt.Errorf("decode index definition: %w", err)
	}
	return definition, nil
}
