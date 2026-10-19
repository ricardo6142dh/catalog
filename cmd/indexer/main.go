package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ricardo6142dh/catalog/internal/cache"
	"github.com/ricardo6142dh/catalog/internal/config"
	"github.com/ricardo6142dh/catalog/internal/indexer"
	"github.com/ricardo6142dh/catalog/internal/search"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("indexer stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	brokers := splitNonEmpty(config.String("KAFKA_BROKERS", "localhost:9092"))
	if len(brokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}
	connectTimeout, err := config.Duration("KAFKA_CONNECT_TIMEOUT", 5*time.Second)
	if err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	dialer := net.Dialer{}
	connection, err := dialer.DialContext(probeCtx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("connect Kafka broker %s: %w", brokers[0], err)
	}
	_ = connection.Close()
	logger.Info("connected to Kafka", "brokers", brokers)

	elasticsearchTimeout, err := config.Duration("ELASTICSEARCH_TIMEOUT", 3*time.Second)
	if err != nil {
		return err
	}
	elasticsearch, err := search.NewElasticsearch(config.String("ELASTICSEARCH_URL", "http://localhost:9200"), elasticsearchTimeout)
	if err != nil {
		return err
	}
	if err := elasticsearch.Health(probeCtx); err != nil {
		return fmt.Errorf("connect Elasticsearch: %w", err)
	}
	redisTimeout, err := config.Duration("REDIS_TIMEOUT", 200*time.Millisecond)
	if err != nil {
		return err
	}
	redisCache := cache.NewRedis(config.String("REDIS_ADDR", "localhost:6379"), redisTimeout)
	defer redisCache.Close()
	writer := indexer.NewMultiTargetWriter(elasticsearch)
	handler := indexer.NewHandlerWithLogger(writer, redisCache, logger)
	consumer := indexer.NewConsumer(
		brokers,
		config.String("KAFKA_TOPIC", "catalog.public.products"),
		config.String("KAFKA_GROUP_ID", "catalog-indexer"),
		handler,
		logger,
	)
	return consumer.Run(ctx)
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
