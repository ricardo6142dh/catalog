package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/config"
	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/search"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("search API stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	elasticsearchTimeout, err := config.Duration("ELASTICSEARCH_TIMEOUT", 3*time.Second)
	if err != nil {
		return err
	}
	requestTimeout, err := config.Duration("REQUEST_TIMEOUT", 5*time.Second)
	if err != nil {
		return err
	}
	elasticsearch, err := search.NewElasticsearch(config.String("ELASTICSEARCH_URL", "http://localhost:9200"), elasticsearchTimeout)
	if err != nil {
		return err
	}
	startupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := elasticsearch.Health(startupCtx); err != nil {
		return fmt.Errorf("connect Elasticsearch: %w", err)
	}

	service := search.NewService(elasticsearch)
	handler := search.NewHTTPHandler(service, logger, requestTimeout)
	health := func(ctx context.Context) error {
		return elasticsearch.Health(ctx)
	}
	server := &http.Server{
		Addr:              ":" + config.String("SEARCH_API_PORT", "8082"),
		Handler:           handler.Routes(health),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("search API listening", "address", server.Addr)
		serverErrors <- server.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		return server.Shutdown(shutdownCtx)
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
