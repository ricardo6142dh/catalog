package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/config"
	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/database"
	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/product"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("product API stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	databaseURL := config.String("DATABASE_URL", "postgres://catalog:catalog@localhost:5432/catalog?sslmode=disable")
	connectTimeout, err := config.Duration("POSTGRES_CONNECT_TIMEOUT", 5*time.Second)
	if err != nil {
		return err
	}
	requestTimeout, err := config.Duration("REQUEST_TIMEOUT", 3*time.Second)
	if err != nil {
		return err
	}

	pool, err := database.Open(ctx, databaseURL, connectTimeout)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := database.Migrate(ctx, pool); err != nil {
		return err
	}

	repository := product.NewPostgresRepository(pool)
	handler := product.NewHandler(repository, logger, requestTimeout)
	server := &http.Server{
		Addr:              ":" + config.String("PRODUCT_API_PORT", "8081"),
		Handler:           handler.Routes(pool.Ping),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("product API listening", "address", server.Addr)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
