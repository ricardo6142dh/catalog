package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/events"
	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/search"
)

type ProductWriter interface {
	IndexProduct(context.Context, search.ProductDocument) error
	DeleteProduct(context.Context, string, int64) error
}

type CacheInvalidator interface {
	Delete(context.Context, string) error
}

type Handler struct {
	writer      ProductWriter
	invalidator CacheInvalidator
	logger      *slog.Logger
}

func NewHandler(writer ProductWriter) *Handler {
	return &Handler{writer: writer, logger: slog.Default()}
}

func NewHandlerWithInvalidator(writer ProductWriter, invalidator CacheInvalidator) *Handler {
	return NewHandlerWithLogger(writer, invalidator, slog.Default())
}

func NewHandlerWithLogger(writer ProductWriter, invalidator CacheInvalidator, logger *slog.Logger) *Handler {
	return &Handler{writer: writer, invalidator: invalidator, logger: logger}
}

func (h *Handler) ApplyProductEvent(ctx context.Context, event events.ProductEvent) error {
	product := event.Product
	var err error
	if event.Operation == events.OperationDelete || !product.Active {
		err = h.writer.DeleteProduct(ctx, product.ID, product.Version)
	} else {
		err = h.writer.IndexProduct(ctx, search.ProductDocument{
			ProductID:   product.ID,
			Name:        product.Name,
			Description: product.Description,
			Brand:       product.Brand,
			Category:    product.Category,
			Price:       product.Price,
			Stock:       product.Stock,
			Version:     product.Version,
			UpdatedAt:   time.UnixMicro(product.UpdatedAt).UTC(),
		})
	}
	if errors.Is(err, search.ErrStaleVersion) {
		err = nil
	}
	if err != nil {
		return fmt.Errorf("apply product version %d: %w", product.Version, err)
	}
	if h.invalidator != nil {
		if err := h.invalidator.Delete(ctx, "product:"+product.ID); err != nil {
			h.logger.Warn("invalidate product cache", "error", err, "product_id", product.ID)
		}
	}
	return nil
}
