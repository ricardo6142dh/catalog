package indexer

import (
	"context"
	"errors"

	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/events"
	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/search"
)

var ErrNotImplemented = errors.New("not implemented")

type ProductWriter interface {
	IndexProduct(context.Context, search.ProductDocument) error
	DeleteProduct(context.Context, string, int64) error
}

type Handler struct {
	writer ProductWriter
}

func NewHandler(writer ProductWriter) *Handler {
	return &Handler{writer: writer}
}

func (h *Handler) ApplyProductEvent(ctx context.Context, event events.ProductEvent) error {
	// TODO(ricardo): implement index/delete, product_id document ID, and stale-version protection.
	return ErrNotImplemented
}
