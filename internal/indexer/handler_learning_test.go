package indexer

import (
	"context"
	"testing"

	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/events"
	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/search"
)

type recordingWriter struct {
	indexed []search.ProductDocument
}

func (w *recordingWriter) IndexProduct(_ context.Context, document search.ProductDocument) error {
	w.indexed = append(w.indexed, document)
	return nil
}

func (w *recordingWriter) DeleteProduct(context.Context, string, int64) error {
	return nil
}

func TestLearningStaleProductVersionIsRejected(t *testing.T) {
	t.Skip("TODO(ricardo): implement ApplyProductEvent version protection")

	writer := &recordingWriter{}
	handler := NewHandler(writer)
	// Fixture assumes Elasticsearch already contains version 43. Your writer/client
	// implementation should expose the conflict as an ignored/rejected stale event.
	event := events.ProductEvent{Operation: events.OperationUpdate, Product: events.ProductState{ID: "product-1", Version: 42}}
	if err := handler.ApplyProductEvent(context.Background(), event); err == nil {
		t.Fatal("ApplyProductEvent() error = nil, want stale version rejection")
	}
}

func TestLearningNewerProductVersionIsIndexed(t *testing.T) {
	t.Skip("TODO(ricardo): implement ApplyProductEvent newer version")

	writer := &recordingWriter{}
	handler := NewHandler(writer)
	event := events.ProductEvent{Operation: events.OperationUpdate, Product: events.ProductState{ID: "product-1", Version: 43}}
	if err := handler.ApplyProductEvent(context.Background(), event); err != nil {
		t.Fatalf("ApplyProductEvent() error = %v", err)
	}
	if len(writer.indexed) != 1 || writer.indexed[0].Version != 43 {
		t.Fatalf("indexed = %+v, want version 43", writer.indexed)
	}
}
