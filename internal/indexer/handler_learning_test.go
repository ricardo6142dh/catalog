package indexer

import (
	"context"
	"errors"
	"testing"

	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/events"
	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/search"
)

type recordingWriter struct {
	indexed []search.ProductDocument
	deleted []string
	err     error
}

type recordingInvalidator struct {
	keys []string
}

func (i *recordingInvalidator) Delete(_ context.Context, key string) error {
	i.keys = append(i.keys, key)
	return nil
}

func (w *recordingWriter) IndexProduct(_ context.Context, document search.ProductDocument) error {
	w.indexed = append(w.indexed, document)
	return w.err
}

func (w *recordingWriter) DeleteProduct(_ context.Context, id string, _ int64) error {
	w.deleted = append(w.deleted, id)
	return w.err
}

func TestStaleProductVersionIsIgnored(t *testing.T) {
	writer := &recordingWriter{err: search.ErrStaleVersion}
	handler := NewHandler(writer)
	event := events.ProductEvent{Operation: events.OperationUpdate, Product: events.ProductState{ID: "product-1", Version: 42}}
	if err := handler.ApplyProductEvent(context.Background(), event); err != nil {
		t.Fatalf("ApplyProductEvent() error = %v, want stale version ignored", err)
	}
}

func TestNewerProductVersionIsIndexed(t *testing.T) {
	writer := &recordingWriter{}
	handler := NewHandler(writer)
	event := events.ProductEvent{Operation: events.OperationUpdate, Product: events.ProductState{ID: "product-1", Version: 43, Active: true}}
	if err := handler.ApplyProductEvent(context.Background(), event); err != nil {
		t.Fatalf("ApplyProductEvent() error = %v", err)
	}
	if len(writer.indexed) != 1 || writer.indexed[0].Version != 43 {
		t.Fatalf("indexed = %+v, want version 43", writer.indexed)
	}
}

func TestInactiveProductIsDeleted(t *testing.T) {
	writer := &recordingWriter{}
	handler := NewHandler(writer)
	event := events.ProductEvent{Operation: events.OperationUpdate, Product: events.ProductState{ID: "product-1", Version: 44, Active: false}}
	if err := handler.ApplyProductEvent(context.Background(), event); err != nil {
		t.Fatalf("ApplyProductEvent() error = %v", err)
	}
	if len(writer.deleted) != 1 || writer.deleted[0] != "product-1" {
		t.Fatalf("deleted = %v, want product-1", writer.deleted)
	}
}

func TestWriterFailureIsReturned(t *testing.T) {
	want := errors.New("backend unavailable")
	writer := &recordingWriter{err: want}
	handler := NewHandler(writer)
	event := events.ProductEvent{Operation: events.OperationCreate, Product: events.ProductState{ID: "product-1", Version: 1, Active: true}}
	if err := handler.ApplyProductEvent(context.Background(), event); !errors.Is(err, want) {
		t.Fatalf("ApplyProductEvent() error = %v, want %v", err, want)
	}
}

func TestAppliedEventInvalidatesProductCache(t *testing.T) {
	writer := &recordingWriter{}
	invalidator := &recordingInvalidator{}
	handler := NewHandlerWithInvalidator(writer, invalidator)
	event := events.ProductEvent{Operation: events.OperationUpdate, Product: events.ProductState{ID: "product-1", Version: 2, Active: true}}

	if err := handler.ApplyProductEvent(context.Background(), event); err != nil {
		t.Fatalf("ApplyProductEvent() error = %v", err)
	}
	if len(invalidator.keys) != 1 || invalidator.keys[0] != "product:product-1" {
		t.Fatalf("invalidated keys = %v", invalidator.keys)
	}
}
