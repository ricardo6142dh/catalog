package indexer

import (
	"context"
	"testing"

	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/search"
)

type fakeTargetIndex struct {
	targets []string
	writes  []string
	staleOn string
}

func (f *fakeTargetIndex) AliasIndices(context.Context, string) ([]string, error) {
	return f.targets, nil
}

func (f *fakeTargetIndex) IndexProductTo(_ context.Context, target string, _ search.ProductDocument) error {
	f.writes = append(f.writes, target)
	if target == f.staleOn {
		return search.ErrStaleVersion
	}
	return nil
}

func TestMultiTargetWriterContinuesAfterStaleTarget(t *testing.T) {
	index := &fakeTargetIndex{targets: []string{"products_v1", "products_v2"}, staleOn: "products_v1"}
	writer := NewMultiTargetWriter(index)
	if err := writer.IndexProduct(context.Background(), search.ProductDocument{ProductID: "p1", Version: 3}); err != nil {
		t.Fatalf("IndexProduct() error = %v", err)
	}
	if len(index.writes) != 2 || index.writes[1] != "products_v2" {
		t.Fatalf("writes = %v, want both targets", index.writes)
	}
}

func (f *fakeTargetIndex) DeleteProductFrom(_ context.Context, target, _ string, _ int64) error {
	f.writes = append(f.writes, target)
	return nil
}

func TestMultiTargetWriterIndexesEveryCDCTarget(t *testing.T) {
	index := &fakeTargetIndex{targets: []string{"products_v1", "products_v2"}}
	writer := NewMultiTargetWriter(index)
	if err := writer.IndexProduct(context.Background(), search.ProductDocument{ProductID: "p1", Version: 3}); err != nil {
		t.Fatalf("IndexProduct() error = %v", err)
	}
	if len(index.writes) != 2 || index.writes[0] != "products_v1" || index.writes[1] != "products_v2" {
		t.Fatalf("writes = %v", index.writes)
	}
}
