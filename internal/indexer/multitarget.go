package indexer

import (
	"context"
	"errors"
	"fmt"

	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/search"
)

type TargetIndex interface {
	AliasIndices(context.Context, string) ([]string, error)
	IndexProductTo(context.Context, string, search.ProductDocument) error
	DeleteProductFrom(context.Context, string, string, int64) error
}

type MultiTargetWriter struct {
	index TargetIndex
}

func NewMultiTargetWriter(index TargetIndex) *MultiTargetWriter {
	return &MultiTargetWriter{index: index}
}

func (w *MultiTargetWriter) IndexProduct(ctx context.Context, product search.ProductDocument) error {
	targets, err := w.targets(ctx)
	if err != nil {
		return err
	}
	for _, target := range targets {
		if err := w.index.IndexProductTo(ctx, target, product); err != nil {
			if errors.Is(err, search.ErrStaleVersion) {
				continue
			}
			return fmt.Errorf("index product in %s: %w", target, err)
		}
	}
	return nil
}

func (w *MultiTargetWriter) DeleteProduct(ctx context.Context, productID string, version int64) error {
	targets, err := w.targets(ctx)
	if err != nil {
		return err
	}
	for _, target := range targets {
		if err := w.index.DeleteProductFrom(ctx, target, productID, version); err != nil {
			if errors.Is(err, search.ErrStaleVersion) {
				continue
			}
			return fmt.Errorf("delete product from %s: %w", target, err)
		}
	}
	return nil
}

func (w *MultiTargetWriter) targets(ctx context.Context) ([]string, error) {
	targets, err := w.index.AliasIndices(ctx, search.ProductsCDCTargetsAlias)
	if err != nil {
		return nil, fmt.Errorf("resolve CDC targets: %w", err)
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("CDC target alias %q has no indices", search.ProductsCDCTargetsAlias)
	}
	return targets, nil
}
