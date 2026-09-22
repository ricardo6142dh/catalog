package search

import (
	"context"
)

type Backend interface {
	SearchProducts(context.Context, SearchParams) (SearchResult, error)
	SuggestProducts(context.Context, SuggestParams) (SuggestResult, error)
	GetProduct(context.Context, string) (ProductDocument, error)
}

type SearchService struct {
	backend Backend
}

func NewService(backend Backend) *SearchService {
	return &SearchService{backend: backend}
}

func (s *SearchService) Search(ctx context.Context, params SearchParams) (SearchResult, error) {
	return s.backend.SearchProducts(ctx, params)
}

func (s *SearchService) Suggest(ctx context.Context, params SuggestParams) (SuggestResult, error) {
	return s.backend.SuggestProducts(ctx, params)
}

func (s *SearchService) GetProduct(ctx context.Context, id string) (ProductDocument, error) {
	return s.backend.GetProduct(ctx, id)
}
