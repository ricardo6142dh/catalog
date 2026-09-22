package search

import (
	"context"
	"testing"
)

type fakeBackend struct {
	result      SearchResult
	searchCalls int
}

func (f *fakeBackend) SearchProducts(context.Context, SearchParams) (SearchResult, error) {
	f.searchCalls++
	return f.result, nil
}

func (f *fakeBackend) SuggestProducts(context.Context, SuggestParams) (SuggestResult, error) {
	return SuggestResult{}, nil
}

func (f *fakeBackend) GetProduct(context.Context, string) (ProductDocument, error) {
	return ProductDocument{}, nil
}

func TestSearchQueriesBackend(t *testing.T) {
	backend := &fakeBackend{result: SearchResult{Page: 1, Limit: 20}}
	service := NewService(backend)
	result, err := service.Search(context.Background(), SearchParams{Query: "iphone", Page: 1, Limit: 20, Sort: "relevance"})
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if backend.searchCalls != 1 {
		t.Fatalf("backend calls = %d, want 1", backend.searchCalls)
	}
	if result.Page != 1 || result.Limit != 20 {
		t.Fatalf("result = %+v, want backend result", result)
	}
}
