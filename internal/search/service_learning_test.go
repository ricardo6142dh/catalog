package search

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeCache struct {
	value    []byte
	hit      bool
	err      error
	setCalls int
}

func (f *fakeCache) Get(context.Context, string) ([]byte, bool, error) {
	return f.value, f.hit, f.err
}

func (f *fakeCache) Set(context.Context, string, []byte, time.Duration) error {
	f.setCalls++
	return f.err
}

type fakeBackend struct {
	result      SearchResult
	searchCalls int
	product     ProductDocument
	getCalls    int
}

func (f *fakeBackend) SearchProducts(context.Context, SearchParams) (SearchResult, error) {
	f.searchCalls++
	return f.result, nil
}

func (f *fakeBackend) SuggestProducts(context.Context, SuggestParams) (SuggestResult, error) {
	return SuggestResult{}, nil
}

func (f *fakeBackend) GetProduct(context.Context, string) (ProductDocument, error) {
	f.getCalls++
	return f.product, nil
}

func TestSearchCacheHitSkipsBackend(t *testing.T) {
	cacheStore := &fakeCache{hit: true, value: []byte(`{"products":[],"total":7,"page":1,"limit":20}`)}
	backend := &fakeBackend{}
	service := NewCachedService(cacheStore, backend, 2*time.Minute, func(SearchParams) string { return "search:key" })

	result, err := service.Search(context.Background(), SearchParams{})
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if backend.searchCalls != 0 || result.Total != 7 {
		t.Fatalf("backend calls = %d, result = %+v", backend.searchCalls, result)
	}
}

func TestSearchCacheMissStoresBackendResult(t *testing.T) {
	cacheStore := &fakeCache{}
	backend := &fakeBackend{result: SearchResult{Total: 1}}
	service := NewCachedService(cacheStore, backend, time.Minute, func(SearchParams) string { return "search:key" })

	if _, err := service.Search(context.Background(), SearchParams{}); err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if backend.searchCalls != 1 || cacheStore.setCalls != 1 {
		t.Fatalf("backend calls = %d, cache sets = %d", backend.searchCalls, cacheStore.setCalls)
	}
}

func TestCacheFailureFallsBackToBackend(t *testing.T) {
	cacheStore := &fakeCache{err: errors.New("redis down")}
	backend := &fakeBackend{result: SearchResult{Total: 2}}
	service := NewCachedService(cacheStore, backend, time.Minute, func(SearchParams) string { return "search:key" })

	result, err := service.Search(context.Background(), SearchParams{})
	if err != nil || result.Total != 2 || backend.searchCalls != 1 {
		t.Fatalf("result = %+v, error = %v, backend calls = %d", result, err, backend.searchCalls)
	}
}

func TestProductCacheHitSkipsBackend(t *testing.T) {
	cacheStore := &fakeCache{hit: true, value: []byte(`{"product_id":"product-1","name":"Phone"}`)}
	backend := &fakeBackend{}
	service := NewCachedService(cacheStore, backend, time.Minute, func(SearchParams) string { return "" })

	product, err := service.GetProduct(context.Background(), "product-1")
	if err != nil || product.Name != "Phone" || backend.getCalls != 0 {
		t.Fatalf("product = %+v, error = %v, backend calls = %d", product, err, backend.getCalls)
	}
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
