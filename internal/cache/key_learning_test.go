package cache_test

import (
	"testing"

	"github.com/ricardo6142dh/catalog/internal/cache"
	"github.com/ricardo6142dh/catalog/internal/search"
)

func TestLearningSearchCacheKeyCanonicalization(t *testing.T) {
	first := cache.BuildSearchCacheKey(search.SearchParams{Query: "IPhone", Brand: "Apple", Page: 1, Limit: 20, Sort: "relevance"})
	second := cache.BuildSearchCacheKey(search.SearchParams{Brand: "apple", Page: 1, Query: "iphone", Limit: 20, Sort: "relevance"})
	if first != second {
		t.Fatalf("equivalent searches produced different keys: %q != %q", first, second)
	}
}

func TestLearningSearchCacheKeyIncludesPage(t *testing.T) {
	first := cache.BuildSearchCacheKey(search.SearchParams{Query: "iphone", Page: 1, Limit: 20, Sort: "relevance"})
	second := cache.BuildSearchCacheKey(search.SearchParams{Query: "iphone", Page: 2, Limit: 20, Sort: "relevance"})
	if first == second {
		t.Fatalf("different pages produced same key: %q", first)
	}
}
