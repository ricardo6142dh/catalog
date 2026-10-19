package search

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
)

type Cache interface {
	Get(context.Context, string) ([]byte, bool, error)
	Set(context.Context, string, []byte, time.Duration) error
}

type CacheKeyBuilder func(SearchParams) string

type Backend interface {
	SearchProducts(context.Context, SearchParams) (SearchResult, error)
	SuggestProducts(context.Context, SuggestParams) (SuggestResult, error)
	GetProduct(context.Context, string) (ProductDocument, error)
}

type SearchService struct {
	cache     Cache
	backend   Backend
	ttl       time.Duration
	searchKey CacheKeyBuilder
	logger    *slog.Logger
}

func NewService(backend Backend) *SearchService {
	return &SearchService{backend: backend}
}

func NewCachedService(cacheStore Cache, backend Backend, ttl time.Duration, searchKey CacheKeyBuilder) *SearchService {
	return NewCachedServiceWithLogger(cacheStore, backend, ttl, searchKey, slog.Default())
}

func NewCachedServiceWithLogger(cacheStore Cache, backend Backend, ttl time.Duration, searchKey CacheKeyBuilder, logger *slog.Logger) *SearchService {
	return &SearchService{cache: cacheStore, backend: backend, ttl: ttl, searchKey: searchKey, logger: logger}
}

func (s *SearchService) Search(ctx context.Context, params SearchParams) (SearchResult, error) {
	if s.cache == nil || s.searchKey == nil {
		return s.backend.SearchProducts(ctx, params)
	}
	key := s.searchKey(params)
	if payload, hit, err := s.cache.Get(ctx, key); err == nil && hit {
		var result SearchResult
		if err := json.Unmarshal(payload, &result); err == nil {
			return result, nil
		} else {
			s.logger.Warn("decode cached search result", "error", err, "key", key)
		}
	} else if err != nil {
		s.logger.Warn("read search cache", "error", err, "key", key)
	}
	result, err := s.backend.SearchProducts(ctx, params)
	if err != nil {
		return SearchResult{}, err
	}
	if payload, err := json.Marshal(result); err == nil {
		if err := s.cache.Set(ctx, key, payload, s.ttl); err != nil {
			s.logger.Warn("write search cache", "error", err, "key", key)
		}
	}
	return result, nil
}

func (s *SearchService) Suggest(ctx context.Context, params SuggestParams) (SuggestResult, error) {
	return s.backend.SuggestProducts(ctx, params)
}

func (s *SearchService) GetProduct(ctx context.Context, id string) (ProductDocument, error) {
	if s.cache == nil {
		return s.backend.GetProduct(ctx, id)
	}
	key := "product:" + id
	if payload, hit, err := s.cache.Get(ctx, key); err == nil && hit {
		var product ProductDocument
		if err := json.Unmarshal(payload, &product); err == nil {
			return product, nil
		} else {
			s.logger.Warn("decode cached product", "error", err, "key", key)
		}
	} else if err != nil {
		s.logger.Warn("read product cache", "error", err, "key", key)
	}
	product, err := s.backend.GetProduct(ctx, id)
	if err != nil {
		return ProductDocument{}, err
	}
	if payload, err := json.Marshal(product); err == nil {
		if err := s.cache.Set(ctx, key, payload, s.ttl); err != nil {
			s.logger.Warn("write product cache", "error", err, "key", key)
		}
	}
	return product, nil
}
