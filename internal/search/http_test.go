package search

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type httpBackend struct {
	result SearchResult
	params SearchParams
}

func (b *httpBackend) SearchProducts(_ context.Context, params SearchParams) (SearchResult, error) {
	b.params = params
	return b.result, nil
}

func (*httpBackend) SuggestProducts(context.Context, SuggestParams) (SuggestResult, error) {
	return SuggestResult{}, nil
}

func (*httpBackend) GetProduct(context.Context, string) (ProductDocument, error) {
	return ProductDocument{}, nil
}

func TestSearchReturnsBackendResults(t *testing.T) {
	backend := &httpBackend{result: SearchResult{
		Products: []ProductDocument{{ProductID: "product-1", Name: "iPhone 16", Price: 999}},
		Total:    1,
		Page:     2,
		Limit:    10,
	}}
	service := NewService(backend)
	handler := NewHTTPHandler(service, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Second)
	request := httptest.NewRequest(http.MethodGet, "/search?q=iphone&brand=apple&limit=10&page=2", nil)
	response := httptest.NewRecorder()

	handler.Routes(func(context.Context) error { return nil }).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, http.StatusOK, response.Body.String())
	}
	var result SearchResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.Total != 1 || len(result.Products) != 1 || result.Products[0].ProductID != "product-1" {
		t.Fatalf("result = %+v, want backend result", result)
	}
	if backend.params.Query != "iphone" || backend.params.Brand != "apple" || backend.params.Limit != 10 || backend.params.Page != 2 {
		t.Fatalf("backend params = %+v, want parsed request parameters", backend.params)
	}
}

func TestParseSearchParams(t *testing.T) {
	request := httptest.NewRequest("GET", "/search?q=iphone&brand=apple&category=phones&min_price=500&max_price=1500&sort=price_asc&limit=10&page=2", nil)
	params, err := ParseSearchParams(request)
	if err != nil {
		t.Fatalf("ParseSearchParams() error = %v", err)
	}
	if params.Query != "iphone" || params.Brand != "apple" || params.Category != "phones" {
		t.Fatalf("unexpected text filters: %+v", params)
	}
	if params.MinPrice == nil || *params.MinPrice != 500 || params.MaxPrice == nil || *params.MaxPrice != 1500 {
		t.Fatalf("unexpected price filters: %+v", params)
	}
	if params.Sort != "price_asc" || params.Limit != 10 || params.Page != 2 {
		t.Fatalf("unexpected pagination/sort: %+v", params)
	}
}

func TestParseSearchParamsRejectsInvalidRange(t *testing.T) {
	request := httptest.NewRequest("GET", "/search?min_price=20&max_price=10", nil)
	if _, err := ParseSearchParams(request); err == nil {
		t.Fatal("ParseSearchParams() error = nil, want invalid range")
	}
}

func TestParseSearchParamsNormalizesExactFilters(t *testing.T) {
	request := httptest.NewRequest("GET", "/search?brand=Apple&category=Phones", nil)
	params, err := ParseSearchParams(request)
	if err != nil {
		t.Fatalf("ParseSearchParams() error = %v", err)
	}
	if params.Brand != "apple" || params.Category != "phones" {
		t.Fatalf("filters = %q/%q, want apple/phones", params.Brand, params.Category)
	}
}
