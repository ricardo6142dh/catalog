package search

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSearchProductsBuildsQueryAndDecodesHits(t *testing.T) {
	var requestPath string
	var requestBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"hits": {
				"total": {"value": 1},
				"hits": [{"_source": {"product_id":"product-1","name":"iPhone 16","brand":"Apple","category":"phones","price":999,"stock":4,"version":3,"updated_at":"2026-09-22T10:00:00Z"}}]
			}
		}`)
	}))
	defer server.Close()

	client, err := NewElasticsearch(server.URL, time.Second)
	if err != nil {
		t.Fatalf("NewElasticsearch() error = %v", err)
	}
	minPrice, maxPrice := 500.0, 1500.0
	result, err := client.SearchProducts(context.Background(), SearchParams{
		Query: "iphone", Brand: "Apple", Category: "phones",
		MinPrice: &minPrice, MaxPrice: &maxPrice,
		Sort: "price_asc", Limit: 10, Page: 2,
	})
	if err != nil {
		t.Fatalf("SearchProducts() error = %v", err)
	}
	if requestPath != "/products/_search" {
		t.Fatalf("path = %q, want /products/_search", requestPath)
	}
	if requestBody["from"] != float64(10) || requestBody["size"] != float64(10) {
		t.Fatalf("pagination body = %+v, want from=10 size=10", requestBody)
	}
	bodyJSON, _ := json.Marshal(requestBody)
	body := string(bodyJSON)
	for _, want := range []string{`"multi_match"`, `"fuzziness":"AUTO"`, `"brand.keyword":"Apple"`, `"category.keyword":"phones"`, `"price":{"gte":500,"lte":1500}`, `"price":"asc"`, `"_id":"asc"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("request body = %s, want %s", body, want)
		}
	}
	if result.Total != 1 || result.Page != 2 || result.Limit != 10 || len(result.Products) != 1 || result.Products[0].ProductID != "product-1" {
		t.Fatalf("result = %+v, want decoded Elasticsearch hit", result)
	}
}

func TestLearningIndexProductUsesProductIDAsDocumentID(t *testing.T) {
	t.Skip("TODO(ricardo): implement ElasticsearchClient.IndexProduct")

	var path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()
	client, _ := NewElasticsearch(server.URL, time.Second)
	productID := "8b31c36e-bb75-48af-8241-57b665d7c75a"
	if err := client.IndexProduct(context.Background(), ProductDocument{ProductID: productID, Version: 42}); err != nil {
		t.Fatalf("IndexProduct() error = %v", err)
	}
	if !strings.HasSuffix(path, "/_doc/"+productID) {
		t.Fatalf("request path = %q, want document ID %q", path, productID)
	}
}

func TestLearningSwitchProductsAliasTargetsV2(t *testing.T) {
	t.Skip("TODO(ricardo): implement ElasticsearchClient.SwitchProductsAlias")

	var body string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := io.ReadAll(r.Body)
		body = string(payload)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, _ := NewElasticsearch(server.URL, time.Second)
	if err := client.SwitchProductsAlias(context.Background(), "products_v2"); err != nil {
		t.Fatalf("SwitchProductsAlias() error = %v", err)
	}
	if !strings.Contains(body, `"alias":"products"`) || !strings.Contains(body, `"index":"products_v2"`) {
		t.Fatalf("alias request = %s, want products -> products_v2", body)
	}
}
