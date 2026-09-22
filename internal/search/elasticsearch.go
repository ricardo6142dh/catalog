package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const ProductsAlias = "products"

type ElasticsearchClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewElasticsearch(baseURL string, timeout time.Duration) (*ElasticsearchClient, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid Elasticsearch URL %q", baseURL)
	}
	return &ElasticsearchClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: timeout},
	}, nil
}

func (c *ElasticsearchClient) Health(ctx context.Context) error {
	var response struct {
		Status string `json:"status"`
	}
	status, err := c.doJSON(ctx, http.MethodGet, "/_cluster/health", nil, &response)
	if err != nil {
		return err
	}
	if status != http.StatusOK || response.Status == "red" {
		return fmt.Errorf("Elasticsearch unhealthy: HTTP %d status %q", status, response.Status)
	}
	return nil
}

func (c *ElasticsearchClient) SearchProducts(ctx context.Context, params SearchParams) (SearchResult, error) {
	must := make([]any, 0, 1)
	if params.Query != "" {
		must = append(must, map[string]any{
			"multi_match": map[string]any{
				"query":     params.Query,
				"fields":    []string{"name^2", "description"},
				"fuzziness": "AUTO",
			},
		})
	}
	filters := make([]any, 0, 3)
	if params.Brand != "" {
		filters = append(filters, map[string]any{"term": map[string]any{"brand.keyword": params.Brand}})
	}
	if params.Category != "" {
		filters = append(filters, map[string]any{"term": map[string]any{"category.keyword": params.Category}})
	}
	if params.MinPrice != nil || params.MaxPrice != nil {
		rangeValues := make(map[string]float64, 2)
		if params.MinPrice != nil {
			rangeValues["gte"] = *params.MinPrice
		}
		if params.MaxPrice != nil {
			rangeValues["lte"] = *params.MaxPrice
		}
		filters = append(filters, map[string]any{"range": map[string]any{"price": rangeValues}})
	}

	sort := []any{map[string]any{"_score": "desc"}, map[string]any{"_id": "asc"}}
	if params.Sort == "price_asc" {
		sort = []any{map[string]any{"price": "asc"}, map[string]any{"_id": "asc"}}
	}
	request := map[string]any{
		"from":             (params.Page - 1) * params.Limit,
		"size":             params.Limit,
		"track_total_hits": true,
		"query": map[string]any{
			"bool": map[string]any{"must": must, "filter": filters},
		},
		"sort": sort,
	}
	var response struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source ProductDocument `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	status, err := c.doJSON(ctx, http.MethodPost, "/"+ProductsAlias+"/_search", request, &response)
	if err != nil {
		return SearchResult{}, err
	}
	if status != http.StatusOK {
		return SearchResult{}, fmt.Errorf("search products: Elasticsearch HTTP %d", status)
	}
	products := make([]ProductDocument, 0, len(response.Hits.Hits))
	for _, hit := range response.Hits.Hits {
		products = append(products, hit.Source)
	}
	return SearchResult{
		Products: products,
		Total:    response.Hits.Total.Value,
		Page:     params.Page,
		Limit:    params.Limit,
	}, nil
}

func (c *ElasticsearchClient) SuggestProducts(ctx context.Context, params SuggestParams) (SuggestResult, error) {
	// TODO(ricardo): implement autocomplete query.
	return SuggestResult{}, ErrNotImplemented
}

func (c *ElasticsearchClient) GetProduct(ctx context.Context, productID string) (ProductDocument, error) {
	var response struct {
		Found  bool            `json:"found"`
		Source ProductDocument `json:"_source"`
	}
	path := "/" + ProductsAlias + "/_doc/" + url.PathEscape(productID)
	status, err := c.doJSON(ctx, http.MethodGet, path, nil, &response)
	if err != nil {
		return ProductDocument{}, err
	}
	if status == http.StatusNotFound || !response.Found {
		return ProductDocument{}, ErrNotFound
	}
	if status != http.StatusOK {
		return ProductDocument{}, fmt.Errorf("get product: Elasticsearch HTTP %d", status)
	}
	return response.Source, nil
}

func (c *ElasticsearchClient) IndexProduct(ctx context.Context, product ProductDocument) error {
	// TODO(ricardo): use product_id as _id and enforce product version semantics.
	return ErrNotImplemented
}

func (c *ElasticsearchClient) DeleteProduct(ctx context.Context, productID string, version int64) error {
	// TODO(ricardo): delete by product_id while preserving version semantics.
	return ErrNotImplemented
}

func (c *ElasticsearchClient) CreateProductsIndex(ctx context.Context, indexName string, definition any) error {
	// TODO(ricardo): create a versioned products index with mappings/settings.
	return ErrNotImplemented
}

func (c *ElasticsearchClient) SwitchProductsAlias(ctx context.Context, newIndex string) error {
	// TODO(ricardo): atomically point ProductsAlias at newIndex.
	return ErrNotImplemented
}

func (c *ElasticsearchClient) doJSON(ctx context.Context, method, path string, requestBody, responseBody any) (int, error) {
	var body io.Reader
	if requestBody != nil {
		payload, err := json.Marshal(requestBody)
		if err != nil {
			return 0, fmt.Errorf("marshal Elasticsearch request: %w", err)
		}
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return 0, fmt.Errorf("create Elasticsearch request: %w", err)
	}
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("Elasticsearch request: %w", err)
	}
	defer response.Body.Close()

	if responseBody != nil && response.ContentLength != 0 {
		if err := json.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(responseBody); err != nil && !errors.Is(err, io.EOF) {
			return response.StatusCode, fmt.Errorf("decode Elasticsearch response: %w", err)
		}
	}
	return response.StatusCode, nil
}
