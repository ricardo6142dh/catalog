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
	"slices"
	"strconv"
	"strings"
	"time"
)

const ProductsAlias = "products"
const ProductsCDCTargetsAlias = "products_cdc_targets"

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
			"bool": map[string]any{
				"must":     must,
				"filter":   filters,
				"must_not": []any{map[string]any{"term": map[string]any{"deleted": true}}},
			},
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
	request := map[string]any{
		"size": params.Limit,
		"query": map[string]any{
			"bool": map[string]any{
				"must": map[string]any{
					"match_bool_prefix": map[string]any{
						"name": map[string]any{"query": params.Query, "fuzziness": "AUTO"},
					},
				},
				"must_not": []any{map[string]any{"term": map[string]any{"deleted": true}}},
			},
		},
		"sort": []any{map[string]any{"_score": "desc"}, map[string]any{"_id": "asc"}},
	}
	var response struct {
		Hits struct {
			Hits []struct {
				Source ProductDocument `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	status, err := c.doJSON(ctx, http.MethodPost, "/"+ProductsAlias+"/_search", request, &response)
	if err != nil {
		return SuggestResult{}, err
	}
	if status != http.StatusOK {
		return SuggestResult{}, fmt.Errorf("suggest products: Elasticsearch HTTP %d", status)
	}
	result := SuggestResult{
		Suggestions: make([]string, 0, len(response.Hits.Hits)),
		Products:    make([]ProductDocument, 0, len(response.Hits.Hits)),
	}
	for _, hit := range response.Hits.Hits {
		result.Suggestions = append(result.Suggestions, hit.Source.Name)
		result.Products = append(result.Products, hit.Source)
	}
	return result, nil
}

func (c *ElasticsearchClient) GetProduct(ctx context.Context, productID string) (ProductDocument, error) {
	return c.GetProductFrom(ctx, ProductsAlias, productID)
}

func (c *ElasticsearchClient) GetProductFrom(ctx context.Context, indexName, productID string) (ProductDocument, error) {
	if !validIndexName(indexName) {
		return ProductDocument{}, fmt.Errorf("invalid Elasticsearch index name %q", indexName)
	}
	var response struct {
		Found  bool            `json:"found"`
		Source ProductDocument `json:"_source"`
	}
	path := "/" + indexName + "/_doc/" + url.PathEscape(productID)
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
	if response.Source.Deleted {
		return ProductDocument{}, ErrNotFound
	}
	return response.Source, nil
}

func (c *ElasticsearchClient) IndexProduct(ctx context.Context, product ProductDocument) error {
	return c.IndexProductTo(ctx, ProductsAlias, product)
}

func (c *ElasticsearchClient) IndexProductTo(ctx context.Context, indexName string, product ProductDocument) error {
	if !validIndexName(indexName) {
		return fmt.Errorf("invalid Elasticsearch index name %q", indexName)
	}
	if product.ProductID == "" {
		return fmt.Errorf("product_id is required")
	}
	if product.Version < 1 {
		return fmt.Errorf("product version must be positive")
	}
	query := url.Values{
		"version":      []string{strconv.FormatInt(product.Version, 10)},
		"version_type": []string{"external_gte"},
	}

	status, err := c.doJSON(
		ctx,
		http.MethodPut,
		"/"+indexName+"/_doc/"+url.PathEscape(product.ProductID)+"?"+query.Encode(),
		product,
		nil,
	)
	if err != nil {
		return err
	}
	if status == http.StatusConflict {
		return ErrStaleVersion
	}

	if status != http.StatusOK && status != http.StatusCreated {
		return fmt.Errorf("index product: Elasticsearch HTTP %d", status)
	}

	return nil
}

func (c *ElasticsearchClient) DeleteProduct(ctx context.Context, productID string, version int64) error {
	return c.DeleteProductFrom(ctx, ProductsAlias, productID, version)
}

func (c *ElasticsearchClient) DeleteProductFrom(ctx context.Context, indexName, productID string, version int64) error {
	if !validIndexName(indexName) {
		return fmt.Errorf("invalid Elasticsearch index name %q", indexName)
	}
	if productID == "" {
		return fmt.Errorf("product_id is required")
	}
	if version < 1 {
		return fmt.Errorf("product version must be positive")
	}
	query := url.Values{
		"version":      []string{strconv.FormatInt(version, 10)},
		"version_type": []string{"external_gte"},
	}
	tombstone := ProductDocument{ProductID: productID, Version: version, Deleted: true, UpdatedAt: time.Now().UTC()}
	status, err := c.doJSON(ctx, http.MethodPut, "/"+indexName+"/_doc/"+url.PathEscape(productID)+"?"+query.Encode(), tombstone, nil)
	if err != nil {
		return err
	}
	if status == http.StatusConflict {
		return ErrStaleVersion
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return fmt.Errorf("delete product: Elasticsearch HTTP %d", status)
	}
	return nil
}

func (c *ElasticsearchClient) AliasIndices(ctx context.Context, alias string) ([]string, error) {
	if !validIndexName(alias) {
		return nil, fmt.Errorf("invalid Elasticsearch alias name %q", alias)
	}
	response := make(map[string]json.RawMessage)
	status, err := c.doJSON(ctx, http.MethodGet, "/_alias/"+alias, nil, &response)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, nil
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("get alias indices: Elasticsearch HTTP %d", status)
	}
	indices := make([]string, 0, len(response))
	for index := range response {
		indices = append(indices, index)
	}
	slices.Sort(indices)
	return indices, nil
}

func (c *ElasticsearchClient) AddAliasTarget(ctx context.Context, alias, index string) error {
	return c.updateAlias(ctx, map[string]any{"add": map[string]any{"index": index, "alias": alias}})
}

func (c *ElasticsearchClient) RemoveAliasTarget(ctx context.Context, alias, index string) error {
	return c.updateAlias(ctx, map[string]any{"remove": map[string]any{"index": index, "alias": alias}})
}

func (c *ElasticsearchClient) RefreshIndex(ctx context.Context, index string) error {
	if !validIndexName(index) {
		return fmt.Errorf("invalid Elasticsearch index name %q", index)
	}
	status, err := c.doJSON(ctx, http.MethodPost, "/"+index+"/_refresh", nil, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("refresh index: Elasticsearch HTTP %d", status)
	}
	return nil
}

func (c *ElasticsearchClient) CountIndex(ctx context.Context, index string) (int64, error) {
	if !validIndexName(index) {
		return 0, fmt.Errorf("invalid Elasticsearch index name %q", index)
	}
	var response struct {
		Count int64 `json:"count"`
	}
	request := map[string]any{"query": map[string]any{"bool": map[string]any{
		"must_not": []any{map[string]any{"term": map[string]any{"deleted": true}}},
	}}}
	status, err := c.doJSON(ctx, http.MethodPost, "/"+index+"/_count", request, &response)
	if err != nil {
		return 0, err
	}
	if status != http.StatusOK {
		return 0, fmt.Errorf("count index: Elasticsearch HTTP %d", status)
	}
	return response.Count, nil
}

func (c *ElasticsearchClient) updateAlias(ctx context.Context, action map[string]any) error {
	status, err := c.doJSON(ctx, http.MethodPost, "/_aliases", map[string]any{"actions": []any{action}}, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("update alias: Elasticsearch HTTP %d", status)
	}
	return nil
}

func (c *ElasticsearchClient) CreateProductsIndex(ctx context.Context, indexName string, definition any) error {
	if !validIndexName(indexName) {
		return fmt.Errorf("invalid Elasticsearch index name %q", indexName)
	}
	status, err := c.doJSON(ctx, http.MethodPut, "/"+indexName, definition, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("create products index: Elasticsearch HTTP %d", status)
	}
	return nil
}

func (c *ElasticsearchClient) SwitchProductsAlias(ctx context.Context, newIndex string) error {
	if !validIndexName(newIndex) {
		return fmt.Errorf("invalid Elasticsearch index name %q", newIndex)
	}
	request := map[string]any{"actions": []any{
		map[string]any{"remove": map[string]any{"index": "*", "alias": ProductsAlias}},
		map[string]any{"add": map[string]any{"index": newIndex, "alias": ProductsAlias, "is_write_index": true}},
	}}
	status, err := c.doJSON(ctx, http.MethodPost, "/_aliases", request, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("switch products alias: Elasticsearch HTTP %d", status)
	}
	return nil
}

func validIndexName(name string) bool {
	if name == "" || name == "." || name == ".." || strings.ToLower(name) != name {
		return false
	}
	for _, char := range name {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '_' && char != '-' {
			return false
		}
	}
	return true
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
