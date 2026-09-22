package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type HTTPHandler struct {
	service *SearchService
	logger  *slog.Logger
	timeout time.Duration
}

func NewHTTPHandler(service *SearchService, logger *slog.Logger, timeout time.Duration) *HTTPHandler {
	return &HTTPHandler{service: service, logger: logger, timeout: timeout}
}

func (h *HTTPHandler) Routes(health func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /search", h.search)
	mux.HandleFunc("GET /suggest", h.suggest)
	mux.HandleFunc("GET /products/{id}", h.getProduct)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		defer cancel()
		if err := health(ctx); err != nil {
			writeSearchError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		writeSearchJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return searchRequestLogger(h.logger, mux)
}

func ParseSearchParams(r *http.Request) (SearchParams, error) {
	query := r.URL.Query()
	params := SearchParams{
		Query:    strings.TrimSpace(query.Get("q")),
		Brand:    strings.TrimSpace(query.Get("brand")),
		Category: strings.TrimSpace(query.Get("category")),
		Sort:     strings.TrimSpace(query.Get("sort")),
		Limit:    20,
		Page:     1,
	}
	if params.Sort == "" {
		params.Sort = "relevance"
	}
	if params.Sort != "relevance" && params.Sort != "price_asc" {
		return SearchParams{}, errors.New("sort must be relevance or price_asc")
	}

	var err error
	if params.Limit, err = parsePositiveInt(query.Get("limit"), 20, 100); err != nil {
		return SearchParams{}, fmt.Errorf("limit: %w", err)
	}
	if params.Page, err = parsePositiveInt(query.Get("page"), 1, 10000); err != nil {
		return SearchParams{}, fmt.Errorf("page: %w", err)
	}
	if params.MinPrice, err = parseOptionalPrice(query.Get("min_price")); err != nil {
		return SearchParams{}, fmt.Errorf("min_price: %w", err)
	}
	if params.MaxPrice, err = parseOptionalPrice(query.Get("max_price")); err != nil {
		return SearchParams{}, fmt.Errorf("max_price: %w", err)
	}
	if params.MinPrice != nil && params.MaxPrice != nil && *params.MinPrice > *params.MaxPrice {
		return SearchParams{}, errors.New("min_price must be less than or equal to max_price")
	}
	return params, nil
}

func ParseSuggestParams(r *http.Request) (SuggestParams, error) {
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		return SuggestParams{}, errors.New("q is required")
	}
	limit, err := parsePositiveInt(r.URL.Query().Get("limit"), 5, 20)
	if err != nil {
		return SuggestParams{}, fmt.Errorf("limit: %w", err)
	}
	return SuggestParams{Query: query, Limit: limit}, nil
}

func parsePositiveInt(raw string, fallback, maximum int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > maximum {
		return 0, fmt.Errorf("must be between 1 and %d", maximum)
	}
	return value, nil
}

func parseOptionalPrice(raw string) (*float64, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || value < 0 {
		return nil, errors.New("must be a non-negative number")
	}
	return &value, nil
}

func (h *HTTPHandler) search(w http.ResponseWriter, r *http.Request) {
	params, err := ParseSearchParams(r)
	if err != nil {
		writeSearchError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	result, err := h.service.Search(ctx, params)
	if errors.Is(err, ErrNotImplemented) {
		writeSearchError(w, http.StatusNotImplemented, "search learning TODO not implemented")
		return
	}
	if err != nil {
		h.logger.Error("search products", "error", err)
		writeSearchError(w, http.StatusServiceUnavailable, "search backend unavailable")
		return
	}
	writeSearchJSON(w, http.StatusOK, result)
}

func (h *HTTPHandler) suggest(w http.ResponseWriter, r *http.Request) {
	params, err := ParseSuggestParams(r)
	if err != nil {
		writeSearchError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	result, err := h.service.Suggest(ctx, params)
	if errors.Is(err, ErrNotImplemented) {
		writeSearchError(w, http.StatusNotImplemented, "suggest learning TODO not implemented")
		return
	}
	if err != nil {
		writeSearchError(w, http.StatusServiceUnavailable, "search backend unavailable")
		return
	}
	writeSearchJSON(w, http.StatusOK, result)
}

func (h *HTTPHandler) getProduct(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeSearchError(w, http.StatusBadRequest, "invalid product id")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	document, err := h.service.GetProduct(ctx, id)
	if errors.Is(err, ErrNotFound) {
		writeSearchError(w, http.StatusNotFound, ErrNotFound.Error())
		return
	}
	if err != nil {
		writeSearchError(w, http.StatusServiceUnavailable, "search backend unavailable")
		return
	}
	writeSearchJSON(w, http.StatusOK, document)
}

func writeSearchJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeSearchError(w http.ResponseWriter, status int, message string) {
	writeSearchJSON(w, status, map[string]string{"error": message})
}

func searchRequestLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		logger.Info("http request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(started))
	})
}
