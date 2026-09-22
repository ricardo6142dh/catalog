package product

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Handler struct {
	repository Repository
	logger     *slog.Logger
	timeout    time.Duration
}

func NewHandler(repository Repository, logger *slog.Logger, timeout time.Duration) *Handler {
	return &Handler{repository: repository, logger: logger, timeout: timeout}
}

func (h *Handler) Routes(health func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /products", h.create)
	mux.HandleFunc("GET /products/{id}", h.get)
	mux.HandleFunc("PUT /products/{id}", h.update)
	mux.HandleFunc("DELETE /products/{id}", h.delete)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		defer cancel()
		if err := health(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "postgres unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return requestLogger(h.logger, mux)
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var input CreateProduct
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := input.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "name, brand, category, non-negative price and stock are required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	created, err := h.repository.Create(ctx, input)
	if err != nil {
		h.logger.Error("create product", "error", err)
		writeError(w, http.StatusInternalServerError, "could not create product")
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, ok := validID(w, r.PathValue("id"))
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	product, err := h.repository.Get(ctx, id)
	if err != nil {
		handleRepositoryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, product)
}

func (h *Handler) update(w http.ResponseWriter, r *http.Request) {
	id, ok := validID(w, r.PathValue("id"))
	if !ok {
		return
	}
	var input UpdateProduct
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := input.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "name, brand, category, non-negative price and stock are required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	updated, err := h.repository.Update(ctx, id, input)
	if err != nil {
		handleRepositoryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	id, ok := validID(w, r.PathValue("id"))
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	if err := h.repository.Delete(ctx, id); err != nil {
		handleRepositoryError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func validID(w http.ResponseWriter, id string) (string, bool) {
	id = strings.TrimSpace(id)
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, http.StatusBadRequest, "invalid product id")
		return "", false
	}
	return id, true
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.New("invalid JSON body")
	}
	return nil
}

func handleRepositoryError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, ErrNotFound.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, "product store error")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func requestLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		logger.Info("http request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(started))
	})
}
