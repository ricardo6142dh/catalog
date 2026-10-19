package search

import (
	"errors"
	"time"
)

var ErrNotImplemented = errors.New("not implemented")
var ErrNotFound = errors.New("search document not found")
var ErrStaleVersion = errors.New("stale product version")

type SearchParams struct {
	Query    string
	Brand    string
	Category string
	MinPrice *float64
	MaxPrice *float64
	Sort     string
	Limit    int
	Page     int
}

type SuggestParams struct {
	Query string
	Limit int
}

type ProductDocument struct {
	ProductID   string    `json:"product_id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Brand       string    `json:"brand"`
	Category    string    `json:"category"`
	Price       float64   `json:"price"`
	Stock       int       `json:"stock"`
	Version     int64     `json:"version"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type SearchResult struct {
	Products []ProductDocument `json:"products"`
	Total    int64             `json:"total"`
	Page     int               `json:"page"`
	Limit    int               `json:"limit"`
}

type SuggestResult struct {
	Suggestions []string          `json:"suggestions"`
	Products    []ProductDocument `json:"products"`
}

type AliasAction struct {
	Alias string `json:"alias"`
	Index string `json:"index"`
}
