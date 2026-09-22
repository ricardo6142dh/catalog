package product

import (
	"errors"
	"strings"
	"time"
)

var (
	ErrNotFound    = errors.New("product not found")
	ErrInvalid     = errors.New("invalid product")
	ErrUnavailable = errors.New("product store unavailable")
)

type Product struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Brand       string     `json:"brand"`
	Category    string     `json:"category"`
	Price       float64    `json:"price"`
	Stock       int        `json:"stock"`
	Version     int64      `json:"version"`
	Active      bool       `json:"active"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	DeletedAt   *time.Time `json:"deleted_at,omitempty"`
}

type CreateProduct struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Brand       string  `json:"brand"`
	Category    string  `json:"category"`
	Price       float64 `json:"price"`
	Stock       int     `json:"stock"`
}

type UpdateProduct = CreateProduct

func (input CreateProduct) Validate() error {
	if strings.TrimSpace(input.Name) == "" || strings.TrimSpace(input.Brand) == "" || strings.TrimSpace(input.Category) == "" {
		return ErrInvalid
	}
	if input.Price < 0 || input.Stock < 0 {
		return ErrInvalid
	}
	return nil
}
