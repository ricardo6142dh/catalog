package product

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	Create(context.Context, CreateProduct) (Product, error)
	Get(context.Context, string) (Product, error)
	Update(context.Context, string, UpdateProduct) (Product, error)
	Delete(context.Context, string) error
}

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

const productColumns = `id::text, name, description, brand, category, price::float8, stock, version, active, created_at, updated_at, deleted_at`

func scanProduct(row pgx.Row) (Product, error) {
	var product Product
	err := row.Scan(
		&product.ID,
		&product.Name,
		&product.Description,
		&product.Brand,
		&product.Category,
		&product.Price,
		&product.Stock,
		&product.Version,
		&product.Active,
		&product.CreatedAt,
		&product.UpdatedAt,
		&product.DeletedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Product{}, ErrNotFound
	}
	if err != nil {
		return Product{}, fmt.Errorf("scan product: %w", err)
	}
	return product, nil
}

func (r *PostgresRepository) Create(ctx context.Context, input CreateProduct) (Product, error) {
	row := r.pool.QueryRow(ctx, `
        INSERT INTO products (name, description, brand, category, price, stock)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING `+productColumns,
		input.Name, input.Description, input.Brand, input.Category, input.Price, input.Stock,
	)
	product, err := scanProduct(row)
	if err != nil {
		return Product{}, fmt.Errorf("create product: %w", err)
	}
	return product, nil
}

func (r *PostgresRepository) Get(ctx context.Context, id string) (Product, error) {
	row := r.pool.QueryRow(ctx, `SELECT `+productColumns+` FROM products WHERE id = $1 AND active`, id)
	product, err := scanProduct(row)
	if err != nil {
		return Product{}, fmt.Errorf("get product: %w", err)
	}
	return product, nil
}

func (r *PostgresRepository) Update(ctx context.Context, id string, input UpdateProduct) (Product, error) {
	row := r.pool.QueryRow(ctx, `
        UPDATE products
        SET name = $2,
            description = $3,
            brand = $4,
            category = $5,
            price = $6,
            stock = $7,
            version = version + 1,
            updated_at = NOW()
        WHERE id = $1 AND active
        RETURNING `+productColumns,
		id, input.Name, input.Description, input.Brand, input.Category, input.Price, input.Stock,
	)
	product, err := scanProduct(row)
	if err != nil {
		return Product{}, fmt.Errorf("update product: %w", err)
	}
	return product, nil
}

func (r *PostgresRepository) Delete(ctx context.Context, id string) error {
	command, err := r.pool.Exec(ctx, `
        UPDATE products
        SET active = FALSE,
            deleted_at = NOW(),
            updated_at = NOW(),
            version = version + 1
        WHERE id = $1 AND active`, id)
	if err != nil {
		return fmt.Errorf("delete product: %w", err)
	}
	if command.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
