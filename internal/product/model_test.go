package product

import (
	"errors"
	"testing"
)

func TestCreateProductValidate(t *testing.T) {
	tests := []struct {
		name  string
		input CreateProduct
		want  error
	}{
		{name: "valid", input: CreateProduct{Name: "iPhone", Brand: "Apple", Category: "phones", Price: 999, Stock: 2}},
		{name: "missing name", input: CreateProduct{Brand: "Apple", Category: "phones"}, want: ErrInvalid},
		{name: "negative price", input: CreateProduct{Name: "iPhone", Brand: "Apple", Category: "phones", Price: -1}, want: ErrInvalid},
		{name: "negative stock", input: CreateProduct{Name: "iPhone", Brand: "Apple", Category: "phones", Stock: -1}, want: ErrInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.input.Validate()
			if !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}
