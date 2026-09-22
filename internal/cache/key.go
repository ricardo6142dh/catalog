package cache

import "github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/search"

func BuildSearchCacheKey(params search.SearchParams) string {
	// TODO(ricardo): canonicalize every response-affecting parameter and hash the result.
	panic("not implemented")
}
