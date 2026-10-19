package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/search"
)

func BuildSearchCacheKey(params search.SearchParams) string {
	parts := []string{
		"v1",
		"q=" + normalize(params.Query),
		"brand=" + normalize(params.Brand),
		"category=" + normalize(params.Category),
		"min=" + formatPrice(params.MinPrice),
		"max=" + formatPrice(params.MaxPrice),
		"sort=" + normalize(params.Sort),
		"limit=" + strconv.Itoa(params.Limit),
		"page=" + strconv.Itoa(params.Page),
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return "search:" + hex.EncodeToString(digest[:])
}

func normalize(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

func formatPrice(value *float64) string {
	if value == nil {
		return ""
	}
	return strconv.FormatFloat(*value, 'f', -1, 64)
}
