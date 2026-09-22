package events

import (
	"errors"
	"testing"
)

func TestParseProductEventUpdate(t *testing.T) {
	payload := []byte(`{
      "before":{"id":"8b31c36e-bb75-48af-8241-57b665d7c75a","name":"Old","brand":"Apple","category":"phones","price":900,"stock":1,"version":1,"active":true,"created_at":1767225600000000,"updated_at":1767225600000000},
      "after":{"id":"8b31c36e-bb75-48af-8241-57b665d7c75a","name":"New","brand":"Apple","category":"phones","price":950,"stock":2,"version":2,"active":true,"created_at":1767225600000000,"updated_at":1767312000000000},
      "source":{"db":"catalog","schema":"public","table":"products","lsn":42},
      "op":"u"
    }`)
	event, err := ParseProductEvent(payload)
	if err != nil {
		t.Fatalf("ParseProductEvent() error = %v", err)
	}
	if event.Operation != OperationUpdate || event.Product.Version != 2 || event.Product.Name != "New" {
		t.Fatalf("unexpected event: %+v", event)
	}
}

func TestParseProductEventTombstone(t *testing.T) {
	if _, err := ParseProductEvent([]byte("null")); !errors.Is(err, ErrTombstone) {
		t.Fatalf("ParseProductEvent() error = %v, want ErrTombstone", err)
	}
}
