package events

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

var ErrTombstone = errors.New("Debezium tombstone")

type Operation string

const (
	OperationCreate   Operation = "create"
	OperationUpdate   Operation = "update"
	OperationDelete   Operation = "delete"
	OperationSnapshot Operation = "snapshot"
)

type ProductState struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Brand       string            `json:"brand"`
	Category    string            `json:"category"`
	Price       float64           `json:"price"`
	Stock       int               `json:"stock"`
	Version     int64             `json:"version"`
	Active      bool              `json:"active"`
	CreatedAt   DebeziumTimestamp `json:"created_at"`
	UpdatedAt   DebeziumTimestamp `json:"updated_at"`
}

type DebeziumTimestamp int64

func (t *DebeziumTimestamp) UnmarshalJSON(payload []byte) error {
	raw := string(payload)
	if len(raw) >= 2 && raw[0] == '"' {
		value, err := strconv.Unquote(raw)
		if err != nil {
			return fmt.Errorf("decode Debezium timestamp: %w", err)
		}
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return fmt.Errorf("parse Debezium timestamp %q: %w", value, err)
		}
		*t = DebeziumTimestamp(parsed.UnixMicro())
		return nil
	}
	micros, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("parse Debezium timestamp %q: %w", raw, err)
	}
	*t = DebeziumTimestamp(micros)
	return nil
}

type ProductEvent struct {
	Operation Operation     `json:"operation"`
	Product   ProductState  `json:"product"`
	Before    *ProductState `json:"before,omitempty"`
	Source    Source        `json:"source"`
}

type Source struct {
	Database string `json:"db"`
	Schema   string `json:"schema"`
	Table    string `json:"table"`
	LSN      int64  `json:"lsn"`
}

type envelope struct {
	Before *ProductState `json:"before"`
	After  *ProductState `json:"after"`
	Source Source        `json:"source"`
	Op     string        `json:"op"`
}

func ParseProductEvent(value []byte) (ProductEvent, error) {
	if len(bytes.TrimSpace(value)) == 0 || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return ProductEvent{}, ErrTombstone
	}
	var envelope envelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		return ProductEvent{}, fmt.Errorf("decode Debezium envelope: %w", err)
	}

	event := ProductEvent{Before: envelope.Before, Source: envelope.Source}
	switch envelope.Op {
	case "c":
		event.Operation = OperationCreate
	case "u":
		event.Operation = OperationUpdate
	case "d":
		event.Operation = OperationDelete
	case "r":
		event.Operation = OperationSnapshot
	default:
		return ProductEvent{}, fmt.Errorf("unsupported Debezium operation %q", envelope.Op)
	}

	if envelope.After != nil {
		event.Product = *envelope.After
	} else if envelope.Before != nil {
		event.Product = *envelope.Before
	} else {
		return ProductEvent{}, errors.New("Debezium event has neither before nor after product")
	}
	if event.Product.ID == "" {
		return ProductEvent{}, errors.New("Debezium product id is empty")
	}
	return event, nil
}
