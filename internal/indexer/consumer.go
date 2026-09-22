package indexer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/events"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader  *kafka.Reader
	handler *Handler
	logger  *slog.Logger
}

func NewConsumer(brokers []string, topic, groupID string, handler *Handler, logger *slog.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        time.Second,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
	})
	return &Consumer{reader: reader, handler: handler, logger: logger}
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.reader.Close()
	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}

		event, err := events.ParseProductEvent(message.Value)
		if errors.Is(err, events.ErrTombstone) {
			if err := c.reader.CommitMessages(ctx, message); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			c.logger.Error("parse Debezium event", "error", err, "topic", message.Topic, "partition", message.Partition, "offset", message.Offset)
			return err
		}

		if err := c.handler.ApplyProductEvent(ctx, event); err != nil {
			c.logger.Error("apply product event", "error", err, "product_id", event.Product.ID, "version", event.Product.Version, "partition", message.Partition, "offset", message.Offset)
			return err
		}
		if err := c.reader.CommitMessages(ctx, message); err != nil {
			return err
		}
		c.logger.Info("product event committed", "product_id", event.Product.ID, "version", event.Product.Version, "partition", message.Partition, "offset", message.Offset)
	}
}
