package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/linuxtips/descomplicando-system-design/exemplos/catalog-search-cdc/internal/events"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader  *kafka.Reader
	dlq     *kafka.Writer
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
	dlq := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic + ".dlq", Balancer: &kafka.Hash{}, AllowAutoTopicCreation: true}
	return &Consumer{reader: reader, dlq: dlq, handler: handler, logger: logger}
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.reader.Close()
	defer c.dlq.Close()
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
			if err := c.deadLetter(ctx, message, err); err != nil {
				return err
			}
			if err := c.reader.CommitMessages(ctx, message); err != nil {
				return err
			}
			continue
		}

		if err := c.applyWithRetry(ctx, event); err != nil {
			c.logger.Error("apply product event", "error", err, "product_id", event.Product.ID, "version", event.Product.Version, "partition", message.Partition, "offset", message.Offset)
			if err := c.deadLetter(ctx, message, err); err != nil {
				return err
			}
		}
		if err := c.reader.CommitMessages(ctx, message); err != nil {
			return err
		}
		c.logger.Info("product event committed", "product_id", event.Product.ID, "version", event.Product.Version, "partition", message.Partition, "offset", message.Offset)
	}
}

func (c *Consumer) applyWithRetry(ctx context.Context, event events.ProductEvent) error {
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		if err = c.handler.ApplyProductEvent(ctx, event); err == nil {
			return nil
		}
		if attempt == 3 {
			break
		}
		timer := time.NewTimer(time.Duration(attempt) * 200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func (c *Consumer) deadLetter(ctx context.Context, message kafka.Message, processingError error) error {
	dlqMessage := kafka.Message{
		Key:   message.Key,
		Value: message.Value,
		Headers: []kafka.Header{
			{Key: "source_topic", Value: []byte(message.Topic)},
			{Key: "source_partition", Value: []byte(fmt.Sprint(message.Partition))},
			{Key: "source_offset", Value: []byte(fmt.Sprint(message.Offset))},
			{Key: "error", Value: []byte(processingError.Error())},
		},
	}
	if err := c.dlq.WriteMessages(ctx, dlqMessage); err != nil {
		return fmt.Errorf("write DLQ: %w", err)
	}
	c.logger.Warn("product event sent to DLQ", "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "error", processingError)
	return nil
}
