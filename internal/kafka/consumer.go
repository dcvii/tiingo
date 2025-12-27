package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/dcvii/tiingo/internal/database"
	"github.com/dcvii/tiingo/pkg/models"
	"github.com/segmentio/kafka-go"
)

// Consumer handles consuming messages from Kafka and persisting to database
type Consumer struct {
	reader *kafka.Reader
	db     *database.Manager
	config *Config
	logger *log.Logger
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(config *Config, db *database.Manager, logger *log.Logger) (*Consumer, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	if db == nil {
		return nil, fmt.Errorf("database manager is required")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        config.Brokers,
		Topic:          config.Topic,
		GroupID:        config.ConsumerGroup,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset, // Start from beginning for new consumer groups
		MaxWait:        500 * time.Millisecond,
	})

	if logger == nil {
		logger = log.Default()
	}

	return &Consumer{
		reader: reader,
		db:     db,
		config: config,
		logger: logger,
	}, nil
}

// Start begins consuming messages from Kafka
func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Printf("Starting Kafka consumer for topic: %s (group: %s)", c.config.Topic, c.config.ConsumerGroup)

	var (
		consecutiveErrors int
		maxRetries        = 3
		baseBackoff       = time.Second
	)

	for {
		select {
		case <-ctx.Done():
			c.logger.Println("Consumer shutting down...")
			return ctx.Err()
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return nil
				}
				c.logger.Printf("Error fetching message: %v", err)
				time.Sleep(time.Second)
				continue
			}

			// Process message with retry logic
			var processErr error
			for attempt := 0; attempt <= maxRetries; attempt++ {
				if attempt > 0 {
					backoff := baseBackoff * time.Duration(1<<uint(attempt-1)) // Exponential backoff
					c.logger.Printf("Retrying message processing (attempt %d/%d) after %v", attempt, maxRetries, backoff)
					time.Sleep(backoff)
				}

				processErr = c.processMessage(ctx, msg)
				if processErr == nil {
					consecutiveErrors = 0
					break
				}

				c.logger.Printf("Error processing message (offset=%d, attempt=%d/%d): %v", msg.Offset, attempt+1, maxRetries+1, processErr)
			}

			// If still failed after retries, log and skip to avoid blocking
			if processErr != nil {
				c.logger.Printf("Failed to process message after %d attempts (offset=%d), skipping: %v", maxRetries+1, msg.Offset, processErr)
				consecutiveErrors++

				// If too many consecutive errors, something may be wrong with the consumer
				if consecutiveErrors >= 10 {
					return fmt.Errorf("too many consecutive processing errors (%d), stopping consumer", consecutiveErrors)
				}
			}

			// Commit offset after processing (success or permanent failure)
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Printf("Error committing offset (will retry on next message): %v", err)
				// Don't return error here - Kafka will retry commit on next message
			}
		}
	}
}

// processMessage processes a single Kafka message
func (c *Consumer) processMessage(ctx context.Context, msg kafka.Message) error {
	c.logger.Printf("Processing message: key=%s, partition=%d, offset=%d", string(msg.Key), msg.Partition, msg.Offset)

	// Determine message type based on topic
	switch c.config.Topic {
	case TopicDailyPrices:
		return c.processDailyPrices(ctx, msg.Value)
	case TopicCryptoPrices:
		return c.processCryptoPrices(ctx, msg.Value)
	default:
		return fmt.Errorf("unknown topic: %s", c.config.Topic)
	}
}

// processDailyPrices handles daily price messages
func (c *Consumer) processDailyPrices(ctx context.Context, payload []byte) error {
	var msg models.KafkaMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return fmt.Errorf("unmarshaling daily prices message: %w", err)
	}

	c.logger.Printf("  Ticker: %s, Records: %d, FetchTime: %s",
		msg.Ticker, msg.RecordCount, msg.FetchTime.Format(time.RFC3339))

	// Convert to database price records
	priceRecords := make([]models.PriceRecord, 0, len(msg.Prices))
	for _, p := range msg.Prices {
		priceRecords = append(priceRecords, models.PriceRecord{
			Ticker:    msg.Ticker,
			Date:      p.Date.Time,
			Open:      p.Open,
			High:      p.High,
			Low:       p.Low,
			Close:     p.Close,
			Volume:    p.Volume,
			AdjClose:  p.AdjClose,
			AdjVolume: p.AdjVolume,
		})
	}

	// Insert into database (idempotent due to ON CONFLICT DO NOTHING)
	if err := c.db.InsertDailyPrices(ctx, priceRecords); err != nil {
		return fmt.Errorf("inserting daily prices: %w", err)
	}

	// Log successful sync
	if err := c.db.LogSyncEvent(ctx, &models.SyncEvent{
		Ticker:          msg.Ticker,
		SyncDate:        time.Now(),
		RecordsInserted: len(priceRecords),
		Status:          "SUCCESS",
		ErrorMessage:    "",
	}); err != nil {
		c.logger.Printf("  Warning: failed to log sync event: %v", err)
	}

	c.logger.Printf("  ✅ Successfully persisted %d price records for %s", len(priceRecords), msg.Ticker)
	return nil
}

// processCryptoPrices handles crypto price messages
func (c *Consumer) processCryptoPrices(ctx context.Context, payload []byte) error {
	var msg models.CryptoKafkaMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return fmt.Errorf("unmarshaling crypto prices message: %w", err)
	}

	c.logger.Printf("  Crypto Ticker: %s, Records: %d, FetchTime: %s",
		msg.Ticker, msg.RecordCount, msg.FetchTime.Format(time.RFC3339))

	// TODO: Implement crypto price persistence when crypto schema is ready
	c.logger.Printf("  ⚠️  Crypto persistence not yet implemented")

	return nil
}

// Close closes the Kafka consumer connection
func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("closing Kafka reader: %w", err)
	}
	return nil
}

// Stats returns consumer statistics
func (c *Consumer) Stats() kafka.ReaderStats {
	return c.reader.Stats()
}
