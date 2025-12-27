package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/dcvii/tiingo/pkg/models"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// Producer handles publishing messages to Kafka
type Producer struct {
	writer *kafka.Writer
	config *Config
}

// NewProducer creates a new Kafka producer
func NewProducer(config *Config) (*Producer, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Configure SASL transport if credentials are available
	transport := &kafka.Transport{}
	
	// Check for SASL credentials in environment
	kafkaUsername := os.Getenv("KAFKA_USERNAME")
	kafkaPassword := os.Getenv("KAFKA_PASSWORD")
	kafkaSecurityProtocol := os.Getenv("KAFKA_SECURITY_PROTOCOL")
	
	if kafkaUsername != "" && kafkaPassword != "" && kafkaSecurityProtocol == "SASL_PLAINTEXT" {
		transport.SASL = plain.Mechanism{
			Username: kafkaUsername,
			Password: kafkaPassword,
		}
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		Topic:        config.Topic,
		Balancer:     &kafka.Hash{}, // Partition by key (ticker)
		BatchSize:    config.BatchSize,
		BatchTimeout: 10 * time.Millisecond,
		Compression:  kafka.Snappy,
		RequiredAcks: kafka.RequireOne, // Wait for leader ack
		Async:        false,             // Synchronous writes for reliability
		Transport:    transport,         // Use configured transport with SASL
	}

	return &Producer{
		writer: writer,
		config: config,
	}, nil
}

// PublishDailyPrices publishes daily price data to Kafka
func (p *Producer) PublishDailyPrices(ctx context.Context, ticker, assetType string, startDate, endDate time.Time, prices []models.DailyPrice) error {
	msg := &models.KafkaMessage{
		MessageID:   uuid.New().String(),
		Ticker:      ticker,
		AssetType:   assetType,
		FetchTime:   time.Now(),
		StartDate:   startDate,
		EndDate:     endDate,
		Prices:      prices,
		RecordCount: len(prices),
	}

	return p.publishMessage(ctx, ticker, msg)
}

// PublishCryptoPrices publishes crypto price data to Kafka
func (p *Producer) PublishCryptoPrices(ctx context.Context, ticker, baseCurrency, quoteCurrency string, startDate, endDate time.Time, prices []models.CryptoPrice) error {
	msg := &models.CryptoKafkaMessage{
		MessageID:     uuid.New().String(),
		Ticker:        ticker,
		BaseCurrency:  baseCurrency,
		QuoteCurrency: quoteCurrency,
		FetchTime:     time.Now(),
		StartDate:     startDate,
		EndDate:       endDate,
		Prices:        prices,
		RecordCount:   len(prices),
	}

	return p.publishMessage(ctx, ticker, msg)
}

// PublishMetadata publishes ticker metadata to Kafka
func (p *Producer) PublishMetadata(ctx context.Context, metadata *models.TickerMetadata) error {
	msg := &models.MetadataKafkaMessage{
		MessageID: uuid.New().String(),
		FetchTime: time.Now(),
		Metadata:  metadata,
	}

	return p.publishMessage(ctx, metadata.Ticker, msg)
}

// publishMessage is a generic method to publish any message to Kafka
func (p *Producer) publishMessage(ctx context.Context, key string, message interface{}) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshaling message: %w", err)
	}

	kafkaMsg := kafka.Message{
		Key:   []byte(key), // Use ticker as partition key for ordering
		Value: payload,
		Time:  time.Now(),
	}

	if err := p.writer.WriteMessages(ctx, kafkaMsg); err != nil {
		return fmt.Errorf("writing message to Kafka: %w", err)
	}

	return nil
}

// Close closes the Kafka producer connection
func (p *Producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("closing Kafka writer: %w", err)
	}
	return nil
}

// Stats returns producer statistics
func (p *Producer) Stats() kafka.WriterStats {
	return p.writer.Stats()
}
