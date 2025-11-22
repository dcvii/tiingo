package kafka

import (
	"fmt"
	"time"
)

const (
	// DefaultBrokers is the default Kafka broker address
	DefaultBrokers = "localhost:9092"
	
	// TopicDailyPrices is the topic for daily price data
	TopicDailyPrices = "tiingo.daily_prices"
	
	// TopicCryptoPrices is the topic for crypto price data
	TopicCryptoPrices = "tiingo.crypto_prices"
	
	// ConsumerGroup for the database writer consumer
	ConsumerGroupDB = "tiingo-db-writer"
	
	// DefaultTimeout for Kafka operations
	DefaultTimeout = 10 * time.Second
)

// Config holds Kafka connection configuration
type Config struct {
	Brokers       []string
	Topic         string
	ConsumerGroup string
	BatchSize     int
	Timeout       time.Duration
}

// NewProducerConfig creates a configuration for the producer
func NewProducerConfig(brokers []string, topic string) *Config {
	if len(brokers) == 0 {
		brokers = []string{DefaultBrokers}
	}
	
	return &Config{
		Brokers:   brokers,
		Topic:     topic,
		BatchSize: 100,
		Timeout:   DefaultTimeout,
	}
}

// NewConsumerConfig creates a configuration for the consumer
func NewConsumerConfig(brokers []string, topic, consumerGroup string) *Config {
	if len(brokers) == 0 {
		brokers = []string{DefaultBrokers}
	}
	
	if consumerGroup == "" {
		consumerGroup = ConsumerGroupDB
	}
	
	return &Config{
		Brokers:       brokers,
		Topic:         topic,
		ConsumerGroup: consumerGroup,
		BatchSize:     100,
		Timeout:       DefaultTimeout,
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("at least one broker is required")
	}
	
	if c.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	
	return nil
}
