package kafka

import (
	"testing"
	"time"
)

func TestNewProducerConfig(t *testing.T) {
	tests := []struct {
		name     string
		brokers  []string
		topic    string
		wantLen  int
		wantTopic string
	}{
		{
			name:     "with brokers",
			brokers:  []string{"gold:9092"},
			topic:    TopicDailyPrices,
			wantLen:  1,
			wantTopic: TopicDailyPrices,
		},
		{
			name:     "empty brokers uses default",
			brokers:  []string{},
			topic:    TopicDailyPrices,
			wantLen:  1,
			wantTopic: TopicDailyPrices,
		},
		{
			name:     "multiple brokers",
			brokers:  []string{"gold:9092", "silver:9092"},
			topic:    TopicCryptoPrices,
			wantLen:  2,
			wantTopic: TopicCryptoPrices,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewProducerConfig(tt.brokers, tt.topic)
			
			if len(config.Brokers) != tt.wantLen {
				t.Errorf("got %d brokers, want %d", len(config.Brokers), tt.wantLen)
			}
			
			if config.Topic != tt.wantTopic {
				t.Errorf("got topic %s, want %s", config.Topic, tt.wantTopic)
			}
			
			if config.Timeout != DefaultTimeout {
				t.Errorf("got timeout %v, want %v", config.Timeout, DefaultTimeout)
			}
		})
	}
}

func TestNewConsumerConfig(t *testing.T) {
	tests := []struct {
		name          string
		brokers       []string
		topic         string
		consumerGroup string
		wantGroup     string
	}{
		{
			name:          "with consumer group",
			brokers:       []string{"gold:9092"},
			topic:         TopicDailyPrices,
			consumerGroup: "test-group",
			wantGroup:     "test-group",
		},
		{
			name:          "empty consumer group uses default",
			brokers:       []string{"gold:9092"},
			topic:         TopicDailyPrices,
			consumerGroup: "",
			wantGroup:     ConsumerGroupDB,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewConsumerConfig(tt.brokers, tt.topic, tt.consumerGroup)
			
			if config.ConsumerGroup != tt.wantGroup {
				t.Errorf("got consumer group %s, want %s", config.ConsumerGroup, tt.wantGroup)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				Brokers: []string{"gold:9092"},
				Topic:   TopicDailyPrices,
				Timeout: DefaultTimeout,
			},
			wantErr: false,
		},
		{
			name: "no brokers",
			config: &Config{
				Brokers: []string{},
				Topic:   TopicDailyPrices,
				Timeout: DefaultTimeout,
			},
			wantErr: true,
		},
		{
			name: "no topic",
			config: &Config{
				Brokers: []string{"gold:9092"},
				Topic:   "",
				Timeout: DefaultTimeout,
			},
			wantErr: true,
		},
		{
			name: "invalid timeout",
			config: &Config{
				Brokers: []string{"gold:9092"},
				Topic:   TopicDailyPrices,
				Timeout: -1 * time.Second,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
