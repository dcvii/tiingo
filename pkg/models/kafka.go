package models

import "time"

// KafkaMessage represents a message published to Kafka
type KafkaMessage struct {
	MessageID  string       `json:"message_id"`
	Ticker     string       `json:"ticker"`
	AssetType  string       `json:"asset_type"`
	FetchTime  time.Time    `json:"fetch_time"`
	StartDate  time.Time    `json:"start_date"`
	EndDate    time.Time    `json:"end_date"`
	Prices     []DailyPrice `json:"prices"`
	RecordCount int         `json:"record_count"`
}

// CryptoKafkaMessage represents a crypto message published to Kafka
type CryptoKafkaMessage struct {
	MessageID   string        `json:"message_id"`
	Ticker      string        `json:"ticker"`
	BaseCurrency string       `json:"base_currency"`
	QuoteCurrency string      `json:"quote_currency"`
	FetchTime   time.Time     `json:"fetch_time"`
	StartDate   time.Time     `json:"start_date"`
	EndDate     time.Time     `json:"end_date"`
	Prices      []CryptoPrice `json:"prices"`
	RecordCount int           `json:"record_count"`
}

// MetadataKafkaMessage represents ticker metadata published to Kafka
type MetadataKafkaMessage struct {
	MessageID string          `json:"message_id"`
	FetchTime time.Time       `json:"fetch_time"`
	Metadata  *TickerMetadata `json:"metadata"`
}
