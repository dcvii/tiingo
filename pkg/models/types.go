package models

import "time"

// DailyPrice represents a single day's price data from Tiingo
type DailyPrice struct {
	Date      JSONDate  `json:"date"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    int64     `json:"volume"`
	AdjClose  float64   `json:"adjClose"`
	AdjVolume int64     `json:"adjVolume"`
}

// JSONDate is a custom type to handle multiple date formats from Tiingo API
type JSONDate struct {
	time.Time
}

// UnmarshalJSON handles both "2006-01-02" and "2006-01-02T15:04:05Z" formats
func (jd *JSONDate) UnmarshalJSON(b []byte) error {
	s := string(b)
	// Remove quotes
	s = s[1 : len(s)-1]
	
	// Try datetime format first
	t, err := time.Parse(time.RFC3339, s)
	if err == nil {
		jd.Time = t
		return nil
	}
	
	// Try date-only format
	t, err = time.Parse("2006-01-02", s)
	if err == nil {
		jd.Time = t
		return nil
	}
	
	return err
}

// TickerMetadata represents metadata about a ticker symbol
type TickerMetadata struct {
	Ticker      string   `json:"ticker"`
	Name        string   `json:"name"`
	Exchange    string   `json:"exchangeCode"`
	AssetType   string   `json:"assetType"`
	StartDate   JSONDate `json:"startDate"`
	EndDate     JSONDate `json:"endDate"`
	Description string   `json:"description"`
}

// IEXQuote represents real-time quote data from Tiingo IEX
type IEXQuote struct {
	Ticker    string    `json:"ticker"`
	Timestamp time.Time `json:"timestamp"`
	Last      float64   `json:"last"`
	LastSize  int       `json:"lastSize"`
	BidPrice  float64   `json:"bidPrice"`
	BidSize   int       `json:"bidSize"`
	AskPrice  float64   `json:"askPrice"`
	AskSize   int       `json:"askSize"`
	Volume    int64     `json:"volume"`
	PrevClose float64   `json:"prevClose"`
}

// CryptoPrice represents cryptocurrency price data
type CryptoPrice struct {
	Ticker         string   `json:"ticker"`
	BaseCurrency   string   `json:"baseCurrency"`
	QuoteCurrency  string   `json:"quoteCurrency"`
	PriceDate      JSONDate `json:"priceDate"`
	Open           float64  `json:"open"`
	High           float64  `json:"high"`
	Low            float64  `json:"low"`
	Close          float64  `json:"close"`
	Volume         float64  `json:"volume"`
	VolumeNotional float64  `json:"volumeNotional"`
}

// TickerRecord represents a ticker in the database
type TickerRecord struct {
	Ticker      string
	Name        string
	Exchange    string
	AssetType   string
	StartDate   time.Time
	EndDate     time.Time
	LastUpdated time.Time
}

// PriceRecord represents a daily price record in the database
type PriceRecord struct {
	Ticker    string
	Date      time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    int64
	AdjClose  float64
	AdjVolume int64
}

// SyncEvent represents a sync operation log entry
type SyncEvent struct {
	Ticker          string
	SyncDate        time.Time
	RecordsInserted int
	Status          string
	ErrorMessage    string
}
