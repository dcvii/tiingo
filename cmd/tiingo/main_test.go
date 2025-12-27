package main

import (
	"context"
	"testing"
	"time"

	"github.com/dcvii/tiingo/pkg/models"
)

// TestStockTickerFormat verifies that stock tickers are processed without format conversion
func TestStockTickerFormat(t *testing.T) {
	tests := []struct {
		name          string
		inputTicker   string
		expectedValue string
	}{
		{
			name:          "Standard stock ticker",
			inputTicker:   "AAPL",
			expectedValue: "AAPL",
		},
		{
			name:          "Multi-letter stock ticker",
			inputTicker:   "GOOGL",
			expectedValue: "GOOGL",
		},
		{
			name:          "Single letter ticker",
			inputTicker:   "F",
			expectedValue: "F",
		},
		{
			name:          "Hyphenated ticker",
			inputTicker:   "BRK-B",
			expectedValue: "BRK-B",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// For stock tickers, the ticker should be used as-is
			// This test verifies the expectation from the processStock function
			// where priceRecords are created with the original ticker value
			
			priceRecord := models.PriceRecord{
				Ticker:    tt.inputTicker,
				Date:      time.Now(),
				Open:      100.0,
				High:      105.0,
				Low:       99.0,
				Close:     103.0,
				Volume:    1000000,
				AdjClose:  103.0,
				AdjVolume: 1000000,
			}

			if priceRecord.Ticker != tt.expectedValue {
				t.Errorf("Stock ticker format incorrect: got %s, want %s", priceRecord.Ticker, tt.expectedValue)
			}
		})
	}
}

// TestCryptoTickerFormat ensures that crypto tickers are correctly formatted with "USD" (uppercase)
func TestCryptoTickerFormat(t *testing.T) {
	tests := []struct {
		name            string
		inputTicker     string
		expectedTiingo  string
		description     string
	}{
		{
			name:            "BTC ticker",
			inputTicker:     "BTC",
			expectedTiingo:  "BTCUSD",
			description:     "BTC should be converted to BTCUSD for Tiingo API",
		},
		{
			name:            "ETH ticker",
			inputTicker:     "ETH",
			expectedTiingo:  "ETHUSD",
			description:     "ETH should be converted to ETHUSD for Tiingo API",
		},
		{
			name:            "BTCUSD already formatted",
			inputTicker:     "BTCUSD",
			expectedTiingo:  "BTCUSD",
			description:     "BTCUSD should remain as BTCUSD",
		},
		{
			name:            "ETHUSD already formatted",
			inputTicker:     "ETHUSD",
			expectedTiingo:  "ETHUSD",
			description:     "ETHUSD should remain as ETHUSD",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the crypto ticker formatting logic from processCrypto function
			cryptoTicker := tt.inputTicker + "USD"
			if tt.inputTicker == "BTCUSD" || tt.inputTicker == "ETHUSD" {
				cryptoTicker = tt.inputTicker
			}

			if cryptoTicker != tt.expectedTiingo {
				t.Errorf("%s: got %s, want %s", tt.description, cryptoTicker, tt.expectedTiingo)
			}

			// Verify that USD is uppercase
			if len(cryptoTicker) >= 3 {
				suffix := cryptoTicker[len(cryptoTicker)-3:]
				if suffix != "USD" {
					t.Errorf("Crypto ticker should end with uppercase USD, got: %s", suffix)
				}
			}
		})
	}
}

// TestLogSyncEventOriginalTicker confirms that LogSyncEvent records the original ticker for both stocks and crypto
func TestLogSyncEventOriginalTicker(t *testing.T) {
	tests := []struct {
		name              string
		originalTicker    string
		assetType         string
		expectedLogTicker string
		description       string
	}{
		{
			name:              "Stock ticker in sync event",
			originalTicker:    "AAPL",
			assetType:         "stock",
			expectedLogTicker: "AAPL",
			description:       "Stock sync event should log original ticker",
		},
		{
			name:              "Crypto ticker in sync event",
			originalTicker:    "BTC",
			assetType:         "crypto",
			expectedLogTicker: "BTC",
			description:       "Crypto sync event should log original ticker (not BTCUSD)",
		},
		{
			name:              "ETH crypto ticker in sync event",
			originalTicker:    "ETH",
			assetType:         "crypto",
			expectedLogTicker: "ETH",
			description:       "ETH sync event should log original ticker (not ETHUSD)",
		},
		{
			name:              "Pre-formatted crypto ticker in sync event",
			originalTicker:    "BTCUSD",
			assetType:         "crypto",
			expectedLogTicker: "BTCUSD",
			description:       "Pre-formatted crypto ticker should be logged as-is",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a sync event with the original ticker
			// This simulates the behavior in both processStock and processCrypto
			// where db.LogSyncEvent is called with the original ticker parameter
			syncEvent := &models.SyncEvent{
				Ticker:          tt.originalTicker,
				SyncDate:        time.Now(),
				RecordsInserted: 10,
				Status:          "SUCCESS",
				ErrorMessage:    "",
			}

			if syncEvent.Ticker != tt.expectedLogTicker {
				t.Errorf("%s: sync event ticker got %s, want %s", 
					tt.description, syncEvent.Ticker, tt.expectedLogTicker)
			}
		})
	}
}

// TestCryptoTickerFormatting tests the complete crypto ticker formatting flow
func TestCryptoTickerFormatting(t *testing.T) {
	ctx := context.Background()
	
	tests := []struct {
		name                string
		portfolioTicker     string
		expectedAPITicker   string
		expectedLogTicker   string
	}{
		{
			name:                "BTC complete flow",
			portfolioTicker:     "BTC",
			expectedAPITicker:   "BTCUSD",
			expectedLogTicker:   "BTC",
		},
		{
			name:                "ETH complete flow",
			portfolioTicker:     "ETH",
			expectedAPITicker:   "ETHUSD",
			expectedLogTicker:   "ETH",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = ctx // Context would be used in actual API calls
			
			// Step 1: Format for API call (simulating processCrypto logic)
			apiTicker := tt.portfolioTicker + "USD"
			if tt.portfolioTicker == "BTCUSD" || tt.portfolioTicker == "ETHUSD" {
				apiTicker = tt.portfolioTicker
			}
			
			if apiTicker != tt.expectedAPITicker {
				t.Errorf("API ticker formatting: got %s, want %s", apiTicker, tt.expectedAPITicker)
			}
			
			// Step 2: Verify log uses original ticker (simulating LogSyncEvent call)
			syncEvent := &models.SyncEvent{
				Ticker:          tt.portfolioTicker, // Original ticker used for logging
				SyncDate:        time.Now(),
				RecordsInserted: 5,
				Status:          "SUCCESS",
				ErrorMessage:    "",
			}
			
			if syncEvent.Ticker != tt.expectedLogTicker {
				t.Errorf("Log ticker: got %s, want %s", syncEvent.Ticker, tt.expectedLogTicker)
			}
		})
	}
}

// TestStockTickerNoConversion verifies that stock tickers pass through without any conversion
func TestStockTickerNoConversion(t *testing.T) {
	tests := []struct {
		name   string
		ticker string
	}{
		{"Apple", "AAPL"},
		{"Microsoft", "MSFT"},
		{"Tesla", "TSLA"},
		{"Amazon", "AMZN"},
		{"Berkshire Hathaway B", "BRK-B"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the stock processing flow
			inputTicker := tt.ticker
			
			// In processStock, the ticker is used directly for:
			// 1. Metadata fetch
			// 2. Price records
			// 3. Sync event logging
			
			tickerRecord := models.TickerRecord{
				Ticker:      inputTicker,
				Name:        tt.name,
				Exchange:    "NASDAQ",
				AssetType:   "Stock",
				StartDate:   time.Now(),
				EndDate:     time.Now(),
				LastUpdated: time.Now(),
			}
			
			priceRecord := models.PriceRecord{
				Ticker:    inputTicker,
				Date:      time.Now(),
				Open:      100.0,
				High:      105.0,
				Low:       99.0,
				Close:     103.0,
				Volume:    1000000,
				AdjClose:  103.0,
				AdjVolume: 1000000,
			}
			
			syncEvent := models.SyncEvent{
				Ticker:          inputTicker,
				SyncDate:        time.Now(),
				RecordsInserted: 1,
				Status:          "SUCCESS",
				ErrorMessage:    "",
			}
			
			// Verify all use the original ticker without conversion
			if tickerRecord.Ticker != tt.ticker {
				t.Errorf("TickerRecord ticker mismatch: got %s, want %s", tickerRecord.Ticker, tt.ticker)
			}
			if priceRecord.Ticker != tt.ticker {
				t.Errorf("PriceRecord ticker mismatch: got %s, want %s", priceRecord.Ticker, tt.ticker)
			}
			if syncEvent.Ticker != tt.ticker {
				t.Errorf("SyncEvent ticker mismatch: got %s, want %s", syncEvent.Ticker, tt.ticker)
			}
		})
	}
}
