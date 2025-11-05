package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/mdcb/tiingo-tracker/internal/portfolio"
	"github.com/mdcb/tiingo-tracker/internal/tiingo"
)

func main() {
	// Parse command-line flags
	portfolioPath := flag.String("portfolio", "portfolio.txt", "Path to portfolio file")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	flag.Parse()

	// Get API key from environment
	apiKey := os.Getenv("TIINGO_API_KEY")
	if apiKey == "" {
		log.Fatal("TIINGO_API_KEY environment variable not set")
	}

	// Initialize logger
	logger := log.New(os.Stdout, "", log.LstdFlags)
	if *verbose {
		logger.Println("Starting Tiingo API test")
		logger.Printf("Portfolio file: %s\n", *portfolioPath)
	}

	// Read portfolio
	reader := portfolio.NewReader()
	tickers, err := reader.ReadPortfolio(*portfolioPath)
	if err != nil {
		log.Fatalf("Error reading portfolio: %v", err)
	}

	logger.Printf("Found %d ticker(s): %v\n", len(tickers), tickers)

	// Initialize Tiingo client
	client := tiingo.NewClient(apiKey)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Process each ticker
	for i, ticker := range tickers {
		logger.Printf("\n[%d/%d] Processing %s...\n", i+1, len(tickers), ticker)
		
		// Convert ticker format for API (e.g., BRK/B -> BRK.B)
		apiTicker := convertTickerFormat(ticker)

		// Try to detect if it's a crypto ticker
		isCrypto := ticker == "BTC" || ticker == "ETH" || ticker == "BTCUSD"

		if isCrypto {
			// Handle cryptocurrency
			if err := processCrypto(ctx, client, ticker, logger, *verbose); err != nil {
				logger.Printf("❌ Error processing crypto %s: %v\n", ticker, err)
				continue
			}
		} else {
			// Handle stock
			if err := processStock(ctx, client, apiTicker, logger, *verbose); err != nil {
				logger.Printf("❌ Error processing stock %s: %v\n", ticker, err)
				continue
			}
		}
	}

	logger.Println("\n✅ Test complete!")
}

func processStock(ctx context.Context, client *tiingo.Client, ticker string, logger *log.Logger, verbose bool) error {
	// Fetch metadata
	metadata, err := client.GetTickerMetadata(ctx, ticker)
	if err != nil {
		return fmt.Errorf("fetching metadata: %w", err)
	}

	logger.Printf("  Name: %s\n", metadata.Name)
	logger.Printf("  Exchange: %s\n", metadata.Exchange)
	logger.Printf("  Type: %s\n", metadata.AssetType)

	if verbose {
		metadataJSON, _ := json.MarshalIndent(metadata, "  ", "  ")
		logger.Printf("  Metadata:\n%s\n", string(metadataJSON))
	}

	// Fetch recent prices (last 5 days)
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -5)

	prices, err := client.GetDailyPrices(ctx, ticker, startDate, endDate)
	if err != nil {
		return fmt.Errorf("fetching prices: %w", err)
	}

	logger.Printf("  Retrieved %d price records\n", len(prices))

	if len(prices) > 0 {
		latest := prices[len(prices)-1]
		logger.Printf("  Latest Close: $%.2f (Date: %s)\n",
			latest.Close,
			latest.Date.Format("2006-01-02"))

		if verbose {
			logger.Println("  Recent prices:")
			for _, price := range prices {
				logger.Printf("    %s: Open=$%.2f High=$%.2f Low=$%.2f Close=$%.2f Volume=%d\n",
					price.Date.Format("2006-01-02"),
					price.Open, price.High, price.Low, price.Close, price.Volume)
			}
		}
	}

	return nil
}

func processCrypto(ctx context.Context, client *tiingo.Client, ticker string, logger *log.Logger, verbose bool) error {
	// For crypto, use the standard ticker format (e.g., "btcusd")
	cryptoTicker := ticker + "usd"
	if ticker == "BTCUSD" || ticker == "ETHUSD" {
		cryptoTicker = ticker
	}

	logger.Printf("  Type: Cryptocurrency\n")
	logger.Printf("  Ticker: %s\n", cryptoTicker)

	// Fetch recent crypto prices (last 5 days)
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -5)

	prices, err := client.GetCryptoPrices(ctx, cryptoTicker, startDate, endDate)
	if err != nil {
		return fmt.Errorf("fetching crypto prices: %w", err)
	}

	logger.Printf("  Retrieved %d price records\n", len(prices))

	if len(prices) > 0 {
		latest := prices[len(prices)-1]
		logger.Printf("  Latest Close: $%.2f (Date: %s)\n",
			latest.Close,
			latest.PriceDate.Format("2006-01-02"))

		if verbose {
			logger.Println("  Recent prices:")
			for _, price := range prices {
				logger.Printf("    %s: Open=$%.2f High=$%.2f Low=$%.2f Close=$%.2f Volume=%.2f\n",
					price.PriceDate.Format("2006-01-02"),
					price.Open, price.High, price.Low, price.Close, price.Volume)
			}
		}
	}

	return nil
}

// convertTickerFormat converts ticker symbols to Tiingo API format
// e.g., BRK/B -> brk-b (Tiingo uses lowercase with hyphen for class shares)
func convertTickerFormat(ticker string) string {
	formatted := strings.ReplaceAll(ticker, "/", "-")
	return strings.ToLower(formatted)
}
