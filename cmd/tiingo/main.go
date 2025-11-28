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

	"github.com/mdcb/tiingo-tracker/internal/kafka"
	"github.com/mdcb/tiingo-tracker/internal/portfolio"
	"github.com/mdcb/tiingo-tracker/internal/tiingo"
)

func main() {
	// Parse command-line flags
	portfolioPath := flag.String("portfolio", "data/portfolio.txt", "Path to portfolio file")
	brokers := flag.String("brokers", "gold:9092", "Comma-separated list of Kafka brokers")
	topic := flag.String("topic", kafka.TopicDailyPrices, "Kafka topic to publish to")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	flag.Parse()

	// Get API key from environment
	apiKey := os.Getenv("TIINGO_API_KEY")
	if apiKey == "" {
		log.Fatal("ERROR: TIINGO_API_KEY environment variable not set.\n" +
			"Please set it using one of:\n" +
			"  1. Fetch from Vault: direnv allow (after vault-login)\n" +
			"  2. Set manually: export TIINGO_API_KEY=your_key_here")
	}

	// Initialize logger
	logger := log.New(os.Stdout, "", log.LstdFlags)
	logger.Println("Starting Tiingo API to Kafka Producer")
	if *verbose {
		logger.Printf("Portfolio file: %s\n", *portfolioPath)
		logger.Printf("Brokers: %s\n", *brokers)
		logger.Printf("Topic: %s\n", *topic)
	}

	// Parse brokers
	brokerList := strings.Split(*brokers, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

	// Initialize Kafka producer
	config := kafka.NewProducerConfig(brokerList, *topic)
	producer, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatalf("Error creating Kafka producer: %v", err)
	}
	defer producer.Close()
	logger.Println("✅ Kafka producer initialized")

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
	overallStart := time.Now()
	successCount := 0
	errorCount := 0
	for i, ticker := range tickers {
		tickerStart := time.Now()
		logger.Printf("\n[%d/%d] Processing %s...\n", i+1, len(tickers), ticker)

		// Try to detect if it's a crypto ticker
		isCrypto := ticker == "BTC" || ticker == "ETH" || ticker == "BTCUSD"

		var publishErr error
		if isCrypto {
			// Handle cryptocurrency
			publishErr = processCrypto(ctx, client, producer, ticker, logger, *verbose)
		} else {
			// Handle stock
			publishErr = processStock(ctx, client, producer, ticker, logger, *verbose)
		}

		if publishErr != nil {
			logger.Printf("❌ Error: %v\n", publishErr)
			errorCount++
		} else {
			successCount++
		}

		logger.Printf("⏱️  %s processing time: %v\n", ticker, time.Since(tickerStart))
	}

	// Display summary
	logger.Printf("\n✅ Publishing complete!\n")
	logger.Printf("  Success: %d\n", successCount)
	logger.Printf("  Errors: %d\n", errorCount)
	logger.Printf("⏱️  Total processing time: %v\n", time.Since(overallStart))

	if *verbose {
		stats := producer.Stats()
		logger.Printf("\n📊 Producer Statistics:\n")
		logger.Printf("  Messages: %d\n", stats.Messages)
		logger.Printf("  Bytes: %d\n", stats.Bytes)
		logger.Printf("  Errors: %d\n", stats.Errors)
	}
}

func processStock(ctx context.Context, client *tiingo.Client, producer *kafka.Producer, ticker string, logger *log.Logger, verbose bool) error {
	// Fetch metadata
	metadataStart := time.Now()
	metadata, err := client.GetTickerMetadata(ctx, ticker)
	logger.Printf("  ⏱️  Metadata fetch: %v\n", time.Since(metadataStart))
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

	// Determine date range (fetch last 30 days)
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -30)

	pricesStart := time.Now()
	prices, err := client.GetDailyPrices(ctx, ticker, startDate, endDate)
	logger.Printf("  ⏱️  Price fetch: %v\n", time.Since(pricesStart))
	if err != nil {
		return fmt.Errorf("fetching prices: %w", err)
	}

	logger.Printf("  Retrieved %d price records\n", len(prices))

	// Publish to Kafka
	publishStart := time.Now()
	if err := producer.PublishDailyPrices(ctx, ticker, metadata.AssetType, startDate, endDate, prices); err != nil {
		return fmt.Errorf("publishing to Kafka: %w", err)
	}
	logger.Printf("  ⏱️  Kafka publish: %v\n", time.Since(publishStart))
	logger.Printf("  ✅ Published %d records to Kafka\n", len(prices))

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

func processCrypto(ctx context.Context, client *tiingo.Client, producer *kafka.Producer, ticker string, logger *log.Logger, verbose bool) error {
	// For crypto, use the standard ticker format (e.g., "btcusd")
	cryptoTicker := ticker + "USD"
	if ticker == "BTCUSD" || ticker == "ETHUSD" {
		cryptoTicker = ticker
	}

	logger.Printf("  Type: Cryptocurrency\n")
	logger.Printf("  Ticker: %s\n", cryptoTicker)

	// Fetch recent crypto prices (last 5 days)
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -5)

	cryptoStart := time.Now()
	prices, err := client.GetCryptoPrices(ctx, cryptoTicker, startDate, endDate)
	logger.Printf("  ⏱️  Crypto price fetch: %v\n", time.Since(cryptoStart))
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
