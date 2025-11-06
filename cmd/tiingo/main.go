package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/mdcb/tiingo-tracker/internal/database"
	"github.com/mdcb/tiingo-tracker/internal/portfolio"
	"github.com/mdcb/tiingo-tracker/internal/tiingo"
	"github.com/mdcb/tiingo-tracker/pkg/models"
)

func main() {
	// Parse command-line flags
	portfolioPath := flag.String("portfolio", "data/portfolio.txt", "Path to portfolio file")
	dbPath := flag.String("db", "portfolio.duckdb", "Path to DuckDB database file")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	flag.Parse()

	// Get API key from environment
	apiKey := os.Getenv("TIINGO_API_KEY")
	if apiKey == "" {
		log.Fatal("TIINGO_API_KEY environment variable not set")
	}

	// Initialize logger
	logger := log.New(os.Stdout, "", log.LstdFlags)
	logger.Println("Starting Tiingo Portfolio Sync")
	if *verbose {
		logger.Printf("Portfolio file: %s\n", *portfolioPath)
		logger.Printf("Database: %s\n", *dbPath)
	}

	// Initialize database
	db, err := database.NewManager(*dbPath)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Initialize schema
	if err := db.InitSchema(); err != nil {
		log.Fatalf("Error initializing schema: %v", err)
	}
	logger.Println("✅ Database initialized")

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

		// Try to detect if it's a crypto ticker
		isCrypto := ticker == "BTC" || ticker == "ETH" || ticker == "BTCUSD"

		var syncErr error
		if isCrypto {
			// Handle cryptocurrency
			syncErr = processCrypto(ctx, client, db, ticker, logger, *verbose)
		} else {
			// Handle stock
syncErr = processStock(ctx, client, db, ticker, logger, *verbose)
		}

		// Log sync event
		if syncErr != nil {
			logger.Printf("❌ Error: %v\n", syncErr)
			_ = db.LogSyncEvent(ctx, &models.SyncEvent{
Ticker:          ticker,
				SyncDate:        time.Now(),
				RecordsInserted: 0,
				Status:          "ERROR",
				ErrorMessage:    syncErr.Error(),
			})
		}
	}

	// Display summary
	totalTickers, _ := db.GetTickerCount(ctx)
	logger.Printf("\n✅ Sync complete! Total tickers in database: %d\n", totalTickers)
}

func processStock(ctx context.Context, client *tiingo.Client, db *database.Manager, ticker string, logger *log.Logger, verbose bool) error {
	// Fetch metadata
	metadata, err := client.GetTickerMetadata(ctx, ticker)
	if err != nil {
		return fmt.Errorf("fetching metadata: %w", err)
	}

	logger.Printf("  Name: %s\n", metadata.Name)
	logger.Printf("  Exchange: %s\n", metadata.Exchange)
	logger.Printf("  Type: %s\n", metadata.AssetType)

	// Save metadata to database
	tickerRecord := &models.TickerRecord{
		Ticker:      ticker,
		Name:        metadata.Name,
		Exchange:    metadata.Exchange,
		AssetType:   metadata.AssetType,
		StartDate:   metadata.StartDate.Time,
		EndDate:     metadata.EndDate.Time,
		LastUpdated: time.Now(),
	}
	if err := db.UpsertTickerMetadata(ctx, tickerRecord); err != nil {
		return fmt.Errorf("saving ticker metadata: %w", err)
	}

	if verbose {
		metadataJSON, _ := json.MarshalIndent(metadata, "  ", "  ")
		logger.Printf("  Metadata:\n%s\n", string(metadataJSON))
	}

	// Check for last sync date
	lastSync, err := db.GetLastSyncDate(ctx, ticker)
	if err != nil {
		return fmt.Errorf("checking last sync: %w", err)
	}

	// Determine date range
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -30) // Default: last 30 days
	if lastSync != nil {
		startDate = lastSync.AddDate(0, 0, 1) // Start from day after last sync
		logger.Printf("  Last sync: %s\n", lastSync.Format("2006-01-02"))
	}

	prices, err := client.GetDailyPrices(ctx, ticker, startDate, endDate)
	if err != nil {
		return fmt.Errorf("fetching prices: %w", err)
	}

	logger.Printf("  Retrieved %d price records\n", len(prices))

	// Convert and save to database
	priceRecords := make([]models.PriceRecord, 0, len(prices))
	for _, p := range prices {
		priceRecords = append(priceRecords, models.PriceRecord{
			Ticker:    ticker,
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

	if err := db.InsertDailyPrices(ctx, priceRecords); err != nil {
		return fmt.Errorf("saving prices: %w", err)
	}

	// Log successful sync
	if err := db.LogSyncEvent(ctx, &models.SyncEvent{
		Ticker:          ticker,
		SyncDate:        time.Now(),
		RecordsInserted: len(priceRecords),
		Status:          "SUCCESS",
		ErrorMessage:    "",
	}); err != nil {
		logger.Printf("  Warning: failed to log sync event: %v\n", err)
	}

	totalPrices, _ := db.GetPriceCount(ctx, ticker)
	logger.Printf("  Saved %d new records (total: %d)\n", len(priceRecords), totalPrices)

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

func processCrypto(ctx context.Context, client *tiingo.Client, db *database.Manager, ticker string, logger *log.Logger, verbose bool) error {
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
