package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/dcvii/tiingo/internal/database"
	"github.com/dcvii/tiingo/internal/kafka"
)

func main() {
	// Parse command-line flags
	dbPath := flag.String("db", "portfolio.duckdb", "Path to DuckDB database file")
	brokers := flag.String("brokers", "gold:9092", "Comma-separated list of Kafka brokers")
	topic := flag.String("topic", kafka.TopicDailyPrices, "Kafka topic to consume from")
	consumerGroup := flag.String("group", kafka.ConsumerGroupDB, "Kafka consumer group ID")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	flag.Parse()

	// Initialize logger
	logger := log.New(os.Stdout, "", log.LstdFlags)
	logger.Println("Starting Tiingo Kafka Consumer")
	if *verbose {
		logger.Printf("Database: %s\n", *dbPath)
		logger.Printf("Brokers: %s\n", *brokers)
		logger.Printf("Topic: %s\n", *topic)
		logger.Printf("Consumer Group: %s\n", *consumerGroup)
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

	// Parse brokers
	brokerList := strings.Split(*brokers, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

	// Create Kafka consumer config
	config := kafka.NewConsumerConfig(brokerList, *topic, *consumerGroup)

	// Initialize Kafka consumer
	consumer, err := kafka.NewConsumer(config, db, logger)
	if err != nil {
		log.Fatalf("Error creating Kafka consumer: %v", err)
	}
	defer consumer.Close()

	logger.Println("✅ Kafka consumer initialized")

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start consumer in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- consumer.Start(ctx)
	}()

	logger.Printf("🚀 Consumer started. Listening on topic: %s\n", *topic)
	logger.Println("Press Ctrl+C to stop...")

	// Wait for shutdown signal or error
	select {
	case sig := <-sigChan:
		logger.Printf("\nReceived signal: %v\n", sig)
		cancel()
		logger.Println("Shutting down gracefully...")
	case err := <-errChan:
		if err != nil && err != context.Canceled {
			logger.Printf("Consumer error: %v\n", err)
		}
	}

	// Print stats before exit
	if *verbose {
		stats := consumer.Stats()
		fmt.Printf("\n📊 Consumer Statistics:\n")
		fmt.Printf("  Messages: %d\n", stats.Messages)
		fmt.Printf("  Bytes: %d\n", stats.Bytes)
		fmt.Printf("  Errors: %d\n", stats.Errors)
	}

	logger.Println("✅ Consumer stopped")
}
