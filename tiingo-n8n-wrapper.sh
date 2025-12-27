#!/bin/bash

# Tiingo N8N Wrapper Script
# 
# This script provides a simple interface for N8N to interact with the Tiingo data pipeline
# It can run the producer only, consumer only, or both in sequence
#
# Usage:
#   ./tiingo-n8n-wrapper.sh producer [options]    - Run producer only
#   ./tiingo-n8n-wrapper.sh consumer [options]    - Run consumer only  
#   ./tiingo-n8n-wrapper.sh both [options]        - Run producer then consumer
#   ./tiingo-n8n-wrapper.sh --help                - Show this help

set -e  # Exit on any error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRODUCER_BIN="${SCRIPT_DIR}/tiingo-n8n"
CONSUMER_BIN="${SCRIPT_DIR}/consumer-n8n"

# Default options
VERBOSE=""
PORTFOLIO_FILE="data/portfolio.txt"
BROKERS="gold:9092"
TOPIC="tiingo.daily_prices"
DB_PATH="portfolio.duckdb"
CONSUMER_GROUP="tiingo-db-writer"

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -v|--verbose)
                VERBOSE="-verbose"
                shift
                ;;
            --portfolio)
                PORTFOLIO_FILE="$2"
                shift 2
                ;;
            --brokers)
                BROKERS="$2"
                shift 2
                ;;
            --topic)
                TOPIC="$2"
                shift 2
                ;;
            --db)
                DB_PATH="$2"
                shift 2
                ;;
            --group)
                CONSUMER_GROUP="$2"
                shift 2
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

show_help() {
    echo "Tiingo N8N Wrapper Script"
    echo "========================="
    echo ""
    echo "Usage:"
    echo "  $0 producer [options]    - Run producer only (fetches data to Kafka)"
    echo "  $0 consumer [options]    - Run consumer only (processes Kafka to DB)"  
    echo "  $0 both [options]        - Run producer then consumer"
    echo "  $0 --help                - Show this help"
    echo ""
    echo "Options:"
    echo "  -v, --verbose           Enable verbose logging"
    echo "  --portfolio FILE        Portfolio file path (default: data/portfolio.txt)"
    echo "  --brokers LIST          Kafka brokers (default: gold:9092)"
    echo "  --topic TOPIC           Kafka topic (default: tiingo.daily_prices)"
    echo "  --db PATH               Database path (default: portfolio.duckdb)"
    echo "  --group GROUP           Consumer group (default: tiingo-db-writer)"
    echo ""
    echo "Environment Variables:"
    echo "  TIINGO_API_KEY          Required - Your Tiingo API key"
    echo ""
    echo "Examples:"
    echo "  # Run producer only with verbose output"
    echo "  $0 producer --verbose"
    echo ""
    echo "  # Run both producer and consumer"
    echo "  $0 both"
    echo ""
    echo "  # Run consumer with custom database"
    echo "  $0 consumer --db my-portfolio.duckdb"
}

check_binaries() {
    if [[ ! -f "$PRODUCER_BIN" ]]; then
        echo "Error: Producer binary not found at $PRODUCER_BIN"
        exit 1
    fi
    
    if [[ ! -f "$CONSUMER_BIN" ]]; then
        echo "Error: Consumer binary not found at $CONSUMER_BIN"
        exit 1
    fi
}

check_api_key() {
    if [[ -z "$TIINGO_API_KEY" ]]; then
        echo "Error: TIINGO_API_KEY environment variable not set"
        echo "Please set it using: export TIINGO_API_KEY=your_key_here"
        echo "Or use direnv: direnv allow"
        exit 1
    fi
}

run_producer() {
    echo "🚀 Running Tiingo Producer..."
    "$PRODUCER_BIN" \
        -portfolio "$PORTFOLIO_FILE" \
        -brokers "$BROKERS" \
        -topic "$TOPIC" \
        $VERBOSE
}

run_consumer() {
    echo "🚀 Running Tiingo Consumer..."
    # Run consumer with timeout (30 seconds should be enough for most portfolios)
    # Use gtimeout on macOS (from coreutils), fallback to timeout on Linux
    local timeout_cmd="timeout"
    if command -v gtimeout >/dev/null 2>&1; then
        timeout_cmd="gtimeout"
    elif ! command -v timeout >/dev/null 2>&1; then
        echo "⚠️  No timeout command available, running consumer without timeout"
        "$CONSUMER_BIN" \
            -db "$DB_PATH" \
            -brokers "$BROKERS" \
            -topic "$TOPIC" \
            -group "$CONSUMER_GROUP" \
            $VERBOSE || {
                echo "❌ Consumer failed"
                exit 1
            }
        return
    fi
    
    $timeout_cmd 30s "$CONSUMER_BIN" \
        -db "$DB_PATH" \
        -brokers "$BROKERS" \
        -topic "$TOPIC" \
        -group "$CONSUMER_GROUP" \
        $VERBOSE || {
            # timeout returns 124 if command timed out
            if [[ $? -eq 124 ]]; then
                echo "✅ Consumer completed (timed out after processing messages)"
            else
                echo "❌ Consumer failed"
                exit 1
            fi
        }
}

main() {
    if [[ $# -eq 0 ]]; then
        echo "Error: No command specified"
        show_help
        exit 1
    fi
    
    # Handle help as first argument
    if [[ "$1" == "--help" || "$1" == "-h" ]]; then
        show_help
        exit 0
    fi
    
    local command=$1
    shift
    
    # Parse remaining arguments
    parse_args "$@"
    
    # Check prerequisites
    check_binaries
    check_api_key
    
    case $command in
        producer)
            run_producer
            ;;
        consumer)
            run_consumer
            ;;
        both)
            run_producer
            echo ""
            echo "⏱️  Waiting 2 seconds before starting consumer..."
            sleep 2
            run_consumer
            ;;
        *)
            echo "Error: Unknown command '$command'"
            show_help
            exit 1
            ;;
    esac
    
    echo ""
    echo "✅ Operation completed successfully!"
}

# Change to script directory to ensure relative paths work
cd "$SCRIPT_DIR"

# Run main function with all arguments
main "$@"