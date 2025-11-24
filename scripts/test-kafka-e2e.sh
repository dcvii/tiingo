#!/bin/bash
# End-to-end test script for Kafka integration with Gold broker
# Tests producer -> Kafka (gold:9092) -> consumer flow

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
KAFKA_BROKER="gold:9092"
TEST_TOPIC="tiingo.test.daily_prices"
TEST_GROUP="tiingo-test-consumer"
TEST_DB="test_portfolio.duckdb"
TEST_PORTFOLIO="data/test_portfolio.txt"
TIMEOUT=30

echo -e "${BLUE}🧪 Tiingo Kafka End-to-End Test${NC}"
echo "================================================"
echo ""

# Step 0: Check prerequisites
echo -e "${YELLOW}📋 Step 0: Checking prerequisites...${NC}"

# Check if Gold server is reachable
if ! ping -c 1 gold &> /dev/null; then
    echo -e "${RED}❌ Cannot reach Gold server${NC}"
    exit 1
fi
echo -e "${GREEN}  ✓ Gold server reachable${NC}"

# Check if Kafka port is accessible
if ! nc -zv gold 9092 &> /dev/null; then
    echo -e "${RED}❌ Cannot connect to Kafka on gold:9092${NC}"
    exit 1
fi
echo -e "${GREEN}  ✓ Kafka broker accessible on gold:9092${NC}"

# Check if TIINGO_API_KEY is set
if [ -z "$TIINGO_API_KEY" ]; then
    echo -e "${RED}❌ TIINGO_API_KEY not set${NC}"
    echo "   Run: direnv allow"
    exit 1
fi
echo -e "${GREEN}  ✓ TIINGO_API_KEY configured${NC}"

# Check if binaries exist
if [ ! -f "./tiingo" ]; then
    echo -e "${YELLOW}  ⚠ Building producer...${NC}"
    go build -o tiingo ./cmd/tiingo
fi

if [ ! -f "./consumer" ]; then
    echo -e "${YELLOW}  ⚠ Building consumer...${NC}"
    go build -o consumer ./cmd/consumer
fi
echo -e "${GREEN}  ✓ Binaries ready${NC}"
echo ""

# Step 1: Setup test environment
echo -e "${YELLOW}📁 Step 1: Setting up test environment...${NC}"

# Create test portfolio with small data set
cat > "$TEST_PORTFOLIO" <<EOF
# Test Portfolio - Small dataset for E2E testing
AAPL
MSFT
EOF
echo -e "${GREEN}  ✓ Created test portfolio: $TEST_PORTFOLIO${NC}"

# Remove test database if exists
if [ -f "$TEST_DB" ]; then
    rm -f "$TEST_DB"
    echo -e "${GREEN}  ✓ Removed old test database${NC}"
fi

# Remove test database WAL file if exists
if [ -f "${TEST_DB}.wal" ]; then
    rm -f "${TEST_DB}.wal"
    echo -e "${GREEN}  ✓ Removed old WAL file${NC}"
fi

echo ""

# Step 2: Create/verify Kafka topic
echo -e "${YELLOW}📡 Step 2: Verifying Kafka topic...${NC}"

# Check if kafka-topics command is available
if command -v kafka-topics &> /dev/null; then
    # Try to create topic (will be skipped if exists)
    kafka-topics --create \
        --bootstrap-server "$KAFKA_BROKER" \
        --replication-factor 1 \
        --partitions 3 \
        --topic "$TEST_TOPIC" \
        --if-not-exists 2>/dev/null || true
    
    # List topics to verify
    if kafka-topics --list --bootstrap-server "$KAFKA_BROKER" 2>/dev/null | grep -q "$TEST_TOPIC"; then
        echo -e "${GREEN}  ✓ Topic '$TEST_TOPIC' exists${NC}"
    else
        echo -e "${YELLOW}  ⚠ Topic '$TEST_TOPIC' not found, but will be auto-created${NC}"
    fi
else
    echo -e "${YELLOW}  ⚠ kafka-topics not available, assuming auto-create enabled${NC}"
fi

echo ""

# Step 3: Start consumer in background
echo -e "${YELLOW}🎧 Step 3: Starting consumer...${NC}"

./consumer \
    -db "$TEST_DB" \
    -brokers "$KAFKA_BROKER" \
    -topic "$TEST_TOPIC" \
    -group "$TEST_GROUP" \
    -verbose > test_consumer.log 2>&1 &

CONSUMER_PID=$!
echo -e "${GREEN}  ✓ Consumer started (PID: $CONSUMER_PID)${NC}"
echo "     Logs: test_consumer.log"

# Wait for consumer to initialize
sleep 3

# Check if consumer is still running
if ! kill -0 $CONSUMER_PID 2>/dev/null; then
    echo -e "${RED}❌ Consumer failed to start${NC}"
    cat test_consumer.log
    exit 1
fi

echo ""

# Step 4: Run producer
echo -e "${YELLOW}📤 Step 4: Running producer...${NC}"

./tiingo \
    -portfolio "$TEST_PORTFOLIO" \
    -brokers "$KAFKA_BROKER" \
    -topic "$TEST_TOPIC" \
    -verbose 2>&1 | tee test_producer.log

echo -e "${GREEN}  ✓ Producer completed${NC}"
echo ""

# Step 5: Wait for consumer to process messages
echo -e "${YELLOW}⏳ Step 5: Waiting for consumer to process messages...${NC}"

sleep 5

# Step 6: Verify data in database
echo -e "${YELLOW}🔍 Step 6: Verifying data in database...${NC}"

# Check if database exists
if [ ! -f "$TEST_DB" ]; then
    echo -e "${RED}❌ Test database not created${NC}"
    kill $CONSUMER_PID 2>/dev/null || true
    exit 1
fi
echo -e "${GREEN}  ✓ Database file exists${NC}"

# Query database for results
TICKER_COUNT=$(duckdb "$TEST_DB" "SELECT COUNT(*) FROM tickers" 2>/dev/null || echo "0")
PRICE_COUNT=$(duckdb "$TEST_DB" "SELECT COUNT(*) FROM daily_prices" 2>/dev/null || echo "0")

echo -e "${BLUE}  📊 Results:${NC}"
echo "     Tickers in database: $TICKER_COUNT"
echo "     Price records: $PRICE_COUNT"

if [ "$TICKER_COUNT" -ge 2 ] && [ "$PRICE_COUNT" -gt 0 ]; then
    echo -e "${GREEN}  ✓ Data successfully persisted${NC}"
    
    # Show sample data
    echo ""
    echo -e "${BLUE}  📈 Sample ticker data:${NC}"
    duckdb "$TEST_DB" "SELECT ticker, name, asset_type, start_date, end_date FROM tickers LIMIT 5" 2>/dev/null || true
    
    echo ""
    echo -e "${BLUE}  💰 Sample price data:${NC}"
    duckdb "$TEST_DB" "SELECT ticker, date, close, volume FROM daily_prices ORDER BY date DESC LIMIT 5" 2>/dev/null || true
else
    echo -e "${RED}❌ Insufficient data in database${NC}"
    kill $CONSUMER_PID 2>/dev/null || true
    exit 1
fi

echo ""

# Step 7: Cleanup
echo -e "${YELLOW}🧹 Step 7: Cleanup...${NC}"

# Stop consumer
kill $CONSUMER_PID 2>/dev/null || true
echo -e "${GREEN}  ✓ Consumer stopped${NC}"

# Optional: Delete test topic (commented out to preserve for inspection)
# if command -v kafka-topics &> /dev/null; then
#     kafka-topics --delete --bootstrap-server "$KAFKA_BROKER" --topic "$TEST_TOPIC" 2>/dev/null || true
#     echo -e "${GREEN}  ✓ Test topic deleted${NC}"
# fi

echo ""
echo -e "${GREEN}✅ End-to-End Test PASSED!${NC}"
echo ""
echo -e "${BLUE}📋 Test Summary:${NC}"
echo "  • Broker: $KAFKA_BROKER"
echo "  • Topic: $TEST_TOPIC"
echo "  • Tickers processed: $TICKER_COUNT"
echo "  • Price records: $PRICE_COUNT"
echo "  • Database: $TEST_DB"
echo ""
echo -e "${BLUE}📄 Test artifacts:${NC}"
echo "  • Producer log: test_producer.log"
echo "  • Consumer log: test_consumer.log"
echo "  • Test database: $TEST_DB"
echo "  • Test portfolio: $TEST_PORTFOLIO"
echo ""
echo -e "${YELLOW}💡 To inspect the test database:${NC}"
echo "   duckdb $TEST_DB"
echo ""
echo -e "${YELLOW}💡 To clean up test artifacts:${NC}"
echo "   rm -f $TEST_DB ${TEST_DB}.wal $TEST_PORTFOLIO test_*.log"
