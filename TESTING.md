# Testing Guide for Tiingo Kafka Integration

This guide covers all testing scenarios for the Tiingo application's Kafka integration with the Gold broker.

## Prerequisites

- Gold server accessible at `192.168.1.178` (hostname: `gold`)
- Kafka broker running on `gold:9092`
- `TIINGO_API_KEY` environment variable set (via `.envrc`)
- Go 1.25.1 or later
- DuckDB CLI (for database verification)

## Quick Start

### 1. Connectivity Test
Verifies network connectivity and Kafka broker accessibility:

```bash
./scripts/test-kafka-connectivity.sh
```

**What it tests:**
- DNS resolution for `gold`
- Network reachability (ping)
- Kafka port accessibility (9092)
- Kafka administrative access (if kafka-topics available)
- Existing topics and consumer groups

**Expected output:**
```
🔌 Kafka Connectivity Test
==================================

Test 1: DNS Resolution
  ✓ 'gold' resolves to 192.168.1.178

Test 2: Network Reachability
  ✓ 'gold' is reachable (latency: 2.32ms)

Test 3: Kafka Port (9092)
  ✓ Port 9092 is open and accepting connections
...
✅ Connectivity Test Complete!
```

### 2. End-to-End Test
Full integration test of producer → Kafka → consumer → database:

```bash
./scripts/test-kafka-e2e.sh
```

**What it tests:**
- Producer can fetch data from Tiingo API
- Producer can publish to Kafka on Gold
- Consumer can subscribe to Kafka topic
- Consumer can persist to DuckDB
- Data integrity end-to-end

**Test flow:**
1. Creates test portfolio with 2 tickers (AAPL, MSFT)
2. Starts consumer in background
3. Runs producer to fetch and publish data
4. Waits for consumer to process messages
5. Verifies data in DuckDB
6. Cleans up test processes

**Expected output:**
```
🧪 Tiingo Kafka End-to-End Test
================================================

📋 Step 0: Checking prerequisites...
  ✓ Gold server reachable
  ✓ Kafka broker accessible on gold:9092
  ✓ TIINGO_API_KEY configured
  ✓ Binaries ready

📁 Step 1: Setting up test environment...
  ✓ Created test portfolio: data/test_portfolio.txt
...

✅ End-to-End Test PASSED!

📋 Test Summary:
  • Broker: gold:9092
  • Topic: tiingo.test.daily_prices
  • Tickers processed: 2
  • Price records: 60
  • Database: test_portfolio.duckdb
```

## Unit Tests

### Kafka Configuration Tests
Tests configuration and validation logic:

```bash
go test ./internal/kafka/... -v
```

**What it tests:**
- Producer configuration creation
- Consumer configuration creation
- Configuration validation
- Default values

**Expected output:**
```
=== RUN   TestNewProducerConfig
=== RUN   TestNewProducerConfig/with_brokers
=== RUN   TestNewProducerConfig/empty_brokers_uses_default
=== RUN   TestNewProducerConfig/multiple_brokers
--- PASS: TestNewProducerConfig (0.00s)
...
PASS
ok      github.com/mdcb/tiingo-tracker/internal/kafka   0.123s
```

## Manual Testing

### Test 1: Producer Only
Test producer in isolation:

```bash
# Build producer
go build -o tiingo ./cmd/tiingo

# Run with test portfolio
./tiingo \
  -portfolio data/test_portfolio.txt \
  -brokers gold:9092 \
  -topic tiingo.test.daily_prices \
  -verbose
```

**Verify:**
- Producer fetches data from Tiingo API
- Messages published to Kafka (check logs)
- No errors in output

### Test 2: Consumer Only
Test consumer with existing messages:

```bash
# Build consumer
go build -o consumer ./cmd/consumer

# Run consumer
./consumer \
  -db test_manual.duckdb \
  -brokers gold:9092 \
  -topic tiingo.test.daily_prices \
  -group tiingo-manual-test \
  -verbose
```

**Verify:**
- Consumer connects to Kafka
- Messages consumed and logged
- Data written to DuckDB
- No errors in output

### Test 3: Two-Terminal Test
Test producer and consumer simultaneously:

**Terminal 1 (Consumer):**
```bash
./consumer -brokers gold:9092 -verbose
```

**Terminal 2 (Producer):**
```bash
./tiingo -brokers gold:9092 -verbose
```

**Verify:**
- Consumer receives messages in real-time
- Both processes complete without errors
- Data appears in `portfolio.duckdb`

## Kafka Admin Operations

### List Topics
```bash
kafka-topics --list --bootstrap-server gold:9092
```

### Describe Topic
```bash
kafka-topics --describe \
  --bootstrap-server gold:9092 \
  --topic tiingo.daily_prices
```

### List Consumer Groups
```bash
kafka-consumer-groups --list --bootstrap-server gold:9092
```

### Check Consumer Group Status
```bash
kafka-consumer-groups \
  --bootstrap-server gold:9092 \
  --describe \
  --group tiingo-db-writer
```

### Peek at Messages
```bash
kafka-console-consumer \
  --bootstrap-server gold:9092 \
  --topic tiingo.daily_prices \
  --from-beginning \
  --max-messages 1
```

### Reset Consumer Group (for replaying messages)
```bash
# Stop consumer first!

# Delete consumer group
kafka-consumer-groups \
  --bootstrap-server gold:9092 \
  --delete \
  --group tiingo-db-writer

# Restart consumer to replay from beginning
```

## Database Verification

### Inspect Test Database
```bash
duckdb test_portfolio.duckdb
```

```sql
-- Check ticker metadata
SELECT * FROM tickers;

-- Check price records count
SELECT ticker, COUNT(*) as records 
FROM daily_prices 
GROUP BY ticker;

-- Check latest prices
SELECT ticker, date, close, volume 
FROM daily_prices 
ORDER BY date DESC 
LIMIT 10;

-- Verify date ranges
SELECT 
  ticker,
  MIN(date) as first_date,
  MAX(date) as last_date,
  COUNT(*) as records
FROM daily_prices
GROUP BY ticker;
```

## Troubleshooting

### Consumer Not Receiving Messages

1. **Check consumer is running:**
   ```bash
   ps aux | grep consumer
   ```

2. **Check consumer group lag:**
   ```bash
   kafka-consumer-groups \
     --bootstrap-server gold:9092 \
     --describe \
     --group tiingo-db-writer
   ```

3. **Check topic exists:**
   ```bash
   kafka-topics --list --bootstrap-server gold:9092 | grep tiingo
   ```

### Producer Not Publishing

1. **Check Kafka connectivity:**
   ```bash
   nc -zv gold 9092
   ```

2. **Check TIINGO_API_KEY:**
   ```bash
   echo $TIINGO_API_KEY
   ```

3. **Run with verbose logging:**
   ```bash
   ./tiingo -verbose 2>&1 | tee producer.log
   ```

### Database Issues

1. **Check database file exists:**
   ```bash
   ls -lh *.duckdb
   ```

2. **Check database is writable:**
   ```bash
   duckdb portfolio.duckdb "SELECT COUNT(*) FROM tickers"
   ```

3. **Check for WAL file conflicts:**
   ```bash
   ls -lh *.duckdb.wal
   # If stuck, stop consumer and remove .wal file
   ```

## Test Data Cleanup

### Clean Test Artifacts
```bash
# Remove test database
rm -f test_portfolio.duckdb test_portfolio.duckdb.wal

# Remove test portfolio
rm -f data/test_portfolio.txt

# Remove test logs
rm -f test_*.log

# All at once
rm -f test_portfolio.duckdb* data/test_portfolio.txt test_*.log
```

### Clean Production Database (CAUTION!)
```bash
# Backup first!
cp portfolio.duckdb portfolio.duckdb.backup

# Remove database to start fresh
rm -f portfolio.duckdb portfolio.duckdb.wal
```

### Clean Kafka Test Topics (Optional)
```bash
# Delete test topic
kafka-topics \
  --delete \
  --bootstrap-server gold:9092 \
  --topic tiingo.test.daily_prices

# Delete test consumer groups
kafka-consumer-groups \
  --bootstrap-server gold:9092 \
  --delete \
  --group tiingo-test-consumer
```

## Continuous Integration

For automated testing in CI/CD:

```bash
#!/bin/bash
# ci-test.sh

set -e

echo "Running unit tests..."
go test ./... -v

echo "Running connectivity test..."
./scripts/test-kafka-connectivity.sh

echo "Running end-to-end test..."
./scripts/test-kafka-e2e.sh

echo "All tests passed!"
```

## Performance Testing

### Measure Throughput
```bash
# Start consumer with verbose logging
./consumer -verbose 2>&1 | tee perf_consumer.log &

# Run producer with timing
time ./tiingo -verbose

# Check consumer stats
tail -20 perf_consumer.log
```

### Monitor Kafka Lag
```bash
# Monitor in real-time
watch -n 5 'kafka-consumer-groups \
  --bootstrap-server gold:9092 \
  --describe \
  --group tiingo-db-writer'
```

## Best Practices

1. **Always run connectivity test first** before debugging issues
2. **Use verbose logging** when troubleshooting
3. **Check consumer logs** if data not appearing in database
4. **Verify topic exists** before running consumer
5. **Use test topics** for development/testing
6. **Clean up test artifacts** after testing
7. **Monitor consumer group lag** for production

## Test Checklist

Before deploying to production:

- [ ] Connectivity test passes
- [ ] Unit tests pass
- [ ] End-to-end test passes
- [ ] Can publish to production topics
- [ ] Consumer can read from production topics
- [ ] Data persists correctly to DuckDB
- [ ] Consumer handles errors gracefully
- [ ] Producer handles rate limits
- [ ] Consumer group offsets committed properly
- [ ] Idempotency works (no duplicate records)
