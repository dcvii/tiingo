# Kafka Test Setup Summary

**Date:** 2025-11-24
**Environment:** macOS development machine → Gold Kafka broker (192.168.1.178:9092)

## Overview

End-to-end test infrastructure has been set up for the Tiingo Kafka integration with the Gold broker. The test suite validates the complete data flow: Tiingo API → Producer → Kafka (Gold) → Consumer → DuckDB.

## Test Components Created

### 1. Test Scripts

#### `scripts/quick-check.sh` ⚡
**Purpose:** Fast prerequisite check (< 5 seconds)

**Checks:**
- Gold server connectivity (ping)
- Kafka port accessibility (9092)
- TIINGO_API_KEY environment variable
- Binary availability (tiingo, consumer)

**Usage:**
```bash
./scripts/quick-check.sh
```

#### `scripts/test-kafka-connectivity.sh` 🔌
**Purpose:** Comprehensive connectivity diagnostics

**Tests:**
- DNS resolution
- Network latency
- Port connectivity
- Kafka administrative access (topics, consumer groups)
- Tiingo-specific topics and groups

**Usage:**
```bash
./scripts/test-kafka-connectivity.sh
```

**Note:** May timeout on Kafka admin commands if broker doesn't allow external admin operations. This is expected and doesn't affect producer/consumer functionality.

#### `scripts/test-kafka-e2e.sh` 🧪
**Purpose:** Full end-to-end integration test

**Test Flow:**
1. ✅ Verify prerequisites (connectivity, API key, binaries)
2. 📁 Create test portfolio (AAPL, MSFT)
3. 🗑️ Clean old test databases
4. 📡 Verify/create Kafka test topic (`tiingo.test.daily_prices`)
5. 🎧 Start consumer in background
6. 📤 Run producer to fetch and publish data
7. ⏳ Wait for consumer to process
8. 🔍 Verify data in DuckDB
9. 🧹 Cleanup test processes

**Artifacts Created:**
- `test_portfolio.duckdb` - Test database
- `data/test_portfolio.txt` - Test ticker list
- `test_producer.log` - Producer output
- `test_consumer.log` - Consumer output

**Usage:**
```bash
./scripts/test-kafka-e2e.sh
```

**Expected Result:**
```
✅ End-to-End Test PASSED!

📋 Test Summary:
  • Broker: gold:9092
  • Topic: tiingo.test.daily_prices
  • Tickers processed: 2
  • Price records: 60+
```

### 2. Unit Tests

#### `internal/kafka/config_test.go`
**Coverage:**
- Producer configuration creation
- Consumer configuration creation
- Configuration validation
- Default value handling
- Error cases

**Usage:**
```bash
go test ./internal/kafka/... -v
```

**Status:** ✅ All tests passing

### 3. Documentation

#### `TESTING.md`
Comprehensive testing guide covering:
- Quick start procedures
- Manual testing scenarios
- Kafka admin operations
- Database verification queries
- Troubleshooting steps
- Test data cleanup
- Performance testing
- Best practices

## Configuration

### Kafka Broker
- **Host:** `gold` (192.168.1.178)
- **Port:** 9092
- **Protocol:** Plain (no auth/SSL for testing)

### Topics
- **Production:** `tiingo.daily_prices`, `tiingo.crypto_prices`
- **Testing:** `tiingo.test.daily_prices`

### Consumer Groups
- **Production:** `tiingo-db-writer`
- **Testing:** `tiingo-test-consumer`

### Environment Variables
Required in `.envrc`:
```bash
export TIINGO_API_KEY='your_api_key'
export PGGOLD_PWD='password'  # Optional, for future use
```

## Test Status

### ✅ Completed
- [x] Unit tests for Kafka configuration
- [x] Quick connectivity check script
- [x] Comprehensive connectivity test script
- [x] End-to-end integration test script
- [x] Test documentation (TESTING.md)
- [x] .gitignore updated for test artifacts
- [x] Gold broker connectivity verified
- [x] Port 9092 accessibility confirmed

### 📋 Ready to Run
- [ ] End-to-end test (requires building binaries first)
- [ ] Manual two-terminal test
- [ ] Performance/throughput testing

## Running the Tests

### Quick Validation (Recommended First)
```bash
# 1. Quick check
./scripts/quick-check.sh

# 2. Build if needed
go build -o tiingo ./cmd/tiingo
go build -o consumer ./cmd/consumer

# 3. Run unit tests
go test ./internal/kafka/... -v

# 4. Run E2E test
./scripts/test-kafka-e2e.sh
```

### Full Test Suite
```bash
# Unit tests
go test ./... -v

# Connectivity check
./scripts/test-kafka-connectivity.sh

# End-to-end test
./scripts/test-kafka-e2e.sh
```

## Manual Testing

### Two-Terminal Manual Test

**Terminal 1 - Consumer:**
```bash
./consumer -brokers gold:9092 -verbose
```

**Terminal 2 - Producer:**
```bash
./tiingo -brokers gold:9092 -verbose
```

## Cleanup

### After Testing
```bash
# Remove test artifacts
rm -f test_portfolio.duckdb* data/test_portfolio.txt test_*.log
```

### Reset Consumer Group (if needed)
```bash
# Stop consumer first!
kafka-consumer-groups \
  --bootstrap-server gold:9092 \
  --delete \
  --group tiingo-test-consumer
```

## Known Limitations

1. **Kafka Admin Commands:** May timeout if Gold broker restricts external admin access. This doesn't affect producer/consumer functionality.

2. **Test Topics:** Auto-created on first use if Kafka allows. Manual creation may be required on restricted brokers.

3. **Network Latency:** Tests assume <100ms latency to Gold. Adjust timeouts if needed.

## Next Steps

1. **Run E2E Test:** Execute `./scripts/test-kafka-e2e.sh` to validate complete flow
2. **Monitor Production:** Set up monitoring for `tiingo-db-writer` consumer group
3. **Performance Testing:** Measure throughput with larger portfolios
4. **Error Recovery:** Test consumer restart scenarios and replay

## Architecture

```
┌────────────────┐    Kafka Message     ┌─────────────────┐
│ Tiingo API     │                      │  Gold Server    │
└────────┬───────┘                      │  192.168.1.178  │
         │                              │                 │
         │ Fetch                        │  ┌───────────┐  │
         ▼                              │  │  Kafka    │  │
┌────────────────┐    Publish          │  │  :9092    │  │
│   Producer     │────────────────────►│  └───────────┘  │
│   (tiingo)     │                      │                 │
└────────────────┘                      │  Topic:         │
                                        │  tiingo.*       │
                                        └────────┬────────┘
                                                 │
                                                 │ Subscribe
                                                 ▼
                                        ┌─────────────────┐
                                        │   Consumer      │
                                        │   (consumer)    │
                                        └────────┬────────┘
                                                 │
                                                 │ Persist
                                                 ▼
                                        ┌─────────────────┐
                                        │   DuckDB        │
                                        │   portfolio.db  │
                                        └─────────────────┘
```

## Support

For issues or questions:
1. Check `TESTING.md` for detailed troubleshooting
2. Review test logs in `test_*.log` files
3. Verify Gold broker status
4. Check DuckDB database with `duckdb test_portfolio.duckdb`

## References

- Main Documentation: `README.md`
- Kafka Architecture: `KAFKA.md`
- Testing Guide: `TESTING.md`
- Test Scripts: `scripts/test-*.sh`
