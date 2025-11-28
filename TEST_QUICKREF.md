# Test Quick Reference

## 🚀 Quick Start (First Time)

```bash
# 1. Check prerequisites
./scripts/quick-check.sh

# 2. Build binaries (if needed)
go build -o tiingo ./cmd/tiingo
go build -o consumer ./cmd/consumer

# 3. Run E2E test
./scripts/test-kafka-e2e.sh
```

## ⚡ Test Commands

### Quick Check (5 seconds)
```bash
./scripts/quick-check.sh
```

### Unit Tests
```bash
go test ./internal/kafka/... -v
```

### Full E2E Test
```bash
./scripts/test-kafka-e2e.sh
```

### Connectivity Test
```bash
./scripts/test-kafka-connectivity.sh
```

## 🎯 Manual Testing

### Two-Terminal Test

**Terminal 1:**
```bash
./consumer -brokers gold:9092 -verbose
```

**Terminal 2:**
```bash
./tiingo -brokers gold:9092 -verbose
```

### Verify Data
```bash
duckdb test_portfolio.duckdb "SELECT COUNT(*) FROM tickers"
duckdb test_portfolio.duckdb "SELECT * FROM daily_prices LIMIT 5"
```

## 🧹 Cleanup

```bash
# Remove test artifacts
rm -f test_portfolio.duckdb* data/test_portfolio.txt test_*.log
```

## 📊 Kafka Commands

### List Topics
```bash
kafka-topics --list --bootstrap-server gold:9092
```

### Check Consumer Group
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

## 🔍 Troubleshooting

### Check Connectivity
```bash
ping gold
nc -zv gold 9092
```

### Check Environment
```bash
echo $TIINGO_API_KEY
direnv allow
```

### View Logs
```bash
tail -f test_consumer.log
tail -f test_producer.log
```

### Verify Database
```bash
ls -lh *.duckdb
duckdb portfolio.duckdb "SELECT COUNT(*) FROM tickers"
```

## 📖 Documentation

- **Full Testing Guide:** `TESTING.md`
- **Test Setup Summary:** `context/test_setup_summary.md`
- **Kafka Architecture:** `KAFKA.md`
- **Main README:** `README.md`

## ⚙️ Configuration

| Setting | Value |
|---------|-------|
| Broker | `gold:9092` |
| IP | `192.168.1.178` |
| Test Topic | `tiingo.test.daily_prices` |
| Prod Topic | `tiingo.daily_prices` |
| Consumer Group | `tiingo-db-writer` |

## ✅ Test Checklist

- [ ] `./scripts/quick-check.sh` passes
- [ ] Unit tests pass
- [ ] E2E test passes
- [ ] Can produce to Kafka
- [ ] Can consume from Kafka
- [ ] Data persists to DuckDB
- [ ] No duplicate records (idempotency)
