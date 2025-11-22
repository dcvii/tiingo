# Kafka Architecture

## Overview

The tiingo application uses Kafka as an intermediary layer between API fetching and database persistence. This provides:

- **Idempotency**: Replay messages if database writes fail
- **Decoupling**: API fetching and DB writes are independent processes
- **Audit trail**: Raw API responses preserved in Kafka
- **Scalability**: Multiple consumers can process the same data stream

## Architecture

```
┌──────────────┐    ┌───────────────┐    ┌────────────────┐
│   Tiingo     │───▶│  Kafka Broker │───▶│   Consumer     │
│   Producer   │    │   (gold:9092) │    │  (DB Writer)   │
└──────────────┘    └───────────────┘    └────────────────┘
      │                                           │
      │                                           ▼
      ▼                                   ┌────────────────┐
 Tiingo API                               │    DuckDB      │
                                          └────────────────┘
```

## Infrastructure

**Kafka Broker**: `gold` (192.168.1.178:9092)

The Kafka broker is already installed and running on the `gold` server. No local Kafka installation is required.

## Components

### 1. Producer (`cmd/tiingo/main.go`)
Fetches data from Tiingo API and publishes to Kafka topics on the gold server

**Usage:**
```bash
./tiingo \
  -portfolio data/portfolio.txt \
  -brokers gold:9092 \
  -topic tiingo.daily_prices \
  -verbose
```

**Flags:**
- `-portfolio`: Path to portfolio file (default: `data/portfolio.txt`)
- `-brokers`: Kafka broker address (default: `gold:9092`)
- `-topic`: Kafka topic to publish to (default: `tiingo.daily_prices`)
- `-verbose`: Enable verbose logging

### 2. Consumer (`cmd/consumer/main.go`)
Consumes from Kafka topics and persists to DuckDB

**Usage:**
```bash
./consumer \
  -db portfolio.duckdb \
  -brokers gold:9092 \
  -topic tiingo.daily_prices \
  -group tiingo-db-writer \
  -verbose
```

**Flags:**
- `-db`: Path to DuckDB database (default: `portfolio.duckdb`)
- `-brokers`: Kafka broker address (default: `gold:9092`)
- `-topic`: Kafka topic to consume from (default: `tiingo.daily_prices`)
- `-group`: Consumer group ID (default: `tiingo-db-writer`)
- `-verbose`: Enable verbose logging

## Kafka Topics

- `tiingo.daily_prices` - Stock daily price data
- `tiingo.crypto_prices` - Cryptocurrency price data

### Message Format

Daily prices message:
```json
{
  "message_id": "uuid",
  "ticker": "AAPL",
  "asset_type": "Stock",
  "fetch_time": "2024-11-22T23:00:00Z",
  "start_date": "2024-10-22T00:00:00Z",
  "end_date": "2024-11-22T00:00:00Z",
  "prices": [...],
  "record_count": 30
}
```

## Setup

### 1. Verify Broker Connectivity

Check that the gold server is reachable:
```bash
ping -c 3 gold
```

Verify Kafka broker is running:
```bash
telnet gold 9092
```

### 2. Create Topics (on gold server)

Connect to the gold server and create topics:

```bash
# Create daily prices topic
kafka-topics --create \
  --bootstrap-server gold:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic tiingo.daily_prices

# Create crypto prices topic
kafka-topics --create \
  --bootstrap-server gold:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic tiingo.crypto_prices
```

### 3. Verify Setup

List topics:
```bash
kafka-topics --list --bootstrap-server gold:9092
```

## Running the Pipeline

### Terminal 1: Start Consumer
```bash
go run ./cmd/consumer -verbose
```

### Terminal 2: Run Producer
```bash
go run ./cmd/tiingo -verbose
```

The producer will fetch data from Tiingo API and publish to Kafka. The consumer will read from Kafka and persist to DuckDB.

## Monitoring

### View Consumer Group Status
```bash
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group tiingo-db-writer
```

### View Topic Messages (for debugging)
```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic tiingo.daily_prices \
  --from-beginning \
  --max-messages 1
```

## Idempotency

The consumer uses DuckDB's `ON CONFLICT DO NOTHING` clause, making writes idempotent:
- Same ticker+date will not create duplicate records
- Safe to replay Kafka messages
- Consumer only commits offset after successful DB write

## Replay from Beginning

To reprocess all messages from the beginning:

1. Stop the consumer
2. Delete the consumer group:
   ```bash
   kafka-consumer-groups --bootstrap-server localhost:9092 \
     --delete --group tiingo-db-writer
   ```
3. Restart the consumer

## Environment Variables

Both producer and consumer require:
```bash
export TIINGO_API_KEY="your_api_key_here"
```

Add to `.envrc`:
```bash
export TIINGO_API_KEY="your_key"
```

## Development

### Build Binaries
```bash
go build -o bin/producer ./cmd/tiingo
go build -o bin/consumer ./cmd/consumer
```

### Run Tests
```bash
go test ./internal/kafka/...
```
