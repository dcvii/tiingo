# Tiingo Portfolio Tracker

A Go-based application that automatically fetches market data from the [Tiingo API](https://www.tiingo.com/) and stores it in a local DuckDB database. Track stocks and cryptocurrencies with automated daily syncing via Kafka messaging architecture.

## Overview

This application reads a portfolio of ticker symbols from a text file, fetches historical and current market data from Tiingo, and maintains a local DuckDB database for fast querying and analysis. The application uses a **Kafka-based architecture** for decoupling API fetching from database persistence, providing idempotency, auditability, and scalability.

**Architecture:**
- **Producer** (`tiingo`): Fetches data from Tiingo API → publishes to Kafka
- **Consumer** (`consumer`): Consumes from Kafka → persists to DuckDB
- **Broker**: Kafka running on `gold` server (192.168.1.178:9092)

## Features

- **Kafka Architecture**: Producer/Consumer pattern for decoupled processing
- **Multi-Asset Support**: Handles both stocks (equities) and cryptocurrencies
- **Incremental Sync**: Only fetches new data since last sync to optimize API usage
- **Rate Limiting**: Built-in request throttling (1 request per 8 seconds with burst of 5)
- **Idempotent**: Prevents duplicate records with `ON CONFLICT` handling
- **Message Replay**: Kafka enables reprocessing failed database writes
- **Audit Trail**: Raw API responses preserved in Kafka topics
- **Metadata Tracking**: Stores ticker information (name, exchange, asset type, date ranges)
- **Sync Logging**: Comprehensive audit trail of all sync operations
- **Error Handling**: Graceful error handling with detailed logging
- **DuckDB Storage**: Fast, embedded analytical database with zero setup
- **Scalability**: Multiple consumers can process the same data stream

## Technology Stack

- **Language**: Go 1.25.1
- **Database**: [DuckDB](https://duckdb.org/) via [go-duckdb](https://github.com/marcboeker/go-duckdb) v1.8.5
- **Messaging**: [Apache Kafka](https://kafka.apache.org/) via [segmentio/kafka-go](https://github.com/segmentio/kafka-go) v0.4.49
- **API**: [Tiingo](https://www.tiingo.com/) for market data
- **Rate Limiting**: [golang.org/x/time/rate](https://pkg.go.dev/golang.org/x/time/rate)
- **Environment**: direnv for environment variable management
- **Kafka Broker**: Running on `gold` server (192.168.1.178:9092)

## Project Structure

```
tiingo/
├── .envrc                      # Environment variables (API key)
├── .gitignore                  # Git ignore rules
├── go.mod                      # Go module definition
├── go.sum                      # Dependency checksums
├── portfolio.duckdb            # DuckDB database file
├── README.md                   # This file
├── KAFKA.md                    # Kafka architecture documentation
├── cmd/
│   ├── tiingo/
│   │   ├── main.go            # Producer - fetches API data to Kafka
│   │   └── main_test.go       # Integration tests
│   ├── consumer/
│   │   └── main.go            # Consumer - Kafka to DuckDB
│   └── quick-check/
│       └── main.go            # Connectivity testing utility
├── context/
│   ├── dev_plan.md            # Development plan and documentation
│   └── status_*.md            # Status reports
├── data/
│   ├── inbox/                 # Data import directory
│   ├── outbox/                # Data export directory
│   └── portfolio.txt          # Portfolio ticker list (input)
├── db/
│   ├── ddl/
│   │   └── 01_schema.sql     # Database schema definition
│   └── sql/                   # SQL query templates
├── internal/
│   ├── database/
│   │   └── manager.go        # DuckDB operations manager
│   ├── kafka/
│   │   ├── config.go          # Kafka configuration
│   │   ├── producer.go        # Kafka producer implementation
│   │   ├── consumer.go        # Kafka consumer implementation
│   │   └── config_test.go     # Kafka tests
│   ├── portfolio/
│   │   └── reader.go         # Portfolio file parser
│   └── tiingo/
│       └── client.go         # Tiingo API client
├── pkg/
│   └── models/
│       ├── types.go          # Shared data types
│       └── kafka.go          # Kafka message structures
└── scripts/
    ├── quick-check.sh         # System connectivity check
    ├── test-kafka-connectivity.sh # Kafka connectivity tests
    ├── test-kafka-e2e.sh      # End-to-end pipeline tests
    ├── start-kafka.sh         # Kafka service management
    └── stop-kafka.sh          # Kafka service management
```

## Installation

### Prerequisites

- Go 1.25.1 or later
- [direnv](https://direnv.net/) (recommended)
- Tiingo API key (free tier available at [tiingo.com](https://www.tiingo.com/))

### Setup

1. **Clone or navigate to the repository**:
   ```bash
   cd /Users/mdcb/devcode/PFIN/tiingo
   ```

2. **Install dependencies**:
   ```bash
   go mod download
   ```

3. **Configure environment variables**:
   
   Create or edit `.envrc`:
   ```bash
   export TIINGO_API_KEY='your_api_key_here'
   export PGGOLD_PWD='your_postgres_password'  # Optional, for future use
   ```

   Load environment:
   ```bash
   direnv allow
   ```

4. **Build the applications**:
   ```bash
   # Build producer
   go build -o tiingo cmd/tiingo/main.go
   
   # Build consumer
   go build -o consumer cmd/consumer/main.go
   
   # Build connectivity check utility
   go build -o quick-check cmd/quick-check/main.go
   ```

## Usage

### Kafka Architecture

**New in v2.0**: The application now uses a Kafka-based architecture with separate producer and consumer processes:

1. **Producer** (`tiingo`): Fetches data from Tiingo API → publishes to Kafka topics
2. **Consumer** (`consumer`): Consumes from Kafka topics → persists to DuckDB database
3. **Broker**: Kafka running on `gold` server (no local installation required)

**Benefits:**
- **Idempotency**: Replay messages if database writes fail
- **Decoupling**: API fetching and DB operations are independent
- **Audit Trail**: Raw API responses preserved in Kafka
- **Scalability**: Multiple consumers can process the same data

See [KAFKA.md](KAFKA.md) for detailed architecture documentation.

### Portfolio Configuration

Edit `data/portfolio.txt` to list your tickers (one per line):

```
# Stocks
ORCL
IBM
DAL
BRKB
GFS
MUB

# Cryptocurrencies
BTC
```

**Format Rules**:
- One ticker per line
- Lines starting with `#` are comments
- Blank lines are ignored
- Tickers are automatically converted to uppercase
- Comments can appear after tickers: `AAPL  # Apple Inc.`

### Running the Application

#### Option 1: Kafka Architecture (Recommended)

**Terminal 1** - Start the consumer (database writer):
```bash
./consumer -verbose
```

**Terminal 2** - Run the producer (API fetcher):
```bash
./tiingo -verbose
```

The producer fetches data from Tiingo API and publishes to Kafka. The consumer reads from Kafka and persists to DuckDB.

#### Option 2: Direct Mode (Legacy)

**Basic sync** (direct API to database, bypassing Kafka):
```bash
./tiingo -direct
```

**With custom options**:
```bash
# Producer with custom Kafka settings
./tiingo -portfolio data/portfolio.txt -brokers gold:9092 -topic tiingo.daily_prices -verbose

# Consumer with custom database
./consumer -db portfolio.duckdb -brokers gold:9092 -topic tiingo.daily_prices -verbose
```

### Command-Line Flags

#### Producer (`tiingo`) Flags:
| Flag | Default | Description |
|------|---------|-------------|
| `-portfolio` | `data/portfolio.txt` | Path to portfolio file |
| `-brokers` | `gold:9092` | Comma-separated Kafka broker addresses |
| `-topic` | `tiingo.daily_prices` | Kafka topic to publish to |
| `-direct` | `false` | Skip Kafka, write directly to database |
| `-db` | `portfolio.duckdb` | Database path (direct mode only) |
| `-verbose` | `false` | Enable detailed logging |

#### Consumer (`consumer`) Flags:
| Flag | Default | Description |
|------|---------|-------------|
| `-db` | `portfolio.duckdb` | Path to DuckDB database file |
| `-brokers` | `gold:9092` | Comma-separated Kafka broker addresses |
| `-topic` | `tiingo.daily_prices` | Kafka topic to consume from |
| `-group` | `tiingo-db-writer` | Kafka consumer group ID |
| `-verbose` | `false` | Enable detailed logging |

### Example Output

#### Producer Output:
```
2025/11/28 14:39:22 Starting Tiingo Kafka Producer
2025/11/28 14:39:22 ✅ Kafka producer initialized (brokers: [gold:9092])
2025/11/28 14:39:22 Found 7 ticker(s): [BTC BRKB DAL GFS MUB ORCL IBM]

[1/7] Processing BTC...
  Type: Cryptocurrency
  Ticker: BTCUSD
  Retrieved 5 price records
  📨 Published to Kafka: tiingo.daily_prices
  Message ID: abc123-def456-789

[2/7] Processing BRKB...
  Name: Berkshire Hathaway Inc. Class B
  Exchange: NYSE
  Type: Stock
  Retrieved 1 price records
  📨 Published to Kafka: tiingo.daily_prices
  Message ID: xyz789-abc123-456

✅ Producer complete! Published 7 messages to Kafka
```

#### Consumer Output:
```
2025/11/28 14:39:30 Starting Tiingo Kafka Consumer
2025/11/28 14:39:30 ✅ Database initialized
2025/11/28 14:39:30 ✅ Kafka consumer initialized
2025/11/28 14:39:30 🚀 Consumer started. Listening on topic: tiingo.daily_prices

📬 Processing message: BTC (5 records)
  💾 Saved 5 new price records
  ✅ Message committed

📬 Processing message: BRKB (1 records)
  💾 Saved 1 new price records
  ✅ Message committed

📊 Consumer Statistics:
  Messages: 7
  Bytes: 15,432
  Errors: 0
```

## Database Schema

The application creates three tables in DuckDB:

### `tickers` - Metadata
```sql
CREATE TABLE tickers (
    ticker VARCHAR PRIMARY KEY,
    name VARCHAR,
    exchange VARCHAR,
    asset_type VARCHAR,
    start_date DATE,
    end_date DATE,
    last_updated TIMESTAMP
);
```

### `daily_prices` - Price Data
```sql
CREATE TABLE daily_prices (
    ticker VARCHAR,
    date DATE,
    open DECIMAL(18,6),
    high DECIMAL(18,6),
    low DECIMAL(18,6),
    close DECIMAL(18,6),
    volume BIGINT,
    adj_close DECIMAL(18,6),
    adj_volume BIGINT,
    created_at TIMESTAMP,
    PRIMARY KEY (ticker, date)
);
```

### `sync_log` - Audit Trail
```sql
CREATE TABLE sync_log (
    log_id INTEGER PRIMARY KEY,
    ticker VARCHAR,
    sync_date TIMESTAMP,
    records_inserted INTEGER,
    status VARCHAR,
    error_message VARCHAR
);
```

## Querying the Database

Use the DuckDB CLI to query your data:

```bash
duckdb portfolio.duckdb
```

### Useful Queries

**Latest prices for all tickers**:
```sql
SELECT ticker, date, close, volume 
FROM daily_prices 
WHERE date = (SELECT MAX(date) FROM daily_prices WHERE ticker = daily_prices.ticker)
ORDER BY ticker;
```

**52-week high/low/average**:
```sql
SELECT 
    ticker,
    MIN(close) as low_52w,
    MAX(close) as high_52w,
    AVG(close) as avg_price,
    COUNT(*) as trading_days
FROM daily_prices 
WHERE date >= CURRENT_DATE - INTERVAL 1 YEAR
GROUP BY ticker
ORDER BY ticker;
```

**Recent sync history**:
```sql
SELECT 
    ticker,
    sync_date,
    records_inserted,
    status,
    error_message
FROM sync_log 
ORDER BY sync_date DESC 
LIMIT 20;
```

**Price trend (30-day moving average)**:
```sql
SELECT 
    ticker,
    date,
    close,
    AVG(close) OVER (
        PARTITION BY ticker 
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as ma_30
FROM daily_prices
WHERE date >= CURRENT_DATE - INTERVAL 90 DAY
ORDER BY ticker, date DESC;
```

**Export to CSV**:
```sql
COPY (
    SELECT * FROM daily_prices 
    WHERE ticker = 'ORCL' 
    ORDER BY date DESC
) TO 'data/outbox/orcl_prices.csv' (HEADER, DELIMITER ',');
```

## Automation

### Daily Sync with Kafka Pipeline

#### Option 1: Systemd Services (Linux)

Create systemd services for producer and consumer:

**Consumer Service** (`/etc/systemd/system/tiingo-consumer.service`):
```ini
[Unit]
Description=Tiingo Kafka Consumer
After=network.target

[Service]
Type=simple
User=mdcb
WorkingDirectory=/Users/mdcb/devcode/PFIN/tiingo
Environment=TIINGO_API_KEY=your_api_key_here
ExecStart=/Users/mdcb/devcode/PFIN/tiingo/consumer -verbose
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Producer Timer** (`/etc/systemd/system/tiingo-producer.timer`):
```ini
[Unit]
Description=Run Tiingo Producer Daily
Requires=tiingo-producer.service

[Timer]
# Run at 4:30 PM PT (after market close), Monday-Friday
OnCalendar=Mon..Fri *-*-* 16:30:00
Persistent=true

[Install]
WantedBy=timers.target
```

**Producer Service** (`/etc/systemd/system/tiingo-producer.service`):
```ini
[Unit]
Description=Tiingo Kafka Producer

[Service]
Type=oneshot
User=mdcb
WorkingDirectory=/Users/mdcb/devcode/PFIN/tiingo
Environment=TIINGO_API_KEY=your_api_key_here
ExecStart=/Users/mdcb/devcode/PFIN/tiingo/tiingo -verbose
```

Enable services:
```bash
sudo systemctl enable tiingo-consumer.service
sudo systemctl enable tiingo-producer.timer
sudo systemctl start tiingo-consumer.service
sudo systemctl start tiingo-producer.timer
```

#### Option 2: Cron (Legacy Direct Mode)

Add to crontab (`crontab -e`):
```bash
# Run at 4:30 PM PT (after market close)
30 16 * * 1-5 cd /Users/mdcb/devcode/PFIN/tiingo && ./tiingo -direct >> logs/sync.log 2>&1
```

#### Option 3: macOS launchd

**Consumer Daemon** (`~/Library/LaunchAgents/com.mdcb.tiingo.consumer.plist`):
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.mdcb.tiingo.consumer</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Users/mdcb/devcode/PFIN/tiingo/consumer</string>
        <string>-verbose</string>
    </array>
    <key>KeepAlive</key>
    <true/>
    <key>RunAtLoad</key>
    <true/>
    <key>WorkingDirectory</key>
    <string>/Users/mdcb/devcode/PFIN/tiingo</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>TIINGO_API_KEY</key>
        <string>your_api_key_here</string>
    </dict>
    <key>StandardOutPath</key>
    <string>/Users/mdcb/devcode/PFIN/tiingo/logs/consumer.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/mdcb/devcode/PFIN/tiingo/logs/consumer-error.log</string>
</dict>
</plist>
```

**Producer Timer** (`~/Library/LaunchAgents/com.mdcb.tiingo.producer.plist`):
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.mdcb.tiingo.producer</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Users/mdcb/devcode/PFIN/tiingo/tiingo</string>
        <string>-verbose</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>16</integer>
        <key>Minute</key>
        <integer>30</integer>
    </dict>
    <key>WorkingDirectory</key>
    <string>/Users/mdcb/devcode/PFIN/tiingo</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>TIINGO_API_KEY</key>
        <string>your_api_key_here</string>
    </dict>
    <key>StandardOutPath</key>
    <string>/Users/mdcb/devcode/PFIN/tiingo/logs/producer.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/mdcb/devcode/PFIN/tiingo/logs/producer-error.log</string>
</dict>
</plist>
```

Load the agents:
```bash
launchctl load ~/Library/LaunchAgents/com.mdcb.tiingo.consumer.plist
launchctl load ~/Library/LaunchAgents/com.mdcb.tiingo.producer.plist
```

## API Rate Limits

Tiingo free tier allows **500 requests per hour**. This application implements:
- Rate limiting: 1 request per 8 seconds (450 requests/hour max)
- Burst allowance: Up to 5 rapid requests
- Incremental sync: Only fetches data since last sync date

**Typical API usage per sync**:
- New ticker: 2 requests (metadata + prices)
- Existing ticker: 1 request (prices only)

## Asset Type Support

### Stocks
- Fetches metadata (name, exchange, asset type, date ranges)
- Retrieves daily OHLCV data (open, high, low, close, volume)
- Includes adjusted prices for splits/dividends
- Default: last 30 days on first sync, incremental thereafter

### Cryptocurrencies
- Auto-detected for common tickers (BTC, ETH)
- Converts ticker format (e.g., `BTC` → `BTCUSD` for API)
- Retrieves daily price data
- Default: last 5 days per sync

## Error Handling

The application handles various error conditions gracefully:

- **Invalid ticker**: Logs error, continues with remaining tickers
- **API errors**: Includes status code and error message
- **Rate limit exceeded**: Automatic throttling via limiter
- **Network failures**: Request context timeouts (5 minutes max)
- **Database errors**: Transaction rollback on failures
- **Missing API key**: Fails fast with clear error message

All errors are logged to the sync_log table for auditing.

## Testing

### Connectivity Testing

**Quick system check**:
```bash
./scripts/quick-check.sh
```

**Kafka connectivity**:
```bash
./scripts/test-kafka-connectivity.sh
```

**End-to-end pipeline test**:
```bash
./scripts/test-kafka-e2e.sh
```

### Unit Tests
```bash
# Run all tests
go test ./... -v

# Run tests with coverage
go test ./... -cover

# Run with race detector
go test ./... -race

# Test specific package
go test ./internal/kafka -v
go test ./cmd/tiingo -v
```

### Test Coverage
The test suite includes:
- Kafka configuration and connectivity
- Producer message serialization
- Consumer message processing
- Ticker format validation (stocks vs crypto)
- Crypto ticker conversion logic (BTC → BTCUSD)
- Sync event logging with original ticker preservation
- Stock ticker pass-through (no conversion)
- Database idempotency (duplicate prevention)

## Development

### Build for Development
```bash
# Build all components
make build  # or manually:
go build -o tiingo cmd/tiingo/main.go
go build -o consumer cmd/consumer/main.go
go build -o quick-check cmd/quick-check/main.go
```

### Build with Optimizations (Production)
```bash
# Producer
go build -ldflags "-s -w" -o tiingo cmd/tiingo/main.go
# Consumer
go build -ldflags "-s -w" -o consumer cmd/consumer/main.go
```

### Run Without Building
```bash
# Producer
go run cmd/tiingo/main.go -verbose
# Consumer
go run cmd/consumer/main.go -verbose
# Quick check
go run cmd/quick-check/main.go
```

### Format Code
```bash
go fmt ./...
```

### Lint
```bash
golangci-lint run
```

## Backup and Maintenance

### Backup Database
```bash
# Simple copy
cp portfolio.duckdb backups/portfolio_$(date +%Y%m%d).duckdb

# Export to Parquet (compressed, portable)
duckdb portfolio.duckdb -c "COPY daily_prices TO 'backups/prices.parquet' (FORMAT PARQUET)"
```

### Database Maintenance
```sql
-- Check database size
SELECT pg_size_pretty(pg_database_size(current_database()));

-- Vacuum (reclaim space)
VACUUM;

-- Analyze (update statistics)
ANALYZE;
```

## Troubleshooting

### Kafka Connectivity Issues

**Cannot connect to Kafka broker**:
```
Error: failed to dial: dial tcp gold:9092: connection refused
```
**Solutions**:
1. Verify gold server is reachable: `ping gold`
2. Check Kafka is running: `telnet gold 9092`
3. Run connectivity test: `./scripts/test-kafka-connectivity.sh`

**Consumer group errors**:
```
Error: consumer group coordination error
```
**Solution**: Reset consumer group: `./scripts/stop-kafka.sh && ./scripts/start-kafka.sh`

### API Key Not Set
```
Error: TIINGO_API_KEY environment variable not set
```
**Solution**: Ensure `.envrc` is configured and `direnv allow` has been run.

### Database Issues

**Database Locked**:
```
Error: database is locked
```
**Solution**: Ensure no other process is accessing the database. Close any DuckDB CLI sessions.

**Consumer cannot write to database**:
- Check database file permissions
- Ensure consumer has exclusive access
- Stop any other DuckDB processes

### API Issues

**Invalid Ticker**:
```
Error: API error (status 404): ticker not found
```
**Solution**: Verify ticker symbol is correct. Check Tiingo documentation for supported tickers.

**Rate Limit Exceeded**:
The built-in rate limiter should prevent this, but if you see rate limit errors:
- Reduce the burst size in `client.go`
- Increase the delay between requests
- Split your portfolio into smaller batches

### Kafka Message Issues

**Messages not being consumed**:
1. Check consumer group status: `kafka-consumer-groups --bootstrap-server gold:9092 --describe --group tiingo-db-writer`
2. Verify topic exists: `kafka-topics --list --bootstrap-server gold:9092`
3. Check message format compatibility

**Messages stuck in Kafka**:
- Consumer might have crashed during processing
- Reset consumer group offset or replay messages
- Check consumer logs for errors

## Future Enhancements

Potential features for future development:
- [ ] Multiple Kafka topic support (stocks vs crypto)
- [ ] Schema registry integration for message versioning
- [ ] Consumer horizontal scaling (multiple instances)
- [ ] Dead letter queue for failed messages
- [ ] Kafka Connect integration
- [ ] Support for custom date ranges via CLI flags
- [ ] Export reports (PDF, Excel)
- [ ] Web dashboard with charts
- [ ] Real-time quote fetching (IEX endpoint)
- [ ] Dividend and split tracking
- [ ] Portfolio valuation with share quantities
- [ ] Alert system for price thresholds
- [ ] Support for international exchanges
- [ ] PostgreSQL migration support (via `pggold`)
- [ ] Kubernetes deployment manifests

## License

This is a personal project. See repository for license information.

## References

- [Tiingo API Documentation](https://api.tiingo.com/documentation/general/overview)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [go-duckdb Driver](https://github.com/marcboeker/go-duckdb)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [segmentio/kafka-go](https://github.com/segmentio/kafka-go)
- [KAFKA.md](KAFKA.md) - Detailed architecture documentation

## Contributing

This is a personal portfolio tracker. For questions or suggestions, please open an issue.

---

**Last Updated**: 2025-12-01  
**Version**: 2.0.0 (Kafka Architecture)  
**Author**: mdcb
