# Tiingo Portfolio Tracker

A Go-based application that automatically fetches market data from the [Tiingo API](https://www.tiingo.com/) and stores it in a local DuckDB database. Track stocks and cryptocurrencies with automated daily syncing.

## Overview

This application reads a portfolio of ticker symbols from a text file, fetches historical and current market data from Tiingo, and maintains a local DuckDB database for fast querying and analysis. It includes intelligent incremental syncing to minimize API calls and features built-in rate limiting to comply with Tiingo's free tier limits.

## Features

- **Multi-Asset Support**: Handles both stocks (equities) and cryptocurrencies
- **Incremental Sync**: Only fetches new data since last sync to optimize API usage
- **Rate Limiting**: Built-in request throttling (1 request per 8 seconds with burst of 5)
- **Idempotent**: Prevents duplicate records with `ON CONFLICT` handling
- **Metadata Tracking**: Stores ticker information (name, exchange, asset type, date ranges)
- **Sync Logging**: Comprehensive audit trail of all sync operations
- **Error Handling**: Graceful error handling with detailed logging
- **DuckDB Storage**: Fast, embedded analytical database with zero setup

## Technology Stack

- **Language**: Go 1.25.1
- **Database**: [DuckDB](https://duckdb.org/) via [go-duckdb](https://github.com/marcboeker/go-duckdb) v1.8.5
- **API**: [Tiingo](https://www.tiingo.com/) for market data
- **Rate Limiting**: [golang.org/x/time/rate](https://pkg.go.dev/golang.org/x/time/rate)
- **Environment**: direnv for environment variable management

## Project Structure

```
tiingo/
├── .envrc                      # Environment variables (API key)
├── .gitignore                  # Git ignore rules
├── go.mod                      # Go module definition
├── go.sum                      # Dependency checksums
├── portfolio.duckdb            # DuckDB database file
├── README.md                   # This file
├── cmd/
│   └── tiingo/
│       ├── main.go            # Application entry point
│       └── main_test.go       # Integration tests
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
│   ├── portfolio/
│   │   └── reader.go         # Portfolio file parser
│   └── tiingo/
│       └── client.go         # Tiingo API client
└── pkg/
    └── models/
        └── types.go          # Shared data types
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

4. **Build the application**:
   ```bash
   go build -o tiingo cmd/tiingo/main.go
   ```

## Usage

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

**Basic sync** (fetches new data since last sync):
```bash
./tiingo
```

**With custom paths**:
```bash
./tiingo -portfolio data/portfolio.txt -db portfolio.duckdb
```

**Verbose mode** (detailed logging):
```bash
./tiingo -verbose
```

### Command-Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-portfolio` | `data/portfolio.txt` | Path to portfolio file |
| `-db` | `portfolio.duckdb` | Path to DuckDB database file |
| `-verbose` | `false` | Enable detailed logging |

### Example Output

```
2025/11/05 14:39:22 Starting Tiingo Portfolio Sync
2025/11/05 14:39:22 ✅ Database initialized
2025/11/05 14:39:22 Found 7 ticker(s): [BTC BRKB DAL GFS MUB ORCL IBM]

[1/7] Processing BTC...
  Type: Cryptocurrency
  Ticker: BTCUSD
  Retrieved 5 price records
  Latest Close: $67543.21 (Date: 2025-11-05)

[2/7] Processing BRKB...
  Name: Berkshire Hathaway Inc. Class B
  Exchange: NYSE
  Type: Stock
  Last sync: 2025-11-04
  Retrieved 1 price records
  Saved 1 new records (total: 30)
  Latest Close: $451.23 (Date: 2025-11-05)

...

✅ Sync complete! Total tickers in database: 7
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

### Daily Sync with Cron

Add to crontab (`crontab -e`):
```bash
# Run at 4:30 PM PT (after market close)
30 16 * * 1-5 cd /Users/mdcb/devcode/PFIN/tiingo && ./tiingo >> logs/sync.log 2>&1
```

### macOS launchd (Recommended)

Create `~/Library/LaunchAgents/com.mdcb.tiingo.plist`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.mdcb.tiingo</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Users/mdcb/devcode/PFIN/tiingo/tiingo</string>
        <string>-portfolio</string>
        <string>data/portfolio.txt</string>
        <string>-db</string>
        <string>portfolio.duckdb</string>
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
    <string>/Users/mdcb/devcode/PFIN/tiingo/logs/stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/mdcb/devcode/PFIN/tiingo/logs/stderr.log</string>
</dict>
</plist>
```

Load the agent:
```bash
launchctl load ~/Library/LaunchAgents/com.mdcb.tiingo.plist
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

### Run Tests
```bash
# Run all tests
go test ./... -v

# Run tests with coverage
go test ./... -cover

# Run with race detector
go test ./... -race

# Test specific package
go test ./cmd/tiingo -v
```

### Test Coverage
The test suite includes:
- Ticker format validation (stocks vs crypto)
- Crypto ticker conversion logic (BTC → BTCUSD)
- Sync event logging with original ticker preservation
- Stock ticker pass-through (no conversion)

## Development

### Build for Development
```bash
go build -o tiingo cmd/tiingo/main.go
```

### Build with Optimizations (Production)
```bash
go build -ldflags "-s -w" -o tiingo cmd/tiingo/main.go
```

### Run Without Building
```bash
go run cmd/tiingo/main.go -verbose
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

### API Key Not Set
```
Error: TIINGO_API_KEY environment variable not set
```
**Solution**: Ensure `.envrc` is configured and `direnv allow` has been run.

### Database Locked
```
Error: database is locked
```
**Solution**: Ensure no other process is accessing the database. Close any DuckDB CLI sessions.

### Invalid Ticker
```
Error: API error (status 404): ticker not found
```
**Solution**: Verify ticker symbol is correct. Check Tiingo documentation for supported tickers.

### Rate Limit Exceeded
The built-in rate limiter should prevent this, but if you see rate limit errors:
- Reduce the burst size in `client.go`
- Increase the delay between requests
- Split your portfolio into smaller batches

## Future Enhancements

Potential features for future development:
- [ ] Support for custom date ranges via CLI flags
- [ ] Export reports (PDF, Excel)
- [ ] Web dashboard with charts
- [ ] Real-time quote fetching (IEX endpoint)
- [ ] Dividend and split tracking
- [ ] Portfolio valuation with share quantities
- [ ] Alert system for price thresholds
- [ ] Support for international exchanges
- [ ] Parallel processing with worker pools
- [ ] PostgreSQL migration support (via `pggold`)

## License

This is a personal project. See repository for license information.

## References

- [Tiingo API Documentation](https://api.tiingo.com/documentation/general/overview)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [go-duckdb Driver](https://github.com/marcboeker/go-duckdb)

## Contributing

This is a personal portfolio tracker. For questions or suggestions, please open an issue.

---

**Last Updated**: 2025-11-20  
**Version**: 1.0.0  
**Author**: mdcb
