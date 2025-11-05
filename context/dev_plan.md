# Portfolio Tracker Development Plan
**Project**: Tiingo API → DuckDB Portfolio Tracker  
**Created**: 2025-11-05

## Project Overview
Application to read portfolio holdings from a text file, fetch market data from Tiingo API, and store results in a local DuckDB database.

---

## Technology Stack
- **Language**: Go 1.21+
- **Package Manager**: Go modules
- **API**: Tiingo (market data)
- **Database**: DuckDB (via go-duckdb driver)
- **Linting**: golangci-lint
- **Environment**: direnv (.envrc)

---

## Directory Structure
```
tiingo/
├── .envrc                    # Environment variables (API keys)
├── context/                  # Documentation and plans
│   ├── dev_plan.md          # This file
│   └── status_*.md          # Status reports
├── go.mod                   # Go module definition
├── go.sum                   # Dependency checksums
├── cmd/
│   └── tiingo/
│       └── main.go          # Application entry point
├── internal/
│   ├── portfolio/
│   │   └── reader.go        # Parse portfolio text file
│   ├── tiingo/
│   │   └── client.go        # API client wrapper
│   ├── database/
│   │   └── manager.go       # DuckDB operations
│   └── config/
│       └── config.go        # Configuration management
├── pkg/
│   └── models/
│       └── types.go         # Shared data types
├── data/
│   ├── portfolio.txt        # Input: holdings list
│   └── portfolio.duckdb     # Output: market data
├── scripts/
│   └── build.sh             # Build scripts
└── README.md
```

---

## Phase 1: Project Setup

### 1.1 Initialize Go Module
```bash
go mod init github.com/mdcb/tiingo-tracker
go get github.com/marcboeker/go-duckdb
go get github.com/joho/godotenv
```

### 1.2 Install Development Tools
```bash
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
go install github.com/cosmtrek/air@latest  # Hot reload for development
```

### 1.3 Configure Linting
Create `.golangci.yml`:
```yaml
linters:
  enable:
    - gofmt
    - govet
    - errcheck
    - staticcheck
    - gosimple
    - ineffassign
    - unused
    - misspell

linters-settings:
  gofmt:
    simplify: true

run:
  timeout: 5m
  tests: true
```

### 1.4 Environment Variables
`.envrc` already exists with:
- `TIINGO_API_KEY`
- Ensure direnv is allowed: `direnv allow`

---

## Phase 2: Core Components

### 2.1 Portfolio Reader (`internal/portfolio/reader.go`)
**Purpose**: Parse text file containing ticker symbols

**Input Format** (portfolio.txt):
```
AAPL
MSFT
GOOGL
TSLA
```

**Functions**:
```go
type Reader struct {}

func NewReader() *Reader
func (r *Reader) ReadPortfolio(filePath string) ([]string, error)
func (r *Reader) ValidateTicker(symbol string) bool
```
- Handle comments (#), blank lines, whitespace
- Return errors using Go's error handling pattern

---

### 2.2 Tiingo API Client (`internal/tiingo/client.go`)
**Purpose**: Fetch market data from Tiingo

**Endpoints to Support**:
1. **Daily Price**: `/tiingo/daily/<ticker>/prices`
2. **Quote**: `/iex/<ticker>` (real-time)
3. **Metadata**: `/tiingo/daily/<ticker>`

**Key Features**:
- Rate limiting (500 requests/hour free tier) using `golang.org/x/time/rate`
- Retry logic with exponential backoff
- Error handling for invalid tickers
- Response validation
- Context support for timeouts and cancellation

**Type Definitions**:
```go
type Client struct {
    baseURL    string
    apiKey     string
    httpClient *http.Client
    limiter    *rate.Limiter
}

type DailyPrice struct {
    Date      time.Time `json:"date"`
    Open      float64   `json:"open"`
    High      float64   `json:"high"`
    Low       float64   `json:"low"`
    Close     float64   `json:"close"`
    Volume    int64     `json:"volume"`
    AdjClose  float64   `json:"adjClose"`
    AdjVolume int64     `json:"adjVolume"`
}

type TickerMetadata struct {
    Ticker      string    `json:"ticker"`
    Name        string    `json:"name"`
    Exchange    string    `json:"exchangeCode"`
    AssetType   string    `json:"assetType"`
    StartDate   time.Time `json:"startDate"`
    EndDate     time.Time `json:"endDate"`
}
```

**Functions**:
```go
func NewClient(apiKey string) *Client
func (c *Client) GetDailyPrices(ctx context.Context, ticker string, startDate, endDate time.Time) ([]DailyPrice, error)
func (c *Client) GetCurrentQuote(ctx context.Context, ticker string) (*DailyPrice, error)
func (c *Client) GetTickerMetadata(ctx context.Context, ticker string) (*TickerMetadata, error)
```

---

### 2.3 DuckDB Manager (`internal/database/manager.go`)
**Purpose**: Manage database schema and operations

**Schema Design**:

```sql
-- Ticker metadata
CREATE TABLE IF NOT EXISTS tickers (
    ticker VARCHAR PRIMARY KEY,
    name VARCHAR,
    exchange VARCHAR,
    asset_type VARCHAR,
    start_date DATE,
    end_date DATE,
    last_updated TIMESTAMP
);

-- Daily price data
CREATE TABLE IF NOT EXISTS daily_prices (
    ticker VARCHAR,
    date DATE,
    open DECIMAL(12,4),
    high DECIMAL(12,4),
    low DECIMAL(12,4),
    close DECIMAL(12,4),
    volume BIGINT,
    adj_close DECIMAL(12,4),
    adj_volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date),
    FOREIGN KEY (ticker) REFERENCES tickers(ticker)
);

-- Portfolio snapshots (optional)
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    snapshot_id INTEGER PRIMARY KEY,
    snapshot_date TIMESTAMP,
    ticker VARCHAR,
    shares DECIMAL(12,4),
    cost_basis DECIMAL(12,4),
    FOREIGN KEY (ticker) REFERENCES tickers(ticker)
);

-- Audit log
CREATE TABLE IF NOT EXISTS sync_log (
    log_id INTEGER PRIMARY KEY,
    ticker VARCHAR,
    sync_date TIMESTAMP,
    records_inserted INTEGER,
    status VARCHAR,
    error_message VARCHAR
);
```

**Type Definitions**:
```go
type Manager struct {
    db *sql.DB
}

type TickerRecord struct {
    Ticker      string
    Name        string
    Exchange    string
    AssetType   string
    StartDate   time.Time
    EndDate     time.Time
    LastUpdated time.Time
}

type PriceRecord struct {
    Ticker    string
    Date      time.Time
    Open      float64
    High      float64
    Low       float64
    Close     float64
    Volume    int64
    AdjClose  float64
    AdjVolume int64
}
```

**Functions**:
```go
func NewManager(dbPath string) (*Manager, error)
func (m *Manager) InitSchema() error
func (m *Manager) UpsertTickerMetadata(ctx context.Context, ticker *TickerRecord) error
func (m *Manager) InsertDailyPrices(ctx context.Context, prices []PriceRecord) error
func (m *Manager) GetLastSyncDate(ctx context.Context, ticker string) (*time.Time, error)
func (m *Manager) LogSyncEvent(ctx context.Context, ticker, status string, records int, err error) error
func (m *Manager) Close() error
```

---

### 2.4 Main Orchestrator (`cmd/tiingo/main.go`)
**Purpose**: Coordinate the workflow

**Workflow**:
```
1. Parse CLI flags
2. Load environment variables
3. Read portfolio.txt → []string
4. Initialize DuckDB connection and schema
5. Initialize Tiingo client
6. For each ticker:
   a. Check last sync date in DB
   b. Fetch metadata (if new ticker)
   c. Fetch prices from last_sync_date to today
   d. Insert/update data in DuckDB using transaction
   e. Log sync event
7. Generate summary report
8. Close connections gracefully
```

**CLI Interface** (using flag package or cobra):
```bash
go run cmd/tiingo/main.go -portfolio data/portfolio.txt -db data/portfolio.duckdb
go run cmd/tiingo/main.go -portfolio data/portfolio.txt -db data/portfolio.duckdb -full-sync
go run cmd/tiingo/main.go -portfolio data/portfolio.txt -db data/portfolio.duckdb -ticker AAPL

# Or after building:
./tiingo -portfolio data/portfolio.txt -db data/portfolio.duckdb -verbose
```

**Flags**:
- `-portfolio`: Path to portfolio file (default: data/portfolio.txt)
- `-db`: Path to DuckDB file (default: data/portfolio.duckdb)
- `-full-sync`: Re-fetch all historical data
- `-ticker`: Only sync specific ticker
- `-start-date`: Custom start date (YYYY-MM-DD)
- `-verbose`: Debug logging
- `-workers`: Number of concurrent workers (default: 3)

**Structure**:
```go
type App struct {
    config   *config.Config
    db       *database.Manager
    client   *tiingo.Client
    reader   *portfolio.Reader
    logger   *log.Logger
}

func main() {
    // Parse flags
    // Load config
    // Initialize app
    // Run sync with context and cancellation
    // Handle graceful shutdown
}
```

---

## Phase 3: Error Handling & Resilience

### 3.1 API Error Handling
- **Rate Limiting**: Implement token bucket or sleep between requests
- **Network Errors**: Retry 3x with exponential backoff
- **Invalid Ticker**: Log warning, continue with other tickers
- **Malformed Response**: Validate schema, log error

### 3.2 Database Error Handling
- **Connection Failures**: Retry with timeout
- **Constraint Violations**: Log and skip duplicate
- **Disk Space**: Check available space before writes

### 3.3 Data Validation
- Date ranges (no future dates)
- Numeric bounds (price > 0, volume >= 0)
- Ticker format (uppercase, alphanumeric)

---

## Phase 4: Testing Strategy

### 4.1 Unit Tests
```bash
go test ./... -v
go test ./... -cover
go test ./... -race  # Race condition detection
```

**Test Coverage**:
- `internal/portfolio/reader_test.go`: File parsing, edge cases
- `internal/tiingo/client_test.go`: Mock HTTP responses using httptest
- `internal/database/manager_test.go`: In-memory DuckDB operations
- `cmd/tiingo/main_test.go`: Integration tests

**Test Patterns**:
```go
func TestReaderReadPortfolio(t *testing.T) {
    tests := []struct {
        name    string
        input   string
        want    []string
        wantErr bool
    }{
        // Test cases
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Test implementation
        })
    }
}
```

### 4.2 Integration Tests
- End-to-end test with sample portfolio
- Mock Tiingo API with httptest.Server
- Verify DuckDB data integrity
- Use testify/assert for cleaner assertions

### 4.3 Manual Testing
```bash
# Test with single ticker
echo "AAPL" > data/test_portfolio.txt
go run cmd/tiingo/main.go -portfolio data/test_portfolio.txt -db data/test.duckdb -verbose
```

---

## Phase 5: Operational Procedures

### 5.1 Daily Sync Command
```bash
./tiingo -portfolio data/portfolio.txt -db data/portfolio.duckdb -verbose

# Or if not built:
go run cmd/tiingo/main.go -portfolio data/portfolio.txt -db data/portfolio.duckdb -verbose
```

**Build the binary**:
```bash
go build -o tiingo cmd/tiingo/main.go

# With version info:
go build -ldflags "-X main.version=1.0.0 -X main.buildDate=$(date -u +%Y-%m-%d)" -o tiingo cmd/tiingo/main.go
```

**Recommended Schedule**: 
- Run after market close (4:30 PM ET / 1:30 PM PT)
- Use cron or launchd for automation

**Example launchd plist** (`~/Library/LaunchAgents/com.mdcb.tiingo.plist`):
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
        <string>data/portfolio.duckdb</string>
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
        <string>YOUR_API_KEY</string>
    </dict>
</dict>
</plist>
```

### 5.2 Full Resync (Monthly)
```bash
./tiingo -full-sync -portfolio data/portfolio.txt -db data/portfolio.duckdb
```

### 5.3 Add New Ticker
1. Edit `data/portfolio.txt`, add ticker
2. Run: `./tiingo`
3. Script auto-detects new ticker and fetches full history

### 5.4 Database Queries
```bash
duckdb data/portfolio.duckdb
```

**Useful Queries**:
```sql
-- Latest prices
SELECT ticker, date, close, volume 
FROM daily_prices 
WHERE date = (SELECT MAX(date) FROM daily_prices)
ORDER BY ticker;

-- Portfolio performance
SELECT ticker, 
       MIN(close) as low_52w,
       MAX(close) as high_52w,
       AVG(close) as avg_price
FROM daily_prices 
WHERE date >= CURRENT_DATE - INTERVAL 1 YEAR
GROUP BY ticker;

-- Sync status
SELECT * FROM sync_log 
ORDER BY sync_date DESC 
LIMIT 10;
```

---

## Phase 6: Monitoring & Maintenance

### 6.1 Health Checks
- API key validity (test request on startup)
- Database file size monitoring
- Last successful sync timestamp

### 6.2 Logging
**Format**: Structured logging using `log/slog` (Go 1.21+)
```go
import "log/slog"

logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
}))

logger.Info("sync complete",
    "ticker", "AAPL",
    "records", 250,
    "duration_ms", 1234,
)
```

**Output**:
```json
{
  "time": "2025-11-05T13:50:00Z",
  "level": "INFO",
  "msg": "sync complete",
  "ticker": "AAPL",
  "records": 250,
  "duration_ms": 1234
}
```

**Log Location**: `logs/tiingo_sync.log` (rotate daily)
- Use `gopkg.in/natefinch/lumberjack.v2` for log rotation

### 6.3 Backup Strategy
```bash
# Daily backup
cp data/portfolio.duckdb data/backups/portfolio_$(date +%Y%m%d).duckdb

# Export to CSV (for portability)
duckdb data/portfolio.duckdb -c "COPY daily_prices TO 'exports/prices.csv' (FORMAT CSV, HEADER)"
```

### 6.4 Maintenance Tasks
**Weekly**:
- Review sync_log for errors
- Check disk space

**Monthly**:
- Validate data integrity (no gaps in dates)
- Full resync to catch any missed data
- Prune old backup files (keep 3 months)

**Quarterly**:
- Review API usage vs. limits
- Optimize database (VACUUM if needed)

---

## Phase 7: Future Enhancements

### 7.1 Features
- Support for CSV portfolio format with share quantities
- Calculate unrealized gains/losses
- Export reports (PDF/Excel)
- Web dashboard (FastAPI + Plotly)
- Support for crypto tickers
- Dividend tracking

### 7.2 Performance
- Goroutines for concurrent API requests
- Worker pool pattern for parallel ticker processing
- Database connection pooling
- Incremental-only sync by default
- Batch inserts with prepared statements

### 7.3 Data Sources
- Add Alpha Vantage fallback
- Yahoo Finance as secondary source
- News sentiment integration

---

## Quick Reference Commands

### Setup
```bash
go mod init github.com/mdcb/tiingo-tracker
go get github.com/marcboeker/go-duckdb
go get github.com/joho/godotenv
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
direnv allow
```

### Build
```bash
# Development build
go build -o tiingo cmd/tiingo/main.go

# Production build with optimizations
go build -ldflags "-s -w" -o tiingo cmd/tiingo/main.go

# Cross-compile for Linux
GOOS=linux GOARCH=amd64 go build -o tiingo-linux cmd/tiingo/main.go
```

### Run
```bash
# Daily sync
./tiingo

# Full sync
./tiingo -full-sync

# Single ticker
./tiingo -ticker AAPL

# Verbose mode
./tiingo -verbose

# Multiple workers
./tiingo -workers 5
```

### Test
```bash
go test ./... -v
go test ./... -cover
go test ./... -race
golangci-lint run
go fmt ./...
```

### Query
```bash
duckdb data/portfolio.duckdb -c "SELECT * FROM daily_prices ORDER BY date DESC LIMIT 10"
```

---

## Risk Mitigation

### API Risks
- **Rate Limits**: Cache responses, batch requests
- **Key Expiry**: Monitor for 401 errors, alert
- **Service Downtime**: Implement retry queue

### Data Risks
- **Missing Data**: Validate date continuity
- **Bad Data**: Implement bounds checking
- **Data Loss**: Automated backups

### Operational Risks
- **Forgotten Syncs**: Automate with cron
- **Config Drift**: Version control .envrc template
- **Knowledge Loss**: This documentation

---

## Success Metrics
- ✅ Zero manual data entry
- ✅ Daily sync completion rate > 99%
- ✅ Data freshness < 24 hours
- ✅ Query performance < 100ms
- ✅ Zero data loss incidents

---

**Next Steps**: Start with Phase 1 project setup
