package database

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/mdcb/tiingo-tracker/pkg/models"
)

//go:embed ddl/*.sql
var ddlFiles embed.FS

// Manager handles all database operations
type Manager struct {
	db *sql.DB
}

// NewManager creates a new database manager
func NewManager(dbPath string) (*Manager, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	return &Manager{db: db}, nil
}

// InitSchema initializes the database schema
func (m *Manager) InitSchema() error {
	// Read and execute schema file
	schemaSQL, err := ddlFiles.ReadFile("ddl/01_schema.sql")
	if err != nil {
		return fmt.Errorf("reading schema file: %w", err)
	}

	if _, err := m.db.Exec(string(schemaSQL)); err != nil {
		return fmt.Errorf("executing schema: %w", err)
	}

	return nil
}

// UpsertTickerMetadata inserts or updates ticker metadata
func (m *Manager) UpsertTickerMetadata(ctx context.Context, ticker *models.TickerRecord) error {
	query := `
		INSERT INTO tickers (ticker, name, exchange, asset_type, start_date, end_date, last_updated)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (ticker) DO UPDATE SET
			name = excluded.name,
			exchange = excluded.exchange,
			asset_type = excluded.asset_type,
			start_date = excluded.start_date,
			end_date = excluded.end_date,
			last_updated = excluded.last_updated
	`

	_, err := m.db.ExecContext(ctx, query,
		ticker.Ticker,
		ticker.Name,
		ticker.Exchange,
		ticker.AssetType,
		ticker.StartDate,
		ticker.EndDate,
		ticker.LastUpdated,
	)

	if err != nil {
		return fmt.Errorf("upserting ticker %s: %w", ticker.Ticker, err)
	}

	return nil
}

// InsertDailyPrices inserts daily price records (skips duplicates)
func (m *Manager) InsertDailyPrices(ctx context.Context, prices []models.PriceRecord) error {
	if len(prices) == 0 {
		return nil
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	query := `
		INSERT INTO daily_prices (ticker, date, open, high, low, close, volume, adj_close, adj_volume)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (ticker, date) DO NOTHING
	`

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	for _, price := range prices {
		_, err := stmt.ExecContext(ctx,
			price.Ticker,
			price.Date,
			price.Open,
			price.High,
			price.Low,
			price.Close,
			price.Volume,
			price.AdjClose,
			price.AdjVolume,
		)
		if err != nil {
			return fmt.Errorf("inserting price for %s on %s: %w", price.Ticker, price.Date, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// GetLastSyncDate returns the most recent date for a ticker
func (m *Manager) GetLastSyncDate(ctx context.Context, ticker string) (*time.Time, error) {
	query := `SELECT MAX(date) FROM daily_prices WHERE ticker = ?`

	var lastDate sql.NullTime
	err := m.db.QueryRowContext(ctx, query, ticker).Scan(&lastDate)
	if err != nil {
		return nil, fmt.Errorf("querying last sync date for %s: %w", ticker, err)
	}

	if !lastDate.Valid {
		return nil, nil // No data yet
	}

	return &lastDate.Time, nil
}

// LogSyncEvent records a sync operation
func (m *Manager) LogSyncEvent(ctx context.Context, event *models.SyncEvent) error {
	query := `
		INSERT INTO sync_log (log_id, ticker, sync_date, records_inserted, status, error_message)
		VALUES (nextval('sync_log_seq'), ?, ?, ?, ?, ?)
	`

	_, err := m.db.ExecContext(ctx, query,
		event.Ticker,
		event.SyncDate,
		event.RecordsInserted,
		event.Status,
		event.ErrorMessage,
	)

	if err != nil {
		return fmt.Errorf("logging sync event for %s: %w", event.Ticker, err)
	}

	return nil
}

// GetTickerCount returns the number of tickers in the database
func (m *Manager) GetTickerCount(ctx context.Context) (int, error) {
	var count int
	err := m.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tickers").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting tickers: %w", err)
	}
	return count, nil
}

// GetPriceCount returns the number of price records for a ticker
func (m *Manager) GetPriceCount(ctx context.Context, ticker string) (int, error) {
	var count int
	query := "SELECT COUNT(*) FROM daily_prices WHERE ticker = ?"
	err := m.db.QueryRowContext(ctx, query, ticker).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting prices for %s: %w", ticker, err)
	}
	return count, nil
}

// Close closes the database connection
func (m *Manager) Close() error {
	if err := m.db.Close(); err != nil {
		return fmt.Errorf("closing database: %w", err)
	}
	return nil
}
