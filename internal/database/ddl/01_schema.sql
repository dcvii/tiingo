-- Portfolio Tracker Database Schema
-- DuckDB 1.0+

-- Ticker metadata table
CREATE TABLE IF NOT EXISTS tickers (
    ticker VARCHAR PRIMARY KEY,
    name VARCHAR,
    exchange VARCHAR,
    asset_type VARCHAR,
    start_date DATE,
    end_date DATE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily price data table
CREATE TABLE IF NOT EXISTS daily_prices (
    ticker VARCHAR,
    date DATE,
    open DECIMAL(18,6),
    high DECIMAL(18,6),
    low DECIMAL(18,6),
    close DECIMAL(18,6),
    volume BIGINT,
    adj_close DECIMAL(18,6),
    adj_volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date),
    FOREIGN KEY (ticker) REFERENCES tickers(ticker)
);

-- Sync operation log table
CREATE TABLE IF NOT EXISTS sync_log (
    log_id INTEGER PRIMARY KEY,
    ticker VARCHAR,
    sync_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_inserted INTEGER,
    status VARCHAR,
    error_message VARCHAR,
    FOREIGN KEY (ticker) REFERENCES tickers(ticker)
);

-- Create sequence for log_id
CREATE SEQUENCE IF NOT EXISTS sync_log_seq START 1;
