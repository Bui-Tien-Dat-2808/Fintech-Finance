\connect analytics;

CREATE TABLE IF NOT EXISTS raw_trades (
    symbol TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    trade_timestamp TIMESTAMPTZ NOT NULL,
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    source TEXT,
    trade_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS aggregated_trades_1m (
    symbol TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,
    min_price DOUBLE PRECISION NOT NULL,
    max_price DOUBLE PRECISION NOT NULL,
    total_volume BIGINT NOT NULL,
    trade_count BIGINT NOT NULL,
    trade_date DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_trades_symbol_timestamp
    ON raw_trades (symbol, trade_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_aggregated_trades_symbol_window
    ON aggregated_trades_1m (symbol, window_start DESC);

CREATE OR REPLACE VIEW stock_summary_latest AS
SELECT
    symbol,
    MAX(window_start) AS latest_window_start,
    SUM(total_volume) AS total_volume,
    SUM(trade_count) AS trade_count,
    AVG(avg_price) AS avg_price
FROM aggregated_trades_1m
GROUP BY symbol;
