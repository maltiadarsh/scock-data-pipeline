-- Stock Market Data Pipeline - Database Initialization Script
-- This script creates the necessary database schema for storing stock market data
-- It will be automatically executed when the PostgreSQL container starts

-- Create stock_data table if it doesn't exist
CREATE TABLE IF NOT EXISTS stock_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open_price NUMERIC(12, 4) NOT NULL,
    high_price NUMERIC(12, 4) NOT NULL,
    low_price NUMERIC(12, 4) NOT NULL,
    close_price NUMERIC(12, 4) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_symbol_timestamp UNIQUE(symbol, timestamp)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_stock_symbol ON stock_data(symbol);
CREATE INDEX IF NOT EXISTS idx_stock_timestamp ON stock_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_stock_symbol_timestamp ON stock_data(symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_stock_created_at ON stock_data(created_at);

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger to automatically update the updated_at column
DROP TRIGGER IF EXISTS update_stock_data_updated_at ON stock_data;
CREATE TRIGGER update_stock_data_updated_at
    BEFORE UPDATE ON stock_data
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create a view for the latest stock prices
CREATE OR REPLACE VIEW latest_stock_prices AS
SELECT DISTINCT ON (symbol)
    symbol,
    timestamp,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    created_at
FROM stock_data
ORDER BY symbol, timestamp DESC;

-- Create a view for daily stock statistics
CREATE OR REPLACE VIEW daily_stock_stats AS
SELECT
    symbol,
    DATE(timestamp) as trade_date,
    COUNT(*) as data_points,
    MIN(low_price) as daily_low,
    MAX(high_price) as daily_high,
    FIRST_VALUE(open_price) OVER (PARTITION BY symbol, DATE(timestamp) ORDER BY timestamp) as daily_open,
    FIRST_VALUE(close_price) OVER (PARTITION BY symbol, DATE(timestamp) ORDER BY timestamp DESC) as daily_close,
    SUM(volume) as total_volume,
    AVG(close_price) as avg_price
FROM stock_data
GROUP BY symbol, DATE(timestamp), open_price, close_price, timestamp, volume
ORDER BY symbol, trade_date DESC;

-- Grant necessary permissions (adjust if using different database name)
GRANT ALL PRIVILEGES ON TABLE stock_data TO airflow;
GRANT ALL PRIVILEGES ON SEQUENCE stock_data_id_seq TO airflow;
GRANT SELECT ON latest_stock_prices TO airflow;
GRANT SELECT ON daily_stock_stats TO airflow;

-- Insert a comment for documentation
COMMENT ON TABLE stock_data IS 'Stores intraday stock market data fetched from Alpha Vantage API';
COMMENT ON COLUMN stock_data.symbol IS 'Stock ticker symbol (e.g., AAPL, GOOGL)';
COMMENT ON COLUMN stock_data.timestamp IS 'Timestamp of the stock data point';
COMMENT ON COLUMN stock_data.open_price IS 'Opening price for the time interval';
COMMENT ON COLUMN stock_data.high_price IS 'Highest price during the time interval';
COMMENT ON COLUMN stock_data.low_price IS 'Lowest price during the time interval';
COMMENT ON COLUMN stock_data.close_price IS 'Closing price for the time interval';
COMMENT ON COLUMN stock_data.volume IS 'Trading volume during the time interval';
COMMENT ON COLUMN stock_data.created_at IS 'Timestamp when the record was first created';
COMMENT ON COLUMN stock_data.updated_at IS 'Timestamp when the record was last updated';

-- Log successful initialization
DO $$
BEGIN
    RAISE NOTICE 'Stock market data pipeline database initialized successfully';
    RAISE NOTICE 'Tables created: stock_data';
    RAISE NOTICE 'Views created: latest_stock_prices, daily_stock_stats';
    RAISE NOTICE 'Indexes created for optimal query performance';
END $$;
