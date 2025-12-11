"""
Stock Market Data Fetcher
Fetches stock market data from Alpha Vantage API and stores in PostgreSQL database.
Includes comprehensive error handling and resilience features.
"""

import os
import sys
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import requests
import psycopg2 #type:ignore
from psycopg2.extras import execute_values #type:ignore
from psycopg2 import pool #type:ignore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockDataFetcher:
    """Fetches and processes stock market data from Alpha Vantage API."""
    
    def __init__(self, api_key: str, max_retries: int = 3, retry_delay: int = 5):
        """
        Initialize the stock data fetcher.
        
        Args:
            api_key: Alpha Vantage API key
            max_retries: Maximum number of retry attempts for API calls
            retry_delay: Delay in seconds between retries
        """
        if not api_key:
            raise ValueError("API key cannot be empty")
        
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        
    def fetch_stock_data(self, symbol: str, function: str = "TIME_SERIES_DAILY", #type:ignore
                        interval: str = None) -> Optional[Dict]:
        """
        Fetch stock data from Alpha Vantage API with retry logic.
        Uses TIME_SERIES_DAILY (free tier) instead of TIME_SERIES_INTRADAY (premium).
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL', 'GOOGL')
            function: API function type (default: TIME_SERIES_DAILY for free tier)
            interval: Time interval (not used for daily data)
            
        Returns:
            Parsed JSON response or None if all retries fail
        """
        params = {
            'function': function,
            'symbol': symbol,
            'apikey': self.api_key,
            'outputsize': 'compact'  # Using 'compact' for free tier (returns 100 latest data points)
        }
        
        # Only add interval for intraday functions
        if interval and 'INTRADAY' in function:
            params['interval'] = interval
        
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"Fetching data for {symbol} (attempt {attempt}/{self.max_retries})")
                
                response = self.session.get( #type:ignore
                    self.base_url, 
                    params=params, 
                    timeout=30
                )
                response.raise_for_status()
                
                data = response.json()
                
                # Check for API errors
                if "Error Message" in data:
                    logger.error(f"API Error for {symbol}: {data['Error Message']}")
                    return None
                
                if "Note" in data:
                    logger.warning(f"API Rate Limit for {symbol}: {data['Note']}")
                    if attempt < self.max_retries:
                        time.sleep(self.retry_delay * 2)  # Longer wait for rate limits
                        continue
                    return None
                
                # Check if data exists - determine the correct time series key
                time_series_key = None
                if "Time Series (Daily)" in data:
                    time_series_key = "Time Series (Daily)"
                elif interval and f"Time Series ({interval})" in data:
                    time_series_key = f"Time Series ({interval})"
                else:
                    # Try to find any time series key
                    for key in data.keys():
                        if key.startswith("Time Series"):
                            time_series_key = key
                            break
                
                if not time_series_key:
                    logger.error(f"No time series data found for {symbol}. Response keys: {list(data.keys())}")
                    return None
                
                logger.info(f"Successfully fetched data for {symbol}")
                return data
                
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout fetching {symbol} (attempt {attempt}/{self.max_retries})")
                if attempt < self.max_retries: #type:ignore
                    time.sleep(self.retry_delay)
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error for {symbol}: {str(e)}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)
                    
            except ValueError as e:
                logger.error(f"JSON parsing error for {symbol}: {str(e)}")
                return None
                
            except Exception as e:
                logger.error(f"Unexpected error fetching {symbol}: {str(e)}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)
        
        logger.error(f"Failed to fetch data for {symbol} after {self.max_retries} attempts")
        return None
    
    def parse_stock_data(self, symbol: str, data: Dict, 
                        interval: str = None) -> List[Tuple]:
        """
        Parse stock data from API response into database-ready format.
        Handles both daily and intraday data formats.
        
        Args:
            symbol: Stock symbol
            data: JSON response from API
            interval: Time interval used (for intraday data)
            
        Returns:
            List of tuples ready for database insertion
        """
        try:
            # Determine the time series key #type:ignore
            time_series_key = None
            if "Time Series (Daily)" in data:
                time_series_key = "Time Series (Daily)"
            elif interval and f"Time Series ({interval})" in data:
                time_series_key = f"Time Series ({interval})"
            else:
                # Try to find any time series key
                for key in data.keys():
                    if key.startswith("Time Series"):
                        time_series_key = key
                        break
            
            if time_series_key not in data: #type:ignore
                logger.error(f"No time series data in response for {symbol}")
                return []
            
            time_series = data[time_series_key]
            parsed_records = []
            
            for timestamp, values in time_series.items():
                try:
                    # Parse timestamp - handle both daily (YYYY-MM-DD) and intraday (YYYY-MM-DD HH:MM:SS) formats
                    try:
                        dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")  # Intraday format #type:ignore
                    except ValueError:
                        dt = datetime.strptime(timestamp, "%Y-%m-%d")  # Daily format
                    
                    # Extract values with defaults for missing data
                    open_price = float(values.get("1. open", 0)) #type:ignore
                    high_price = float(values.get("2. high", 0))
                    low_price = float(values.get("3. low", 0))
                    close_price = float(values.get("4. close", 0))
                    volume = int(values.get("5. volume", 0))
                    
                    # Validate data
                    if all([open_price > 0, high_price > 0, low_price > 0, 
                           close_price > 0, volume >= 0]):
                        record = (
                            symbol,
                            dt,
                            open_price,
                            high_price,
                            low_price,
                            close_price,
                            volume,
                            datetime.now()  # created_at #type:ignore
                        )
                        parsed_records.append(record)
                    else:
                        logger.warning(f"Invalid data for {symbol} at {timestamp}: skipping")
                        
                except (ValueError, KeyError) as e:
                    logger.warning(f"Error parsing record for {symbol} at {timestamp}: {str(e)}")
                    continue
            
            logger.info(f"Parsed {len(parsed_records)} valid records for {symbol}")
            return parsed_records
            
        except Exception as e:
            logger.error(f"Error parsing data for {symbol}: {str(e)}")
            return []


class StockDatabase:
    """Manages database operations for stock data."""
    
    def __init__(self, host: str, port: int, dbname: str, user: str, password: str,
                 min_conn: int = 1, max_conn: int = 10):
        """
        Initialize database connection pool.
        
        Args:
            host: Database host
            port: Database port
            dbname: Database name
            user: Database user
            password: Database password
            min_conn: Minimum connections in pool
            max_conn: Maximum connections in pool
        """
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool( #type:ignore
                min_conn,
                max_conn,
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password
            )
            
            if self.connection_pool:
                logger.info("Database connection pool created successfully")
            else:
                raise Exception("Failed to create connection pool") #type:ignore
                
        except psycopg2.Error as e:
            logger.error(f"Error creating database connection pool: {str(e)}")
            raise
    
    def get_connection(self):
        """Get a connection from the pool."""
        try:
            return self.connection_pool.getconn()
        except psycopg2.Error as e:
            logger.error(f"Error getting connection from pool: {str(e)}")
            raise
    
    def return_connection(self, conn):
        """Return a connection to the pool."""
        if not conn:
            return
        try:
            self.connection_pool.putconn(conn)
        except (psycopg2.Error, AttributeError) as e:
            logger.error(f"Error returning connection to pool: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error returning connection: {str(e)}")
    
    def create_table_if_not_exists(self):
        """Create the stock_data table if it doesn't exist."""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()#type:ignore
            
            create_table_query = """
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
                UNIQUE(symbol, timestamp)
            );
            
            CREATE INDEX IF NOT EXISTS idx_stock_symbol ON stock_data(symbol);
            CREATE INDEX IF NOT EXISTS idx_stock_timestamp ON stock_data(timestamp);
            CREATE INDEX IF NOT EXISTS idx_stock_symbol_timestamp ON stock_data(symbol, timestamp);
            """
            
            cursor.execute(create_table_query)
            conn.commit()
            cursor.close()
            
            logger.info("Stock data table and indexes created/verified successfully")
            
        except psycopg2.Error as e:
            logger.error(f"Error creating table: {str(e)}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.return_connection(conn)
    
    def upsert_stock_data(self, records: List[Tuple]) -> Tuple[int, int]:
        """
        Insert or update stock data records in the database.
        
        Args:
            records: List of tuples containing stock data
            
        Returns:
            Tuple of (inserted_count, updated_count)
        """
        if not records:
            logger.warning("No records to insert")
            return 0, 0
        
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            upsert_query = """
            INSERT INTO stock_data (symbol, timestamp, open_price, high_price, 
                                   low_price, close_price, volume, created_at)
            VALUES %s
            ON CONFLICT (symbol, timestamp) 
            DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                created_at = EXCLUDED.created_at
            """
            
            execute_values(cursor, upsert_query, records)
            affected_rows = cursor.rowcount
            
            conn.commit()
            cursor.close()
            
            logger.info(f"Successfully upserted {affected_rows} records")
            return affected_rows, 0 #type:ignore
            
        except psycopg2.Error as e:
            logger.error(f"Error upserting data: {str(e)}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.return_connection(conn)
    
    def get_latest_timestamp(self, symbol: str) -> Optional[datetime]:
        """
        Get the latest timestamp for a given symbol.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Latest timestamp or None if no data exists
        """
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT MAX(timestamp) FROM stock_data WHERE symbol = %s
            """
            
            cursor.execute(query, (symbol,))
            result = cursor.fetchone()
            cursor.close()
            
            return result[0] if result and result[0] else None
            
        except psycopg2.Error as e:
            logger.error(f"Error fetching latest timestamp: {str(e)}")
            return None
        finally:
            if conn:
                self.return_connection(conn)
    
    def close_all_connections(self):
        """Close all connections in the pool."""
        if not hasattr(self, 'connection_pool') or not self.connection_pool:
            return
        try:
            self.connection_pool.closeall()
            logger.info("All database connections closed")
        except (psycopg2.Error, AttributeError) as e:
            logger.error(f"Error closing connections: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error closing connections: {str(e)}")


def fetch_and_store_stock_data(symbols: List[str], **kwargs) -> Dict:#type:ignore
    """
    Main function to fetch and store stock data for given symbols.
    
    Args:
        symbols: List of stock symbols to fetch
        **kwargs: Additional arguments (for Airflow compatibility)
        
    Returns:
        Dictionary with execution statistics
    """
    stats = {
        'total_symbols': len(symbols),
        'successful_fetches': 0,
        'failed_fetches': 0,
        'total_records_inserted': 0,
        'errors': []
    }
    
    # Get configuration from environment variables
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    db_host = os.getenv('STOCK_DB_HOST', 'postgres')
    db_port = int(os.getenv('STOCK_DB_PORT', 5432))#type:ignore
    db_name = os.getenv('STOCK_DB_NAME', 'airflow')
    db_user = os.getenv('STOCK_DB_USER', 'airflow')
    db_password = os.getenv('STOCK_DB_PASSWORD', 'airflow')
    
    if not api_key:
        error_msg = "ALPHA_VANTAGE_API_KEY environment variable not set"
        logger.error(error_msg)
        stats['errors'].append(error_msg)
        raise ValueError(error_msg)
    
    fetcher = None
    db = None
    
    try:
        # Initialize fetcher and database
        fetcher = StockDataFetcher(api_key)
        db = StockDatabase(db_host, db_port, db_name, db_user, db_password)
        
        # Create table if not exists
        db.create_table_if_not_exists()
        
        # Process each symbol
        for symbol in symbols:
            try:
                logger.info(f"Processing symbol: {symbol}")
                
                # Fetch data
                data = fetcher.fetch_stock_data(symbol)
                
                if data is None:
                    stats['failed_fetches'] += 1
                    stats['errors'].append(f"Failed to fetch data for {symbol}")
                    continue
                
                # Parse data
                records = fetcher.parse_stock_data(symbol, data)
                
                if not records:
                    stats['failed_fetches'] += 1
                    stats['errors'].append(f"No valid records parsed for {symbol}")
                    continue
                
                # Store in database
                inserted, updated = db.upsert_stock_data(records)
                
                stats['successful_fetches'] += 1
                stats['total_records_inserted'] += inserted
                
                logger.info(f"Completed processing for {symbol}: {inserted} records")
                
                # Rate limiting - Alpha Vantage free tier: 5 calls per minute
                time.sleep(12)  # Wait 12 seconds between calls
                
            except Exception as e:
                error_msg = f"Error processing {symbol}: {str(e)}"
                logger.error(error_msg)
                stats['failed_fetches'] += 1
                stats['errors'].append(error_msg)
                continue
        
        # Log final statistics
        logger.info("=" * 60)
        logger.info("Execution Summary:")
        logger.info(f"Total Symbols: {stats['total_symbols']}")
        logger.info(f"Successful Fetches: {stats['successful_fetches']}")
        logger.info(f"Failed Fetches: {stats['failed_fetches']}")
        logger.info(f"Total Records Inserted: {stats['total_records_inserted']}")
        logger.info("=" * 60)
        
        if stats['errors']:
            logger.warning(f"Errors encountered: {len(stats['errors'])}")
            for error in stats['errors']:
                logger.warning(f"  - {error}")
        
        return stats
        
    except Exception as e:
        error_msg = f"Critical error in main execution: {str(e)}"
        logger.error(error_msg)
        stats['errors'].append(error_msg)
        raise
        
    finally:
        # Cleanup
        if db:
            db.close_all_connections()
        if fetcher and fetcher.session:
            fetcher.session.close()


def main():
    """Main entry point when running as a standalone script."""
    try:
        # Get symbols from environment or use defaults
        symbols_str = os.getenv('STOCK_SYMBOLS', 'AAPL,GOOGL,MSFT')
        symbols = [s.strip() for s in symbols_str.split(',') if s.strip()]
        
        logger.info(f"Starting stock data pipeline for symbols: {symbols}")
        
        stats = fetch_and_store_stock_data(symbols)
        
        # Exit with error if all fetches failed
        if stats['failed_fetches'] == stats['total_symbols']:
            logger.error("All stock fetches failed!")
            sys.exit(1)
        
        logger.info("Stock data pipeline completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
