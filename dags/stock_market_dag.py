"""
Stock Market Data Pipeline DAG
Apache Airflow DAG for orchestrating stock market data fetching and storage.
Scheduled to run hourly with comprehensive error handling and monitoring.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List

from airflow import DAG #type:ignore
from airflow.operators.python import PythonOperator #type:ignore
from airflow.operators.bash import BashOperator #type:ignore
from airflow.utils.dates import days_ago  #type:ignore
from airflow.exceptions import AirflowException  #type:ignore

# Add scripts directory to Python path
sys.path.insert(0, '/opt/airflow/scripts') #type:ignore

from fetch_stock_data import fetch_and_store_stock_data  #type:ignore


# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
    'execution_timeout': timedelta(minutes=30),
}


def validate_environment() -> None:
    """
    Validate that all required environment variables are set.
    Raises AirflowException if any required variables are missing.
    """
    required_vars = [
        'ALPHA_VANTAGE_API_KEY',
        'STOCK_DB_HOST',
        'STOCK_DB_NAME',
        'STOCK_DB_USER',
        'STOCK_DB_PASSWORD'
    ]
    
    missing_vars = []
    for var in required_vars: #type:ignore
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        raise AirflowException(error_msg)
    
    print("✓ All required environment variables are set") #type:ignore


def get_stock_symbols() -> List[str]:
    """
    Get the list of stock symbols from environment variable.
    
    Returns:
        List of stock symbols to process
    """
    symbols_str = os.getenv('STOCK_SYMBOLS', 'AAPL,GOOGL,MSFT,TSLA,AMZN')
    symbols = [s.strip().upper() for s in symbols_str.split(',') if s.strip()] #type:ignore
    
    if not symbols:
        raise AirflowException("No stock symbols configured in STOCK_SYMBOLS environment variable")
    
    print(f"Processing {len(symbols)} stock symbols: {', '.join(symbols)}")
    return symbols


def fetch_stock_data_task(**context) -> dict:
    """
    Airflow task to fetch and store stock data.
    
    Args:
        **context: Airflow context dictionary
        
    Returns:
        Dictionary with execution statistics
    """
    try:
        # Get symbols
        symbols = get_stock_symbols()
        
        # Fetch and store data
        stats = fetch_and_store_stock_data(symbols, **context)
        
        # Push stats to XCom for downstream tasks
        context['task_instance'].xcom_push(key='fetch_stats', value=stats)
        
        # Check if we should fail the task
        if stats['failed_fetches'] == stats['total_symbols']:
            raise AirflowException("All stock data fetches failed!")
        
        return stats
        
    except Exception as e: #type:ignore
        print(f"Error in fetch_stock_data_task: {str(e)}")
        raise AirflowException(f"Stock data fetch task failed: {str(e)}")


def validate_data_quality(**context) -> None:
    """
    Validate the quality of fetched data.
    
    Args:
        **context: Airflow context dictionary
    """
    try:
        # Pull stats from previous task
        task_instance = context['task_instance']
        stats = task_instance.xcom_pull(task_ids='fetch_stock_data', key='fetch_stats')
        
        if not stats:
            raise AirflowException("No statistics available from fetch task")
        
        # Validation checks
        total_symbols = stats.get('total_symbols', 0)
        successful_fetches = stats.get('successful_fetches', 0)
        total_records = stats.get('total_records_inserted', 0)
        
        print("Data Quality Validation Results:")
        print(f"  Total Symbols Processed: {total_symbols}")
        print(f"  Successful Fetches: {successful_fetches}")
        print(f"  Total Records Inserted: {total_records}")
        
        # Quality thresholds
        success_rate = (successful_fetches / total_symbols * 100) if total_symbols > 0 else 0
        
        if success_rate < 50:
            raise AirflowException(
                f"Data quality check failed: Success rate {success_rate:.1f}% is below 50% threshold"
            )
        
        if successful_fetches > 0 and total_records == 0:
            raise AirflowException("Data quality check failed: No records inserted despite successful fetches")
        
        print(f"✓ Data quality validation passed (success rate: {success_rate:.1f}%)")
        
    except AirflowException:
        raise
    except Exception as e:
        raise AirflowException(f"Data quality validation failed: {str(e)}")


def log_execution_summary(**context) -> None:
    """
    Log final execution summary.
    
    Args:
        **context: Airflow context dictionary
    """
    try:
        task_instance = context['task_instance']
        stats = task_instance.xcom_pull(task_ids='fetch_stock_data', key='fetch_stats')
        
        if not stats:
            print("No statistics available for summary")
            return
        
        print("=" * 60)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 60)
        print(f"Execution Date: {context['ds']}")
        print(f"Total Symbols: {stats.get('total_symbols', 0)}")
        print(f"Successful Fetches: {stats.get('successful_fetches', 0)}")
        print(f"Failed Fetches: {stats.get('failed_fetches', 0)}")
        print(f"Total Records Inserted: {stats.get('total_records_inserted', 0)}")
        
        errors = stats.get('errors', [])
        if errors:
            print(f"\nErrors ({len(errors)}):")
            for idx, error in enumerate(errors, 1):
                print(f"  {idx}. {error}")
        else:
            print("\n✓ No errors encountered")
        
        print("=" * 60)
        
    except Exception as e: #type:ignore
        print(f"Error logging summary: {str(e)}")


# Define the DAG
with DAG(
    dag_id='stock_market_data_pipeline',
    default_args=default_args,
    description='Fetches stock market data from Alpha Vantage and stores in PostgreSQL',
    schedule_interval='@hourly',  # Run every hour
    catchup=False,  # Don't run for past dates
    max_active_runs=1,  # Only one run at a time
    tags=['stock-market', 'data-pipeline', 'alpha-vantage'],
) as dag:
    
    # Task 1: Validate environment configuration
    validate_env_task = PythonOperator(
        task_id='validate_environment',
        python_callable=validate_environment,
        doc_md="""
        ### Validate Environment
        Validates that all required environment variables are properly configured:
        - ALPHA_VANTAGE_API_KEY
        - STOCK_DB_HOST
        - STOCK_DB_NAME
        - STOCK_DB_USER
        - STOCK_DB_PASSWORD
        """
    )
    
    # Task 2: Test database connectivity
    test_db_connection = BashOperator(
        task_id='test_database_connection',
        bash_command="""
        echo "Testing PostgreSQL connection..."
        python -c "
import psycopg2
import os
try:
    conn = psycopg2.connect(
        host=os.getenv('STOCK_DB_HOST'),
        port=os.getenv('STOCK_DB_PORT', 5432),
        dbname=os.getenv('STOCK_DB_NAME'),
        user=os.getenv('STOCK_DB_USER'),
        password=os.getenv('STOCK_DB_PASSWORD')
    )
    conn.close()
    print('✓ Database connection successful')
except Exception as e:
    print(f'✗ Database connection failed: {e}')
    exit(1)
"
        """,
        doc_md="""
        ### Test Database Connection
        Verifies that the PostgreSQL database is accessible and credentials are valid.
        """
    )
    
    # Task 3: Fetch and store stock data
    fetch_data_task = PythonOperator(
        task_id='fetch_stock_data',
        python_callable=fetch_stock_data_task,
        provide_context=True, #type:ignore
        doc_md="""
        ### Fetch Stock Data
        Main task that:
        1. Retrieves stock symbols from configuration
        2. Fetches data from Alpha Vantage API for each symbol
        3. Parses and validates the data
        4. Stores/updates records in PostgreSQL database
        
        Includes retry logic and comprehensive error handling.
        """
    )
    
    # Task 4: Validate data quality
    validate_quality_task = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
        provide_context=True,
        doc_md="""
        ### Validate Data Quality
        Performs quality checks on the fetched data:
        - Verifies success rate is above threshold (50%)
        - Ensures records were inserted if fetches succeeded
        - Logs quality metrics for monitoring
        """
    )
    
    # Task 5: Log execution summary
    log_summary_task = PythonOperator(
        task_id='log_execution_summary',
        python_callable=log_execution_summary,
        provide_context=True,
        trigger_rule='all_done',  # Run even if previous tasks fail
        doc_md="""
        ### Log Execution Summary
        Generates comprehensive execution summary including:
        - Total symbols processed
        - Success/failure counts
        - Records inserted
        - Any errors encountered
        """
    )
    
    # Define task dependencies
    validate_env_task >> test_db_connection >> fetch_data_task >> validate_quality_task >> log_summary_task


# DAG documentation
dag.doc_md = """
# Stock Market Data Pipeline

## Overview
This DAG orchestrates the automated fetching, processing, and storage of stock market data 
from Alpha Vantage API into a PostgreSQL database.

## Schedule
- **Frequency**: Hourly
- **Start Date**: 1 day ago
- **Catchup**: Disabled

## Data Flow
1. **Environment Validation**: Ensures all required configuration is present
2. **Database Connection Test**: Verifies PostgreSQL connectivity
3. **Data Fetching**: Retrieves stock data from Alpha Vantage API
4. **Data Quality Validation**: Validates the fetched and stored data
5. **Execution Summary**: Logs comprehensive pipeline execution statistics

## Configuration
The pipeline is configured via environment variables:
- `ALPHA_VANTAGE_API_KEY`: Your Alpha Vantage API key
- `STOCK_SYMBOLS`: Comma-separated list of stock symbols (e.g., "AAPL,GOOGL,MSFT")
- `STOCK_DB_HOST`: PostgreSQL host
- `STOCK_DB_NAME`: Database name
- `STOCK_DB_USER`: Database user
- `STOCK_DB_PASSWORD`: Database password

## Error Handling
- Automatic retries (3 attempts with 5-minute delay)
- Individual symbol failures don't fail the entire pipeline
- Comprehensive error logging
- Data quality validation with configurable thresholds

## Monitoring
- Task execution logs available in Airflow UI
- XCom variables store execution statistics
- Execution summary logged at completion
"""
