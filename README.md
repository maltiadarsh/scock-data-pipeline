# Stock Market Data Pipeline with Apache Airflow

A production-ready, Dockerized data pipeline that automatically fetches stock market data from Alpha Vantage API and stores it in PostgreSQL using Apache Airflow for orchestration.

## 🎯 Features

- **Automated Data Collection**: Hourly scheduled fetching of stock market data
- **Robust Error Handling**: Comprehensive retry logic and graceful failure management
- **Scalable Architecture**: Docker-based deployment with connection pooling
- **Data Quality Validation**: Automated data quality checks after each run
- **Production Ready**: Environment-based configuration and security best practices
- **Monitoring**: Detailed logging and execution statistics via Airflow UI

## 🚀 Quick Start

### 1. Clone and Navigate

```bash
cd d:\adarsh\assignment
```

### 2. Configure Environment Variables

Copy the example environment file and update with your credentials:

```bash
# On Windows PowerShell
Copy-Item .env.example .env
```

Edit `.env` file and update the following:

```env
# REQUIRED: Get your free API key from https://www.alphavantage.co/support/#api-key
ALPHA_VANTAGE_API_KEY=YOUR_ACTUAL_API_KEY_HERE

# Optional: Customize stock symbols (comma-separated)
STOCK_SYMBOLS=AAPL,GOOGL,MSFT,TSLA,AMZN

# Optional: Update Airflow credentials (set your own secure password)
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=<your_secure_password_here>
```

### 3. Build and Start the Pipeline

```bash
docker-compose up -d
```

This command will:
- Pull necessary Docker images
- Create PostgreSQL database with stock_data table
- Initialize Airflow database
- Create Airflow admin user
- Start Airflow webserver and scheduler

### 4. Access Airflow UI

1. Open browser and navigate to: `http://localhost:8080`
2. Login with credentials from `.env` file (default: `admin` / `admin123`)
3. Find the DAG: `stock_market_data_pipeline`
4. Toggle the DAG to "ON" to enable automatic scheduling

### 5. Trigger Manual Run (Optional)

Click the "Play" button (▶️) on the DAG to trigger an immediate run.

## 📁 Project Structure

```
assignment/
├── docker-compose.yml          # Docker orchestration configuration
├── .env                        # Environment variables (DO NOT COMMIT)
├── .env.example               # Example environment configuration
├── requirements.txt           # Python dependencies
├── README.md                  # This file
│
├── dags/
│   └── stock_market_dag.py    # Airflow DAG definition
│
├── scripts/
│   └── fetch_stock_data.py    # Stock data fetching logic
│
├── init_db/
│   └── init.sql              # Database initialization script
│
├── logs/                      # Airflow logs (auto-generated)
└── plugins/                   # Airflow plugins (optional)
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `ALPHA_VANTAGE_API_KEY` | Alpha Vantage API key | - | ✅ Yes |
| `STOCK_SYMBOLS` | Comma-separated stock symbols | AAPL,GOOGL,MSFT,TSLA,AMZN | ❌ No |
| `POSTGRES_USER` | PostgreSQL username | airflow | ❌ No |
| `POSTGRES_PASSWORD` | PostgreSQL password | airflow_password_123 | ❌ No |
| `POSTGRES_DB` | PostgreSQL database name | airflow | ❌ No |
| `AIRFLOW_USERNAME` | Airflow web UI username | admin | ❌ No |
| `AIRFLOW_PASSWORD` | Airflow web UI password | admin123 | ❌ No |

### Pipeline Schedule

- **Default**: Hourly (`@hourly`)
- **Modify**: Edit `schedule_interval` in `dags/stock_market_dag.py`

Available options:
- `@hourly` - Every hour
- `@daily` - Every day at midnight
- `0 */6 * * *` - Every 6 hours
- `0 9 * * 1-5` - Weekdays at 9 AM

### Stock Symbols

Add or modify symbols in `.env`:

```env
STOCK_SYMBOLS=AAPL,GOOGL,MSFT,TSLA,AMZN,NVDA,META,NFLX
```

## 📊 Database Schema

### `stock_data` Table

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key |
| `symbol` | VARCHAR(10) | Stock ticker symbol |
| `timestamp` | TIMESTAMP | Data point timestamp |
| `open_price` | NUMERIC(12,4) | Opening price |
| `high_price` | NUMERIC(12,4) | Highest price |
| `low_price` | NUMERIC(12,4) | Lowest price |
| `close_price` | NUMERIC(12,4) | Closing price |
| `volume` | BIGINT | Trading volume |
| `created_at` | TIMESTAMP | Record creation time |
| `updated_at` | TIMESTAMP | Last update time |

### Views

- `latest_stock_prices`: Latest price for each symbol
- `daily_stock_stats`: Daily aggregated statistics

## 🔍 Monitoring and Logs

### Airflow UI

- **URL**: http://localhost:8080
- **Features**:
  - View DAG runs and task status
  - Access logs for each task
  - Monitor execution times
  - View XCom variables (execution statistics)

### Docker Logs

View real-time logs:

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
docker-compose logs -f postgres
```

## 🗃️ Database Access

### Using psql (PostgreSQL CLI)

```bash
docker exec -it stock_pipeline_postgres psql -U airflow -d airflow
```

### Sample Queries

```sql
-- View latest stock prices
SELECT * FROM latest_stock_prices;

-- Count records per symbol
SELECT symbol, COUNT(*) as record_count 
FROM stock_data 
GROUP BY symbol 
ORDER BY record_count DESC;

-- View recent records
SELECT * FROM stock_data 
ORDER BY timestamp DESC 
LIMIT 10;

-- Daily statistics
SELECT * FROM daily_stock_stats 
WHERE symbol = 'AAPL' 
ORDER BY trade_date DESC 
LIMIT 7;
```

## 🛠️ Maintenance

### Stop the Pipeline

```bash
docker-compose down
```

### Stop and Remove All Data

```bash
docker-compose down -v
```

### Restart Services

```bash
docker-compose restart
```

### Update Dependencies

1. Modify `requirements.txt`
2. Rebuild containers:

```bash
docker-compose down
docker-compose up -d --build
```

### View Service Status

```bash
docker-compose ps
```

## 🐛 Troubleshooting

### Issue: "API Rate Limit Exceeded"

**Cause**: Alpha Vantage free tier limits: 5 calls/minute, 500 calls/day

**Solution**:
- Reduce number of symbols in `STOCK_SYMBOLS`
- Increase schedule interval (e.g., from hourly to every 4 hours)
- Upgrade to paid Alpha Vantage plan

### Issue: "Database Connection Failed"

**Solution**:
```bash
# Check if PostgreSQL is running
docker-compose ps postgres

# View PostgreSQL logs
docker-compose logs postgres

# Restart PostgreSQL
docker-compose restart postgres
```

### Issue: "DAG Not Appearing in Airflow UI"

**Solution**:
```bash
# Check scheduler logs
docker-compose logs airflow-scheduler

# Verify DAG file syntax
docker exec airflow_scheduler python /opt/airflow/dags/stock_market_dag.py

# Restart scheduler
docker-compose restart airflow-scheduler
```

### Issue: "Permission Denied" on Windows

**Solution**:
```bash
# Run PowerShell as Administrator
# Check Docker Desktop is running
# Ensure drives are shared in Docker Desktop settings
```

## 📈 Scaling Considerations

### Increase Symbols

The pipeline includes rate limiting (12 seconds between calls) to respect API limits:

```python
# In fetch_stock_data.py
time.sleep(12)  # Adjust if needed
```

### Database Performance

For large datasets, consider:
- Partitioning `stock_data` table by date
- Archiving old data
- Adding additional indexes

### Airflow Executor

For production, consider upgrading from LocalExecutor to:
- **CeleryExecutor**: Distributed task execution
- **KubernetesExecutor**: Dynamic scaling on Kubernetes

## 🔐 Security Best Practices

1. **Never commit `.env` file** to version control
2. **Use strong passwords** in production
3. **Rotate API keys** regularly
4. **Enable Airflow authentication** (already configured)
5. **Use secrets management** for production (AWS Secrets Manager, HashiCorp Vault)

## 📝 API Rate Limits

### Alpha Vantage Free Tier

- **5 API calls per minute**
- **500 API calls per day**

### Pipeline Rate Limiting

The pipeline automatically waits 12 seconds between API calls to stay within rate limits:

- 5 symbols = ~1 minute per run
- 10 symbols = ~2 minutes per run

## 🧪 Testing

### Test API Connection

```bash
docker exec airflow_scheduler python -c "
import os
import requests
api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
response = requests.get(f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=AAPL&interval=60min&apikey={api_key}')
print(response.json())
"
```

### Test Database Connection

```bash
docker exec airflow_scheduler python -c "
import psycopg2
import os
conn = psycopg2.connect(
    host='postgres',
    dbname=os.getenv('STOCK_DB_NAME'),
    user=os.getenv('STOCK_DB_USER'),
    password=os.getenv('STOCK_DB_PASSWORD')
)
print('Database connection successful!')
conn.close()
"
```

### Run Script Manually

```bash
docker exec airflow_scheduler python /opt/airflow/scripts/fetch_stock_data.py
```

## 📦 Deliverables Checklist

- ✅ `docker-compose.yml` - Docker Compose configuration
- ✅ `dags/stock_market_dag.py` - Airflow DAG with scheduling
- ✅ `scripts/fetch_stock_data.py` - Data fetching script
- ✅ `init_db/init.sql` - Database schema initialization
- ✅ `requirements.txt` - Python dependencies
- ✅ `.env.example` - Environment variable template
- ✅ `README.md` - Comprehensive documentation

## 🎓 Assignment Compliance

### Requirements Met

| Requirement | Implementation | Status |
|------------|----------------|--------|
| Docker Compose deployment | `docker-compose.yml` with multi-service setup | ✅ |
| Airflow orchestration | DAG with 5 tasks, hourly schedule | ✅ |
| API data fetching | Alpha Vantage integration with retry logic | ✅ |
| JSON parsing | Comprehensive parsing with validation | ✅ |
| PostgreSQL storage | Table with indexes and views | ✅ |
| Error handling | Try-except blocks, retry logic, graceful failures | ✅ |
| Environment variables | All credentials in .env file | ✅ |
| Scalability | Connection pooling, rate limiting, modular design | ✅ |
| Code quality | Well-documented, type hints, logging | ✅ |

## 📞 Support

For issues or questions:

1. Check the [Troubleshooting](#-troubleshooting) section
2. Review Airflow logs: `docker-compose logs airflow-scheduler`
3. Check database logs: `docker-compose logs postgres`

## 📄 License

This project is created for educational purposes as part of a data engineering assignment.

## 🙏 Acknowledgments

- [Alpha Vantage](https://www.alphavantage.co/) for free stock market API
- [Apache Airflow](https://airflow.apache.org/) for workflow orchestration
- [PostgreSQL](https://www.postgresql.org/) for reliable data storage

---

