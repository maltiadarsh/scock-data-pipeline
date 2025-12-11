# Stock Market Data Pipeline - Quick Start Guide

## 🚀 Get Started in 3 Steps

### Step 1: Get API Key (2 minutes)
1. Visit https://www.alphavantage.co/support/#api-key
2. Enter your email
3. Copy the API key sent to your email

### Step 2: Configure (1 minute)
1. Open `.env` file in this directory
2. Replace `YOUR_API_KEY_HERE` with your actual API key:
   ```
   ALPHA_VANTAGE_API_KEY=YOUR_ACTUAL_KEY
   ```
3. Save the file

### Step 3: Launch (5 minutes)
Open PowerShell in this directory and run:
```powershell
docker-compose up -d
```

Wait ~3-5 minutes for initialization, then open: http://localhost:8080

**Login Credentials:**
- Username: `admin`
- Password: `admin123` #type:ignore

## ✅ Verify It's Working

1. In Airflow UI, find `stock_market_data_pipeline`
2. Toggle the DAG switch to "ON"
3. Click the play button (▶️) to trigger a manual run
4. Watch the task progress turn green!

## 📊 View Your Data

Connect to PostgreSQL:
```powershell
docker exec -it stock_pipeline_postgres psql -U airflow -d airflow
```

Query the data:
```sql
SELECT * FROM latest_stock_prices;
```

## 🛑 Stop the Pipeline

```powershell
docker-compose down
```

---

For detailed documentation, see [README.md](README.md)
