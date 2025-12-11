# Architecture Overview

## System Components

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Compose                          │
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌─────────────┐  │
│  │   Airflow    │    │   Airflow    │    │             │  │
│  │  Webserver   │◄───┤  Scheduler   │◄───┤ PostgreSQL  │  │
│  │  (Port 8080) │    │              │    │  Database   │  │
│  └──────┬───────┘    └──────┬───────┘    │ (Port 5432) │  │
│         │                   │            └─────────────┘  │
│         │                   │                             │
│         └───────────┬───────┘                             │
│                     │                                      │
│         ┌───────────▼────────────┐                        │
│         │   Stock Data Script    │                        │
│         │  (fetch_stock_data.py) │                        │
│         └───────────┬────────────┘                        │
│                     │                                      │
└─────────────────────┼──────────────────────────────────────┘
                      │
                      │ HTTPS
                      ▼
          ┌───────────────────────┐
          │   Alpha Vantage API   │
          │  (Stock Market Data)  │
          └───────────────────────┘
```

## Data Flow

1. **Scheduler** triggers the DAG hourly
2. **Validation** checks environment and database connectivity
3. **Fetcher** calls Alpha Vantage API for each stock symbol
4. **Parser** extracts and validates stock data
5. **Database** stores/updates records in PostgreSQL
6. **Quality Check** validates data integrity
7. **Summary** logs execution statistics

## Database Schema

```
stock_data
├── id (PK)
├── symbol
├── timestamp
├── open_price
├── high_price
├── low_price
├── close_price
├── volume
├── created_at
└── updated_at

Indexes:
- idx_stock_symbol
- idx_stock_timestamp
- idx_stock_symbol_timestamp
```

## Error Handling Strategy

### API Level
- Retry with exponential backoff (3 attempts)
- Rate limiting (12 seconds between calls)
- Timeout handling (30 seconds)

### DAG Level
- Task-level retries (3 attempts, 5-minute delay)
- Individual symbol failures don't fail entire run
- Quality validation with configurable thresholds

### Database Level
- Connection pooling (1-10 connections)
- Upsert operations (INSERT ON CONFLICT)
- Transaction management with rollback

## Scalability Considerations

### Current Design
- **LocalExecutor**: Single-machine execution
- **Connection Pool**: 1-10 database connections
- **Rate Limiting**: API-friendly throttling

### Production Scaling
- **CeleryExecutor**: Distributed workers
- **Database Partitioning**: Time-based partitions
- **Caching Layer**: Redis for frequently accessed data
- **Load Balancer**: Multiple Airflow instances
