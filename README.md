# Retail Analytics Pipeline

A project that simulates a client-ready retail analytics data pipeline for batch-style ingestion, transformation, and downstream analytics use cases.

## Project Goal
Build a practical data engineering project that demonstrates:
- raw data ingestion
- ETL transformation
- clean processed datasets
- an interview-ready project structure
- a foundation for later analytics, data quality checks, and ML/API extensions

## Current Features
- Generates raw retail order events into JSONL
- Transforms raw orders into cleaned processed data
- Stores raw and processed data in separate pipeline layers

## Project Structure

```
RetailAnalyticsPipeline/
├── data
│   ├── processed
│   │   ├── retail_kpis.csv
│   │   ├── retail_orders_clean.jsonl
│   │   └── retail_order_summary.json
│   ├── raw
│   │   └── retail_orders.jsonl
│   └── warehouse
│       └── retail.duckdb
├── sql
│   ├── analytics
│   │   ├── daily_revenue_trend.sql
│   │   ├── kpi_summary.sql
│   │   ├── orders_by_status.sql
│   │   ├── revenue_by_customer_segment.sql
│   │   ├── revenue_by_region.sql
│   │   ├── revenue_by_weekday.sql
│   │   └── top_categories.sql
│   └── models
├── src
│   ├── dashboard
│   │   └── app.py
│   ├── etl
│   │   ├── aggregate_retail_orders.py
│   │   ├── create_dim_customers_table.py
│   │   ├── create_dim_date_table.py
│   │   ├── create_fact_orders_table.py
│   │   ├── export_retail_kpis.py
│   │   ├── __init__.py
│   │   ├── query_retail_kpis.py
│   │   ├── transform_retail_orders.py
│   │   └── validate_retail_orders.py
│   ├── ingestion
│   │   └── generate_retail_data.py
│   ├── __init__.py
│   └── run_retail_pipeline.py
├── tests
│   ├── etl
│   │   └── test_transform_retail_orders.py
│   └── conftest.py
├── Makefile
├── README.md
└── requirements.txt
```

## Pipeline Flow

```
Generate Raw Orders
        ↓
Write raw events to data/raw/orders.jsonl
        ↓
Transform raw orders
        ↓
Write cleaned data to data/processed/orders_clean.jsonl
        ↓
Validate cleaned data
        ↓
Count/flag bad records
        ↓
Aggregate cleaned data
        ↓
Write business summary to data/processed/order_summary.json
```

## Current Pipeline Versions

### V1 Prototype Pipeline
- Generates simple raw order events
- Transforms records into a cleaned processed layer
- Validates order data
- Produces a basic order summary

### V2 Retail Analytics Pipeline
- Generates richer retail order events with category, region, payment method, and order status
- Transforms raw retail orders into a cleaned analytics-ready dataset
- Validates business rules and field consistency
- Produces revenue and order summaries by category, status, and region
- Runs end-to-end through `src/run_retail_pipeline.py`
