# Retail Analytics Pipeline

![CI](https://github.com/paulbylina/RetailAnalyticsPipeline/actions/workflows/ci.yml/badge.svg)

## Overview

This project demonstrates an end-to-end batch analytics pipeline using Python, DuckDB, SQL, and Streamlit.

It starts with synthetic retail order events, transforms and validates them, loads them into a local DuckDB warehouse, models them into analytics tables, and exposes business insights through SQL queries and a dashboard UI.

## Dashboard Preview

![Retail Analytics Dashboard](docs/dashboard-screenshot.png)

## Architecture

The project follows a layered data engineering workflow:

```text
Raw Data Generation
        ↓
Processed / Cleaned Data
        ↓
DuckDB Warehouse
        ├── fact_orders
        ├── dim_date
        └── dim_customers
        ↓
Analytics SQL Queries
        ↓
Streamlit Dashboard

Automation / Orchestration
- GitHub Actions CI for automated test runs
- GitHub Actions pipeline workflow for scheduled/manual execution
- Airflow DAG for task orchestration
- Docker / Docker Compose for containerized local execution
```

## Stack

- Python
- SQL
- DuckDB
- Pandas
- NumPy
- Streamlit
- Altair
- Pytest
- Docker
- Docker Compose
- GitHub Actions
- Apache Airflow
- Makefile

## Project Goals

This project is designed to demonstrate:

- batch data ingestion and generation
- ETL transformation and validation
- warehouse-style modeling with fact and dimension tables
- SQL-based analytics for business reporting
- dashboard delivery through Streamlit
- automated testing and CI with GitHub Actions
- containerized local execution with Docker and Docker Compose
- workflow orchestration with Apache Airflow
- a clean data engineering project structure

## Project Structure

```text
RetailAnalyticsPipeline/
├── airflow
│   └── dags
│       └── retail_pipeline_dag.py
├── data
│   ├── processed
│   ├── raw
│   └── warehouse
├── docs
│   ├── airflow-dag.png
│   └── dashboard-screenshot.png
├── sql
│   └── analytics
│       ├── daily_revenue_trend.sql
│       ├── kpi_summary.sql
│       ├── orders_by_status.sql
│       ├── revenue_by_customer_segment.sql
│       ├── revenue_by_region.sql
│       ├── revenue_by_weekday.sql
│       └── top_categories.sql
├── src
│   ├── __init__.py
│   ├── dashboard
│   │   └── app.py
│   ├── etl
│   │   ├── __init__.py
│   │   ├── aggregate_retail_orders.py
│   │   ├── create_dim_customers_table.py
│   │   ├── create_dim_date_table.py
│   │   ├── create_fact_orders_table.py
│   │   ├── export_retail_kpis.py
│   │   ├── query_retail_kpis.py
│   │   ├── transform_retail_orders.py
│   │   └── validate_retail_orders.py
│   ├── ingestion
│   │   └── generate_retail_data.py
│   └── run_retail_pipeline.py
├── tests
│   ├── etl
│   │   └── test_transform_retail_orders.py
│   ├── conftest.py
│   └── test_warehouse_smoke.py
├── docker-compose.airflow.yml
├── docker-compose.yml
├── Dockerfile
├── Makefile
├── README.md
└── requirements.txt
```
* Generated pipeline outputs are written to the `data/raw`, `data/processed`, and `data/warehouse` directories when the pipeline runs.

## Warehouse Model

The DuckDB warehouse contains a small star-schema-style layout:

### fact_orders
The central transaction table containing one row per retail order.

Example fields:
- order_id
- customer_id
- product_id
- order_date
- category
- region
- payment_method
- order_status
- quantity
- unit_price
- total_amount

### dim_date
A calendar dimension used for time-based analysis.

Example fields:
- date_key
- full_date
- year
- month
- month_name
- day
- weekday_name
- weekend_flag

### dim_customers
A customer lookup dimension used for segmentation analysis.

Example fields:
- customer_id
- customer_segment

## Analytics Questions Answered

The SQL analytics layer answers questions like:

- How many total orders were placed?
- What is total revenue and average order value?
- Which regions generate the most revenue?
- Which product categories perform best?
- How are orders distributed by status?
- How is revenue trending over time?
- How do customer segments perform?
- Which weekdays generate the most revenue?

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/paulbylina/RetailAnalyticsPipeline.git
cd RetailAnalyticsPipeline
```

### 2. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

Or with Make:

```bash
make install
```

## Running the Pipeline

### 1. Generate raw retail data

```bash
python src/ingestion/generate_retail_data.py
```

### 2. Transform and validate the data

```bash
python src/etl/transform_retail_orders.py
python src/etl/validate_retail_orders.py
python src/etl/aggregate_retail_orders.py
```

### 3. Build the DuckDB warehouse tables

```bash
python src/etl/create_fact_orders_table.py
python src/etl/create_dim_date_table.py
python src/etl/create_dim_customers_table.py
```

Or with Make:

```bash
make create-fact-orders-table
make create-dim-date-table
make create-dim-customers-table
```

## Running with Docker
### Recommended: Docker Compose
```bash
docker compose up
```

Or with Make:

```bash
make docker-run
```
## Manual Docker run
### Build the image

```bash
docker build -t retail-analytics-pipeline .
```
### Run the container:
```bash
docker run -p 8501:8501 retail-analytics-pipeline
```

## Running Analytics Queries

Example:

```bash
make kpi-summary-df
make revenue-by-region-df
make top-categories-df
make orders-by-status
make daily-revenue-trend
make revenue-by-customer-segment
make revenue-by-weekday
```

## Streamlit Dashboard

### Start with Bash

```bash
streamlit run src/dashboard/app.py
```

### Start with Make

```bash
make streamlit
```

The dashboard includes:
- KPI summary cards
- revenue by region
- top categories
- daily revenue trend
- customer segment performance
- weekday revenue analysis

## Airflow Orchestration

This project also includes an Airflow DAG for orchestrating the retail pipeline.

![Airflow DAG](docs/airflow-dag.png)

### Start Airflow:

```bash
docker compose -f docker-compose.airflow.yml up
```

or:
```bash
make airflow-up
```

UI will be available at:
[http://localhost:8080](http://localhost:8080)


### Login credentials:
```text
username: admin
password: admin123
```

### Shutdown
```bash
docker compose -f docker-compose.airflow.yml down
```

or:
```bash
make airflow-down
```

## DAG Included
- retail_analytics_pipeline
### This DAG orchestrates:
- raw retail data generation
- transformation
- validation
- aggregation
- fact table creation
- date dimension creation
- customer dimension creation

## CI / Automation

This project includes two GitHub Actions workflows:

- **CI workflow**: runs automated tests on pushes and pull requests
- **Retail pipeline workflow**: supports manual runs and scheduled pipeline execution

The pipeline workflow also uploads generated outputs as workflow artifacts so results can be inspected from GitHub Actions.

## Testing

Run tests with:

```bash
pytest
```

## Future Improvements
- dbt-style SQL models
- more tests
  - validation tests
  - SQL smoke tests
  - dashboard smoke test
- dashboard filters and richer interactivity
- cloud deployment