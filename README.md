# *Retail Analytics Pipeline* ![CI](https://github.com/paulbylina/RetailAnalyticsPipeline/actions/workflows/ci.yml/badge.svg)

## Overview
RetailAnalyticsPipeline is an end-to-end retail data engineering project that demonstrates batch processing, streaming ingestion, cloud loading, warehouse modeling, and analytics delivery. The project includes Python and SQL transformations, a local DuckDB warehouse, an optional BigQuery target, a Redpanda-based streaming pipeline, a PySpark transformation path, a dbt warehouse layer, automated testing, structured logging, and a Streamlit dashboard.

## Current Scope
- Batch pipeline
- Local warehouse in DuckDB
- Airflow orchestration
- Docker / Docker Compose
- GitHub Actions CI
- Streamlit dashboard
- Optional BigQuery warehouse target for `fact_orders`
- BigQuery verification script
- Local streaming pipeline using Redpanda
- Python producer publishing retail order events to a Kafka-compatible topic
- Python consumer loading streamed events into DuckDB
- Streaming smoke test for `retail_order_events`
- PySpark transformation path for curated `fact_orders`
- Curated Spark Parquet output at `data/curated/fact_orders_spark.parquet`
- Spark smoke test for curated output
- dbt warehouse layer targeting DuckDB
- staging model: `stg_fact_orders`
- mart model: `customer_order_summary`
- dbt schema tests for staging and mart models


## Tech Stack
- Python
- SQL
- PySpark
- DuckDB
- BigQuery
- dbt
- Pandas
- NumPy
- Streamlit
- Altair
- Pytest
- Great Expectations
- Docker
- Docker Compose
- Kubernetes
- Redpanda
- GitHub Actions
- Apache Airflow
- Makefile


## Architecture
The project includes multiple data-engineering paths built around the same retail dataset:

1. **Batch pipeline**
   - Generate synthetic retail order data
   - Transform and validate orders with Python
   - Load warehouse tables into DuckDB
   - Serve analytics through Streamlit

2. **Cloud warehouse path**
   - Load `fact_orders` from DuckDB into BigQuery
   - Verify cloud-side row counts and key metrics

3. **Streaming pipeline**
   - Publish retail order events with a Python producer
   - Route events through Redpanda
   - Consume events into DuckDB with a Python consumer

4. **Spark pipeline**
   - Transform cleaned retail orders with PySpark
   - Write curated Parquet output for `fact_orders`

5. **dbt warehouse layer**
   - Define DuckDB warehouse sources
   - Build staging and mart models
   - Run dbt schema tests

6. **Operational layer**
   - Airflow DAG for orchestration
   - GitHub Actions for CI
   - Great Expectations and pytest for validation
   - Docker for containerization
   - Kubernetes for local dashboard deployment
   

## Project Structure
```text
RetailAnalyticsPipeline/
├── airflow/
│   └── dags/
├── data/
│   ├── processed/
│   ├── curated/
│   └── warehouse/
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── dbt_project.yml
│   └── profiles.yml
├── k8s/
├── src/
│   ├── common/
│   ├── etl/
│   ├── ingestion/
│   ├── load/
│   ├── quality/
│   └── streaming/
├── tests/
│   ├── etl/
│   └── ...
├── .github/
│   └── workflows/
├── Makefile
├── pytest.ini
├── docker-compose.yml
├── Dockerfile
└── README.md
```


## How to Run
#### 1. Clone the repository
```bash
git clone https://github.com/paulbylina/RetailAnalyticsPipeline.git
cd RetailAnalyticsPipeline
```

#### 2. Create and activate a virtual environment
```bash
python -m venv .venv
source .venv/bin/activate
```

#### 3. Install dependencies
```bash
pip install -r requirements.txt
```

Or with Make:

```bash
make install
```

## Running the Pipeline
#### 1. Generate raw retail data
```bash
python -m src.ingestion.generate_retail_data
```

#### 2a. Transform and validate the data
```bash
python -m src.etl.transform_retail_orders
python -m src.etl.validate_retail_orders
python -m src.etl.aggregate_retail_orders
```
#### 2b. Run the PySpark transform
```bash
make pyspark-transform
```

#### 3. Build the DuckDB warehouse tables
```bash
python -m src.etl.create_fact_orders_table
python -m src.etl.create_dim_date_table
python -m src.etl.create_dim_customers_table
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

![Airflow DAG](docs/images/airflow-dag.png)

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

## BigQuery Cloud Target
This project includes an optional BigQuery warehouse target for the `fact_orders` table.

### Prerequisites
- Google Cloud project
- BigQuery API enabled
- Local Application Default Credentials configured with `gcloud auth application-default login`

### Environment Variables
```bash
export GCP_PROJECT_ID="retail-analytics-pipeline-01"
export BIGQUERY_DATASET="retail_analytics"
```

### Load fact_orders to BigQuery
```bash
python -m src.load.load_fact_orders_to_bigquery
```
This reads the local DuckDB ```fact_orders```table and loads it into BigQuery.

### Verify the BigQuery table
```bash
python -m src.load.verify_fact_orders_in_bigquery
```
This runs a smoke test against the BigQuery table and checks:
- row count
- distinct order_id count
- min/max order_date
- total revenue

### Current BigQuery Scope
Implemented:
- create dataset if it does not exist
- load fact_orders from DuckDB to BigQuery
- verify the loaded table with a cloud-side query

Planned:
- load dimension tables
- orchestrate cloud load with Airflow
- add BigQuery data quality assertions in CI

## CI / Automation
This project includes two GitHub Actions workflows:

- **CI workflow**: runs automated tests on pushes and pull requests
- **Retail pipeline workflow**: supports manual runs and scheduled pipeline execution

The pipeline workflow also uploads generated outputs as workflow artifacts so results can be inspected from GitHub Actions.

## Streaming Pipeline
This project includes a local streaming path built with Redpanda.

Flow:

Clean retail order events (JSONL)
→ Python producer
→ Redpanda topic (`retail-orders`)
→ Python consumer
→ DuckDB landing table (`retail_order_events`)

### Components
- **Broker:** Redpanda
- **Topic:** `retail-orders`
- **Producer:** `src/streaming/produce_retail_orders.py`
- **Consumer:** `src/streaming/consume_retail_orders_to_duckdb.py`
- **Landing table:** `retail_order_events` in DuckDB

### Start Redpanda
```bash
docker compose up -d redpanda redpanda-console
```

### Create the topic
```bash
docker exec -it redpanda rpk topic create retail-orders -p 1
```

### Produce events
```bash
python -m src.streaming.produce_retail_orders
```

### Consume events into DuckDB
```bash
python -m src.streaming.consume_retail_orders_to_duckdb
```

### Run the streaming smoke test
```bash
pytest tests/test_streaming_events_smoke.py -v
```
This streaming path demonstrates event-driven ingestion alongside the project’s existing batch pipeline.

### Test suites
- `pytest -m etl -v` → ETL/unit-style tests
- `pytest -m warehouse -v` → local DuckDB warehouse tests
- `pytest -m integration -v` → cloud integration tests (requires GCP credentials and environment variables)

Note: BigQuery integration tests are expected to skip in CI unless GCP credentials are configured.


## Spark Pipeline
This project includes a PySpark transformation path that builds a curated `fact_orders` dataset from cleaned retail order events.

#### Run the Spark transformation
```bash
python -m src.etl.run_spark_fact_orders
```
This reads data/processed/retail_orders_clean.jsonl and writes curated Parquet output to:
- data/curated/fact_orders_spark.parquet

#### Validate the Spark output
```bash
pytest tests/test_spark_fact_orders_smoke.py -v
```
This verifies:
- the Spark Parquet output exists
- the dataset has rows
- **order_id** remains unique
- **total_amount** is non-negative

## Data Quality and Observability
This project includes multiple layers of data-quality validation and operational visibility.

#### Data Quality
- **Pytest ETL tests** for transformation logic
- **DuckDB warehouse smoke tests** for `fact_orders`
- **Streaming smoke tests** for `retail_order_events`
- **Spark smoke tests** for curated Parquet output
- **Great Expectations validation** for `fact_orders`
- **BigQuery integration test** for cloud warehouse verification

#### Validation Commands
```bash
make test-etl
make test-warehouse
make test-streaming
make test-spark
make validate-gx
make verify-bigquery
```

#### Observability
Core pipeline scripts now emit structured logs for:
- BigQuery load and verification
- streaming producer and consumer flows
- Spark fact table generation
- Great Expectations validation

This makes pipeline runs easier to debug and gives the project a more production-style operational story.

## Kubernetes Deployment
This project includes a local Kubernetes deployment path for the Streamlit dashboard.

#### Kubernetes Manifests
- `k8s/dashboard-deployment.yaml`
- `k8s/dashboard-service.yaml`

#### Build the dashboard image
```bash
docker build -t retail-analytics-dashboard:latest .
```

#### Create a local cluster
```bash
kind create cluster
kind load docker-image retail-analytics-dashboard:latest
```

#### Deploy to Kubernetes
```bash
kubectl apply -f k8s/dashboard-deployment.yaml
kubectl apply -f k8s/dashboard-service.yaml
kubectl rollout status deployment/retail-dashboard --timeout=5m
```

#### Access the dashboard
```bash
kubectl port-forward service/retail-dashboard-service 8501:8501
```
Then open:
```bash
http://localhost:8501
```

## dbt Warehouse Layer
This project includes a dbt layer on top of the local DuckDB warehouse.

#### dbt Models
- **Source:** `fact_orders`
- **Staging model:** `stg_fact_orders`
- **Mart model:** `customer_order_summary`

#### dbt Commands
```bash
make dbt-debug
make dbt-run
make dbt-test
```

#### What the dbt layer adds
- source-to-model lineage
- modular SQL transformations
- staging and mart separation
- dbt-native schema tests for null and uniqueness checks


## Dashboard Preview
![Retail Analytics Dashboard](docs/images/dashboard.png)