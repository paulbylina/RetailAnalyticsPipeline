# Databricks / GCP Architecture Mapping

This project is implemented locally using Python, DuckDB, SQL, Streamlit, Docker, GitHub Actions, and Airflow.

For a Databricks + GCP environment, the architecture would map as follows:

## Current Project → Databricks / GCP Mapping

- `data/raw/`
  - Current: local raw data layer
  - Databricks / GCP equivalent: Google Cloud Storage (GCS) raw landing zone

- `data/processed/`
  - Current: cleaned / transformed local outputs
  - Databricks / GCP equivalent: Delta Lake silver layer on Databricks or processed data in GCS

- `data/warehouse/retail.duckdb`
  - Current: local analytics warehouse
  - Databricks / GCP equivalent: BigQuery warehouse tables or Delta Lake gold layer

- `src/etl/transform_retail_orders.py`
  - Current: Python-based transformation script
  - Databricks / GCP equivalent: PySpark transformation job in Databricks

- `sql/analytics/*.sql`
  - Current: business analytics queries
  - Databricks / GCP equivalent: Databricks SQL or BigQuery SQL

- `src/dashboard/app.py`
  - Current: Streamlit dashboard
  - Databricks / GCP equivalent: BI/dashboard layer connected to BigQuery or Databricks SQL

- `airflow/dags/retail_pipeline_dag.py`
  - Current: local Airflow DAG
  - Databricks / GCP equivalent: Cloud Composer or Databricks Workflows

- GitHub Actions
  - Current: CI and scheduled workflow automation
  - Databricks / GCP equivalent: CI/CD pipeline integrated with cloud deployment workflows

## Distributed Processing Upgrade Path

If this project were extended for distributed processing, the most natural next step would be:

1. Replace selected Python ETL steps with PySpark jobs
2. Write transformed outputs as Parquet / Delta-style datasets
3. Load curated analytics tables into BigQuery
4. Orchestrate the workflow with Airflow / Cloud Composer / Databricks Workflows
5. Add cloud storage and IAM-aware deployment patterns

## Why This Matters

This project demonstrates the core data engineering concepts behind Databricks and GCP workflows:

- ETL / ELT pipeline design
- dimensional modeling
- analytics-ready data outputs
- workflow orchestration
- automated validation
- CI/CD
- dashboard delivery

The main difference is the execution environment:
- current version: local / containerized
- target enterprise version: distributed / cloud-native