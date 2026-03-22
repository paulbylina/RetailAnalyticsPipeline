# RetailAnalyticsPipeline

A portfolio project that simulates a client-ready retail analytics data pipeline for batch-style ingestion, transformation, and downstream analytics use cases.

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
```text
RetailAnalyticsPipeline/
├── data/
│   ├── raw/
│   └── processed/
├── src/
│   ├── ingestion/
│   ├── etl/
│   ├── models/
│   └── api/
├── notebooks/
├── infra/
├── tests/
├── README.md
└── requirements.txt