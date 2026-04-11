from __future__ import annotations

import os

import pytest
from google.cloud import bigquery


def require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        pytest.skip(f"Missing required environment variable: {var_name}")
    return value


@pytest.mark.integration
def test_bigquery_fact_orders_smoke():
    project_id = require_env("GCP_PROJECT_ID")
    dataset_id = require_env("BIGQUERY_DATASET")
    table_name = os.getenv("BIGQUERY_TABLE", "fact_orders")

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_name}"

    query = f"""
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT order_id) AS distinct_order_ids
    FROM `{table_ref}`
    """

    result = client.query(query).result()
    row = next(result)

    assert row.row_count > 0, "BigQuery table is empty"
    assert row.row_count == row.distinct_order_ids, "order_id is not unique"