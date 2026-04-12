from __future__ import annotations

import os
import sys

from google.cloud import bigquery
from src.common.log_utils import get_logger, log_event

logger = get_logger(__name__)

def require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value


def main() -> int:
    try:
        project_id = require_env("GCP_PROJECT_ID")
        dataset_id = require_env("BIGQUERY_DATASET")
        table_name = os.getenv("BIGQUERY_TABLE", "fact_orders")

        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_name}"

        query = f"""
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT order_id) AS distinct_order_ids,
            MIN(order_date) AS min_order_date,
            MAX(order_date) AS max_order_date,
            ROUND(SUM(total_amount), 2) AS total_revenue
        FROM `{table_ref}`
        """

        result = client.query(query).result()
        row = next(result)

        log_event(
            logger,
            "bigquery_verification_result",
            table=table_ref,
            row_count=row.row_count,
            distinct_order_ids=row.distinct_order_ids,
            min_order_date=row.min_order_date,
            max_order_date=row.max_order_date,
            total_revenue=row.total_revenue,
        )

        if row.row_count == 0:
            raise ValueError("BigQuery table is empty.")

        if row.row_count != row.distinct_order_ids:
            raise ValueError("order_id is not unique in BigQuery table.")

        log_event(logger, "bigquery_verification_complete", table=table_ref, success=True)
        return 0

    except Exception as exc:
        logger.error("bigquery_verification_failed | error=%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())