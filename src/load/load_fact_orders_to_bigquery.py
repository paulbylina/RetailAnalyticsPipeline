from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DUCKDB_PATH = PROJECT_ROOT / "data" / "warehouse" / "retail.duckdb"

DUCKDB_TABLE = os.getenv("DUCKDB_TABLE", "fact_orders")
BQ_TABLE = os.getenv("BIGQUERY_TABLE", "fact_orders")
BQ_LOCATION = os.getenv("BIGQUERY_LOCATION", "US")
WRITE_DISPOSITION = os.getenv("BIGQUERY_WRITE_DISPOSITION", "WRITE_TRUNCATE")


def require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value


def ensure_duckdb_table_exists(db_path: Path, table_name: str) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB database not found: {db_path}")

    with duckdb.connect(str(db_path)) as conn:
        result = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchone()

    if not result or result[0] == 0:
        raise ValueError(f"Table '{table_name}' not found in DuckDB database: {db_path}")


def read_fact_orders_from_duckdb(db_path: Path, table_name: str) -> pd.DataFrame:
    with duckdb.connect(str(db_path)) as conn:
        df = conn.execute(f"SELECT * FROM {table_name}").df()

    if df.empty:
        raise ValueError(f"Table '{table_name}' is empty. Nothing to load.")

    for col in ("order_date", "created_at", "updated_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def ensure_bigquery_dataset(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    location: str,
) -> None:
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = location

    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset exists: {project_id}.{dataset_id}")
    except NotFound:
        client.create_dataset(dataset_ref)
        print(f"Created dataset: {project_id}.{dataset_id}")


def load_dataframe_to_bigquery(
    client: bigquery.Client,
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_name: str,
    write_disposition: str,
) -> None:
    destination_table = f"{project_id}.{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
    )

    load_job = client.load_table_from_dataframe(
        df,
        destination_table,
        job_config=job_config,
    )
    load_job.result()

    table = client.get_table(destination_table)
    print(f"Loaded {table.num_rows} rows into {destination_table}")


def main() -> int:
    try:
        project_id = require_env("GCP_PROJECT_ID")
        dataset_id = require_env("BIGQUERY_DATASET")

        print(f"Using DuckDB database: {DUCKDB_PATH}")
        print(f"Reading table: {DUCKDB_TABLE}")

        ensure_duckdb_table_exists(DUCKDB_PATH, DUCKDB_TABLE)
        df = read_fact_orders_from_duckdb(DUCKDB_PATH, DUCKDB_TABLE)

        print(f"Read {len(df)} rows from DuckDB")

        client = bigquery.Client(project=project_id)

        ensure_bigquery_dataset(
            client=client,
            project_id=project_id,
            dataset_id=dataset_id,
            location=BQ_LOCATION,
        )

        load_dataframe_to_bigquery(
            client=client,
            df=df,
            project_id=project_id,
            dataset_id=dataset_id,
            table_name=BQ_TABLE,
            write_disposition=WRITE_DISPOSITION,
        )

        print("BigQuery load completed successfully.")
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())