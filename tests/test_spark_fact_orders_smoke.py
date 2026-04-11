from pathlib import Path

import duckdb
import pytest


pytestmark = pytest.mark.etl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PARQUET_PATH = PROJECT_ROOT / "data" / "curated" / "fact_orders_spark.parquet"


def query_one(sql: str):
    with duckdb.connect() as conn:
        return conn.execute(sql).fetchone()[0]


def test_spark_parquet_output_exists():
    assert PARQUET_PATH.exists(), f"Missing Spark output path: {PARQUET_PATH}"


def test_spark_parquet_has_rows():
    sql = f"SELECT COUNT(*) FROM read_parquet('{PARQUET_PATH}')"
    assert query_one(sql) > 0


def test_spark_parquet_order_id_unique():
    sql = f"""
        SELECT COUNT(*)
        FROM (
            SELECT order_id
            FROM read_parquet('{PARQUET_PATH}')
            GROUP BY order_id
            HAVING COUNT(*) > 1
        ) dupes
    """
    assert query_one(sql) == 0


def test_spark_parquet_total_amount_non_negative():
    sql = f"""
        SELECT COUNT(*)
        FROM read_parquet('{PARQUET_PATH}')
        WHERE total_amount < 0
    """
    assert query_one(sql) == 0