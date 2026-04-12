from pathlib import Path

import duckdb
import pytest


pytestmark = pytest.mark.etl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PARQUET_DIR = PROJECT_ROOT / "data" / "curated" / "fact_orders_spark.parquet"
PARQUET_GLOB = str(PARQUET_DIR / "*.parquet")

if not PARQUET_DIR.exists() or not list(PARQUET_DIR.glob("*.parquet")):
    pytest.skip(
        f"Missing Spark parquet output in {PARQUET_DIR}. Run python -m src.etl.run_spark_fact_orders first.",
        allow_module_level=True,
    )


def query_one(sql: str):
    with duckdb.connect() as conn:
        return conn.execute(sql).fetchone()[0]


def test_spark_parquet_has_rows():
    sql = f"SELECT COUNT(*) FROM read_parquet('{PARQUET_GLOB}')"
    assert query_one(sql) > 0


def test_spark_parquet_order_id_unique():
    sql = f"""
        SELECT COUNT(*)
        FROM (
            SELECT order_id
            FROM read_parquet('{PARQUET_GLOB}')
            GROUP BY order_id
            HAVING COUNT(*) > 1
        ) dupes
    """
    assert query_one(sql) == 0


def test_spark_parquet_total_amount_non_negative():
    sql = f"""
        SELECT COUNT(*)
        FROM read_parquet('{PARQUET_GLOB}')
        WHERE total_amount < 0
    """
    assert query_one(sql) == 0