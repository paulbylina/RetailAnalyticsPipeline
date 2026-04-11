from pathlib import Path

import duckdb
import pytest


pytestmark = pytest.mark.warehouse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_PATH = PROJECT_ROOT / "data" / "warehouse" / "retail.duckdb"
TABLE_NAME = "retail_order_events"


def query_one(sql: str):
    with duckdb.connect(str(DUCKDB_PATH)) as conn:
        return conn.execute(sql).fetchone()[0]


def test_retail_order_events_table_exists():
    sql = f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{TABLE_NAME}'
    """
    assert query_one(sql) == 1


def test_retail_order_events_has_rows():
    sql = f"SELECT COUNT(*) FROM {TABLE_NAME}"
    assert query_one(sql) > 0


def test_retail_order_events_order_id_not_null():
    sql = f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE order_id IS NULL"
    assert query_one(sql) == 0


def test_retail_order_events_order_id_unique():
    sql = f"""
        SELECT COUNT(*)
        FROM (
            SELECT order_id
            FROM {TABLE_NAME}
            GROUP BY order_id
            HAVING COUNT(*) > 1
        ) dupes
    """
    assert query_one(sql) == 0