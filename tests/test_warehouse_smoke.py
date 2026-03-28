from pathlib import Path
import subprocess
import sys
import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_FILE = PROJECT_ROOT / "data" / "warehouse" / "retail.duckdb"


def run_script(script_path: str) -> None:
    full_path = PROJECT_ROOT / script_path
    result = subprocess.run([sys.executable, str(full_path)], cwd=PROJECT_ROOT)
    assert result.returncode == 0, f"Script failed: {script_path}"


def test_warehouse_tables_exist():
    run_script("src/ingestion/generate_retail_data.py")
    run_script("src/etl/transform_retail_orders.py")
    run_script("src/etl/create_fact_orders_table.py")
    run_script("src/etl/create_dim_date_table.py")
    run_script("src/etl/create_dim_customers_table.py")

    assert DB_FILE.exists(), "DuckDB database file was not created"

    con = duckdb.connect(str(DB_FILE))

    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    assert "fact_orders" in tables
    assert "dim_date" in tables
    assert "dim_customers" in tables

    fact_count = con.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    dim_date_count = con.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
    dim_customers_count = con.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]

    con.close()

    assert fact_count > 0
    assert dim_date_count > 0
    assert dim_customers_count > 0