import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_FILE = PROJECT_ROOT / "data" / "processed" / "retail_orders_clean.jsonl"
DB_DIR = PROJECT_ROOT / "data" / "warehouse"
DB_FILE = DB_DIR / "retail.duckdb"


def main():
    DB_DIR.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(DB_FILE))

    conn.execute("DROP TABLE IF EXISTS fact_orders")

    conn.execute(f"""
        CREATE TABLE fact_orders AS
        SELECT
            order_id,
            customer_id,
            product_id,
            CAST(timestamp AS TIMESTAMP) AS order_timestamp,
            CAST(CAST(timestamp AS TIMESTAMP) AS DATE) AS order_date,
            category,
            region,
            payment_method,
            order_status,
            quantity,
            unit_price,
            total_amount,
            amount_bucket
        FROM read_json_auto('{INPUT_FILE}')
    """)

    row_count = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    print(f"Loaded {row_count} rows into fact_orders")
    print(f"DuckDB database created at: {DB_FILE}")

    conn.close()


if __name__ == "__main__":
    main()