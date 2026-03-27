import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_FILE = PROJECT_ROOT / "data" / "warehouse" / "retail.duckdb"


def get_customer_segment(customer_id: int) -> str:
    if customer_id <= 150:
        return "budget"
    if customer_id <= 350:
        return "standard"
    return "premium"


def main():
    conn = duckdb.connect(str(DB_FILE))

    conn.execute("DROP TABLE IF EXISTS dim_customers")

    customer_ids = conn.execute("""
        SELECT DISTINCT customer_id
        FROM fact_orders
        ORDER BY customer_id
    """).fetchall()

    customer_rows = [
        (customer_id, get_customer_segment(customer_id))
        for (customer_id,) in customer_ids
    ]

    conn.execute("""
        CREATE TABLE dim_customers (
            customer_id INTEGER,
            customer_segment VARCHAR
        )
    """)

    conn.executemany("""
        INSERT INTO dim_customers (customer_id, customer_segment)
        VALUES (?, ?)
    """, customer_rows)

    row_count = conn.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
    print(f"Created dim_customers with {row_count} rows")

    conn.close()


if __name__ == "__main__":
    main()