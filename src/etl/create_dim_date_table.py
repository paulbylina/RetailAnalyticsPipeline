import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_FILE = PROJECT_ROOT / "data" / "warehouse" / "retail.duckdb"


def main():
    conn = duckdb.connect(str(DB_FILE))

    conn.execute("DROP TABLE IF EXISTS dim_date")

    conn.execute("""
        CREATE TABLE dim_date AS
        SELECT DISTINCT
            CAST(STRFTIME(order_date, '%Y%m%d') AS BIGINT) AS date_key,
            order_date AS full_date,
            EXTRACT(YEAR FROM order_date) AS year,
            EXTRACT(MONTH FROM order_date) AS month,
            STRFTIME(order_date, '%B') AS month_name,
            EXTRACT(DAY FROM order_date) AS day,
            STRFTIME(order_date, '%A') AS weekday_name,
            CASE
                WHEN STRFTIME(order_date, '%w') IN ('0', '6') THEN TRUE
                ELSE FALSE
            END AS weekend_flag
        FROM fact_orders
        ORDER BY full_date
    """)

    row_count = conn.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
    print(f"Created dim_date with {row_count} rows")

    conn.close()


if __name__ == "__main__":
    main()