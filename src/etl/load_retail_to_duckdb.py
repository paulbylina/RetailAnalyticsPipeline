import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_FILE = PROJECT_ROOT / "data" / "warehouse" / "retail_analytics.duckdb"
INPUT_FILE = PROJECT_ROOT / "data" / "processed" / "retail_orders_clean.jsonl"

def main():
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(DB_FILE))

    conn.execute("DROP TABLE IF EXISTS retail_orders_clean")

    conn.execute(f"""
        CREATE TABLE retail_orders_clean AS
        SELECT *
        FROM read_json_auto('{INPUT_FILE}')
    """)

    row_count = conn.execute("SELECT COUNT(*) FROM retail_orders_clean").fetchone()[0]
    print(f"Loaded {row_count} rows into {DB_FILE}")

    conn.close()

if __name__ == "__main__":
    main()