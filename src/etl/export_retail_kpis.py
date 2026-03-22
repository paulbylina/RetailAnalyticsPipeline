import csv
import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_FILE = PROJECT_ROOT / "data" / "warehouse" / "retail_analytics.duckdb"
SQL_FILE = PROJECT_ROOT / "sql" / "retail_kpis.sql"
OUTPUT_FILE = PROJECT_ROOT / "data" / "processed" / "retail_kpis.csv"

def main():
    query = SQL_FILE.read_text()

    con = duckdb.connect(str(DB_FILE))
    results = con.execute(query).fetchall()
    columns = [desc[0] for desc in con.description]
    con.close()

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(results)

    print(f"Exported KPI results to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()