import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_FILE = PROJECT_ROOT / "data" / "warehouse" / "retail_analytics.duckdb"
SQL_FILE = PROJECT_ROOT / "sql" / "retail_kpis.sql"


def main():
    con = duckdb.connect(str(DB_FILE))
    query = SQL_FILE.read_text()
    results = con.execute(query).fetchall()

    for row in results:
        print(row)

    con.close()

if __name__ == "__main__":
    main()