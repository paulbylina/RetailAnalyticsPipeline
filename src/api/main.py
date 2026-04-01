from pathlib import Path

import duckdb
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Retail Analytics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_FILE = PROJECT_ROOT / "data" / "warehouse" / "retail.duckdb"


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/kpis")
def get_kpis():
    con = duckdb.connect(str(DB_FILE))

    query = """
        SELECT
            COUNT(*) AS total_orders,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(total_amount), 2) AS avg_order_value,
            ROUND(SUM(CASE WHEN order_status = 'completed' THEN total_amount ELSE 0 END), 2) AS completed_revenue,
            SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
            SUM(CASE WHEN order_status = 'returned' THEN 1 ELSE 0 END) AS returned_orders
        FROM fact_orders
    """

    row = con.execute(query).fetchone()
    con.close()

    return {
        "total_orders": row[0],
        "total_revenue": row[1],
        "avg_order_value": row[2],
        "completed_revenue": row[3],
        "cancelled_orders": row[4],
        "returned_orders": row[5],
    }


@app.get("/revenue-by-region")
def get_revenue_by_region():
    con = duckdb.connect(str(DB_FILE))

    query = """
        SELECT
            region,
            COUNT(*) AS total_orders,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(total_amount), 2) AS avg_order_value
        FROM fact_orders
        GROUP BY region
        ORDER BY total_revenue DESC
    """

    rows = con.execute(query).fetchall()
    con.close()

    return [
        {
            "region": row[0],
            "total_orders": row[1],
            "total_revenue": row[2],
            "avg_order_value": row[3],
        }
        for row in rows
    ]