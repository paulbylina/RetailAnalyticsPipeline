from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_FILE = PROJECT_ROOT / "data" / "warehouse" / "retail.duckdb"
SQL_DIR = PROJECT_ROOT / "sql" / "analytics"


def run_query(query_name: str) -> pd.DataFrame:
    sql_file = SQL_DIR / f"{query_name}.sql"
    sql = sql_file.read_text()

    con = duckdb.connect(str(DB_FILE))
    df = con.execute(sql).fetchdf()
    con.close()

    return df


st.set_page_config(page_title="Retail Analytics Dashboard", layout="wide")

st.title("Retail Analytics Dashboard")

kpi_df = run_query("kpi_summary")
kpi = kpi_df.iloc[0]
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Orders", f"{int(kpi['total_orders'])}")
col2.metric("Total Revenue", f"${kpi['total_revenue']:,.2f}")
col3.metric("Avg Order Value", f"${kpi['avg_order_value']:,.2f}")
col4.metric("Completed Revenue", f"${kpi['completed_revenue']:,.2f}")
st.divider()

# Revenue by region
st.subheader("Which regions generate the most revenue?")
region_df = run_query("revenue_by_region")
st.bar_chart(region_df.set_index("region")["total_revenue"])

# Top categories
st.subheader("Which categories perform best?")
category_df = run_query("top_categories")
st.bar_chart(category_df.set_index("category")["total_revenue"])

# Daily revenue trend
st.subheader("How is revenue trending over time?")
daily_df = run_query("daily_revenue_trend")
daily_df["order_date"] = pd.to_datetime(daily_df["order_date"])
daily_df = daily_df.sort_values("order_date")
st.line_chart(daily_df.set_index("order_date")["total_revenue"])

# Revenue by customer segment
st.subheader("How do customer segments perform?")
segment_df = run_query("revenue_by_customer_segment")
st.bar_chart(segment_df.set_index("customer_segment")["total_revenue"])