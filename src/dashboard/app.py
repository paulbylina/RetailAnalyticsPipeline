from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st
import altair as alt

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

def apply_region_filter(df: pd.DataFrame, selected_region: str) -> pd.DataFrame:
    if selected_region == "All":
        return df
    if "region" in df.columns:
        return df[df["region"] == selected_region]
    return df


st.set_page_config(page_title="Retail Analytics Dashboard", layout="wide")
region_options = ["All"] + sorted(run_query("revenue_by_region")["region"].tolist())
selected_region = st.sidebar.selectbox("Filter by region", region_options)
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

region_chart = (
    alt.Chart(region_df)
    .mark_bar()
    .encode(
        x=alt.X("region:N", sort="-y", title="Region"),
        y=alt.Y("total_revenue:Q", title="Total Revenue"),
        tooltip=["region", "total_orders", "total_revenue", "avg_order_value"],
    )
    .properties(height=400)
)
st.altair_chart(region_chart, use_container_width=True)

# Top categories
st.subheader("Which categories perform best?")
category_df = apply_region_filter(run_query("top_categories"), selected_region)

category_chart = (
    alt.Chart(category_df)
    .mark_bar()
    .encode(
        x=alt.X("category:N", sort="-y", title="Category"),
        y=alt.Y("total_revenue:Q", title="Total Revenue"),
        tooltip=["category:N", "total_orders:Q", "total_revenue:Q", "avg_order_value:Q"],
    )
    .properties(height=400)
)
st.altair_chart(category_chart, use_container_width=True)

# Daily revenue trend
st.subheader("How is revenue trending over time?")
daily_df = apply_region_filter(run_query("daily_revenue_trend"), selected_region)
daily_df["order_date"] = pd.to_datetime(daily_df["order_date"])
daily_df = daily_df.sort_values("order_date")

daily_chart = (
    alt.Chart(daily_df)
    .mark_line(point=True)
    .encode(
        x=alt.X("order_date:T", title="Order Date"),
        y=alt.Y("total_revenue:Q", title="Total Revenue"),
        tooltip=["order_date:T", "total_orders:Q", "total_revenue:Q"],
    )
    .properties(height=400)
)
st.altair_chart(daily_chart, use_container_width=True)

# Revenue by customer segment
st.subheader("How do customer segments perform?")
segment_df = apply_region_filter(run_query("revenue_by_customer_segment"), selected_region)

segment_chart = (
    alt.Chart(segment_df)
    .mark_bar()
    .encode(
        x=alt.X("customer_segment:N", sort="-y", title="Customer Segment"),
        y=alt.Y("total_revenue:Q", title="Total Revenue"),
        tooltip=[
            "customer_segment:N",
            "total_orders:Q",
            "total_revenue:Q",
            "avg_order_value:Q",
        ],
    )
    .properties(height=400)
)
st.altair_chart(segment_chart, use_container_width=True)

# Revenue by weekday
st.subheader("Which weekdays generate the most revenue?")
weekday_df = apply_region_filter(run_query("revenue_by_weekday"), selected_region)

weekday_chart = (
    alt.Chart(weekday_df)
    .mark_bar()
    .encode(
        x=alt.X(
            "weekday_name:N",
            sort=[
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ],
            title="Weekday",
        ),
        y=alt.Y("total_revenue:Q", title="Total Revenue"),
        tooltip=["weekday_name", "total_orders", "total_revenue", "avg_order_value"],
    )
    .properties(height=400)
)
st.altair_chart(weekday_chart, use_container_width=True)