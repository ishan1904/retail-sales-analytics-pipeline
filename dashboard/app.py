# dashboard/app.py

import sqlite3
from pathlib import Path
import pandas as pd
import streamlit as st

# === Setup ===
ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "warehouse" / "retail.db"

st.set_page_config(page_title="Retail Dashboard", layout="wide")
st.title("📊 Retail Sales Analytics Dashboard")

# === Load Data ===
@st.cache_data
def load_query(query: str):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn)

# === KPI 1: Monthly Revenue by Category ===
st.subheader("Monthly Revenue by Category")
q1 = """
SELECT d.year, d.month, p.category, ROUND(SUM(f.net_revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY d.year, d.month, p.category
ORDER BY d.year, d.month, revenue DESC;
"""
df1 = load_query(q1)
st.dataframe(df1, use_container_width=True)

if not df1.empty:
    pivot = df1.pivot_table(index=["year", "month"], columns="category", values="revenue", aggfunc="sum")
    pivot = pivot.fillna(0).reset_index()  # flatten index

    # ✅ Combine year and month into a single column for easy plotting
    pivot["year_month"] = pivot["year"].astype(str) + "-" + pivot["month"].astype(str).str.zfill(2)
    pivot = pivot.drop(columns=["year", "month"])

    st.subheader("📊 Monthly Revenue by Category")
    st.dataframe(pivot, use_container_width=True)

    # ✅ Set a simple string index for Streamlit to understand
    st.bar_chart(pivot.set_index("year_month"))

    # === KPI 2: Top Subcategories ===
st.subheader("Top 10 Subcategories by Revenue")
q2 = """
SELECT p.category, p.subcategory, ROUND(SUM(f.net_revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category, p.subcategory
ORDER BY revenue DESC
LIMIT 10;
"""
df2 = load_query(q2)
st.dataframe(df2, use_container_width=True)

# === KPI 3: Revenue by Region ===
st.subheader("Regional Revenue")
q3 = """
SELECT r.region, ROUND(SUM(f.net_revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_region r ON f.region_id = r.region_id
GROUP BY r.region
ORDER BY revenue DESC;
"""
df3 = load_query(q3)
st.dataframe(df3, use_container_width=True)

if not df3.empty:
    st.bar_chart(df3.set_index("region"))
