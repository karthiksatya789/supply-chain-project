import streamlit as st
import pandas as pd
import plotly.express as px
import os
import pandas_gbq
from google.oauth2 import service_account


SERVICE_ACCOUNT_FILE = "supply-chain-data-integration-e58b2d6792ca.json"
project_id = "supply-chain-data-integration"
dataset = "supply_chain"
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)


st.set_page_config(page_title="Supply Chain Dashboard", layout="wide")
st.title("Supply Chain Integration Dashboard")


@st.cache_data(ttl=600)
def load_data():
    orders_query = f"SELECT * FROM `{project_id}.{dataset}.vw_orders_enriched`"
    inventory_query = f"SELECT * FROM `{project_id}.{dataset}.fact_inventory`"
    orders_df = pandas_gbq.read_gbq(orders_query, project_id=project_id, credentials=credentials)
    inventory_df = pandas_gbq.read_gbq(inventory_query, project_id=project_id, credentials=credentials)

    
    orders_df.columns = [col.strip().lower().replace(" ", "_") for col in orders_df.columns]
    inventory_df.columns = [col.strip().lower().replace(" ", "_") for col in inventory_df.columns]

    return orders_df, inventory_df

try:
    orders_df, inventory_df = load_data()
except Exception as e:
    st.error(f" Failed to load data from BigQuery: {e}")
    st.stop()

st.markdown("### 🔍 Filters")

col1, col2, col3 = st.columns(3)
with col1:
    start_date = st.date_input("Start Date", pd.to_datetime(inventory_df["date"]).min())
with col2:
    end_date = st.date_input("End Date", pd.to_datetime(inventory_df["date"]).max())
with col3:
    selected_category = st.selectbox("Select Category", inventory_df["category"].unique())

products_filtered = inventory_df[inventory_df["category"] == selected_category]["product_name"].unique()
selected_product = st.selectbox("Select Product", products_filtered)


inventory_df["date"] = pd.to_datetime(inventory_df["date"])
filtered_inv = inventory_df[
    (inventory_df["date"] >= pd.to_datetime(start_date)) &
    (inventory_df["date"] <= pd.to_datetime(end_date)) &
    (inventory_df["category"] == selected_category) &
    (inventory_df["product_name"] == selected_product)
]


st.header(" Overview: Key Metrics and Category Orders")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Orders", len(orders_df))
col2.metric("Unique Customers", orders_df["customer_id"].nunique() if "customer_id" in orders_df.columns else "N/A")
avg_inventory = int(filtered_inv["inventory_level"].mean()) if not filtered_inv.empty else 0
stockouts = (filtered_inv["inventory_level"] == 0).sum() if not filtered_inv.empty else 0
col3.metric("Avg Inventory Level", avg_inventory)
col4.metric("Stockouts", stockouts)

st.markdown("---")
st.subheader("Orders by Category")
if "category" in orders_df.columns:
    order_counts = orders_df["category"].value_counts().reset_index()
    order_counts.columns = ["Category", "Count"]
    fig_cat = px.bar(order_counts, x="Category", y="Count", color="Category", title="Order Distribution by Category")
    st.plotly_chart(fig_cat, use_container_width=True)
else:
    st.warning("Category column not found in order data.")

st.header(" Analysis: Inventory, Prices, Restocks")

st.subheader(f"Inventory Trend for {selected_product}")
if not filtered_inv.empty:
    trend = filtered_inv.pivot_table(index="date", values="inventory_level")
    st.line_chart(trend)
else:
    st.warning("No inventory data for selected filters.")

st.subheader("Price Trend Over Time")
if not filtered_inv.empty:
    price_trend = filtered_inv.groupby("date")["price"].mean().reset_index()
    st.area_chart(price_trend.rename(columns={"price": "Average Price"}).set_index("date"))

st.subheader("Restock Frequency by Date")
if not filtered_inv.empty:
    restock_by_day = filtered_inv[filtered_inv["restock"] == 1].groupby("date")["restock"].count().reset_index()
    fig_restock = px.bar(restock_by_day, x="date", y="restock", title="Restock Counts", labels={"restock": "Restocks"})
    st.plotly_chart(fig_restock, use_container_width=True)

st.subheader("Inventory Share by Product")
pie_df = filtered_inv.groupby("product_name")["inventory_level"].sum().reset_index()
fig_pie = px.pie(pie_df, names="product_name", values="inventory_level", title="Inventory Share (Filtered)")
st.plotly_chart(fig_pie, use_container_width=True)


st.header(" Alerts: Low Inventory Warnings")

st.subheader("Low Inventory Products (≤ 20 Units)")
low_stock = filtered_inv[filtered_inv["inventory_level"] <= 20]
if not low_stock.empty:
    st.dataframe(low_stock[["date", "product_name", "inventory_level", "restock"]])
    csv = low_stock.to_csv(index=False).encode("utf-8")
    st.download_button(" Download Alerts CSV", data=csv, file_name="low_inventory_alerts.csv", mime="text/csv")
else:
    st.success(" No low inventory issues detected.")


st.markdown("---")
st.caption(" Data source: BigQuery • Global Superstore + Fake Store API Inventory")
