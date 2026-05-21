"""Streamlit dashboard for real-time air quality visualization."""
import os

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://localhost:8000")
REFRESH_SEC = 30

st.set_page_config(page_title="Air Quality Monitor", layout="wide")
st.title("Air Quality Monitor — OpenAQ")

# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")
    try:
        countries_resp = requests.get(f"{API_URL}/countries", timeout=5)
        country_options = countries_resp.json() if countries_resp.ok else []
    except Exception:
        country_options = []

    selected_country = st.selectbox("Country", ["(all)"] + country_options)
    selected_param = st.selectbox("Parameter", ["(all)", "pm25", "pm10", "o3", "no2", "so2", "co"])
    limit = st.slider("Max records", 50, 2000, 500)
    auto_refresh = st.checkbox("Auto-refresh every 30 s", value=True)

# ── Data fetch ────────────────────────────────────────────────────────────────
params: dict[str, str | int] = {"limit": limit}
if selected_country != "(all)":
    params["country"] = selected_country
if selected_param != "(all)":
    params["parameter"] = selected_param

try:
    resp = requests.get(f"{API_URL}/measurements", params=params, timeout=10)
    data = resp.json() if resp.ok else []
except Exception as exc:
    data = []
    st.error(f"Cannot reach API at {API_URL}: {exc}")

df = pd.DataFrame(data)

# ── Stats ─────────────────────────────────────────────────────────────────────
try:
    stats = requests.get(f"{API_URL}/stats", timeout=5).json()
except Exception:
    stats = {}

col1, col2, col3 = st.columns(3)
col1.metric("Total records in memory", stats.get("total_records", len(df)))
col2.metric("Countries", len(stats.get("countries", [])))
col3.metric("Parameters", len(stats.get("parameters", [])))

if df.empty:
    st.info("No data yet — waiting for collector to publish measurements.")
    if auto_refresh:
        st.rerun()
    st.stop()

df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
df["value"] = pd.to_numeric(df["value"], errors="coerce")
df = df.dropna(subset=["value"])

# ── Charts ────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["Time Series", "Map", "Distribution"])

with tab1:
    st.subheader("Measurements over time")
    fig = px.scatter(
        df,
        x="timestamp",
        y="value",
        color="parameter",
        hover_data=["location_name", "country_code", "unit"],
        title="All measurements",
    )
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader("Station map")
    map_df = df.dropna(subset=["latitude", "longitude"]).drop_duplicates("location_id")
    if not map_df.empty:
        fig_map = px.scatter_mapbox(
            map_df,
            lat="latitude",
            lon="longitude",
            hover_name="location_name",
            hover_data=["country_code", "city"],
            color="country_code",
            zoom=1,
            mapbox_style="open-street-map",
        )
        st.plotly_chart(fig_map, use_container_width=True)

with tab3:
    st.subheader("Value distribution by parameter")
    fig_box = px.box(df, x="parameter", y="value", color="parameter", points="outliers")
    st.plotly_chart(fig_box, use_container_width=True)

# ── Raw table ─────────────────────────────────────────────────────────────────
with st.expander("Raw data"):
    st.dataframe(df.tail(200))

if auto_refresh:
    import time
    time.sleep(REFRESH_SEC)
    st.rerun()
