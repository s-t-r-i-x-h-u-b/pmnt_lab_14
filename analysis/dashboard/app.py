"""Streamlit dashboard — shows both raw measurements and tumbling-window aggregates."""
import os
import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

API_URL     = os.getenv("API_URL", "http://localhost:8000")
REFRESH_SEC = 30

st.set_page_config(page_title="Air Quality Monitor", layout="wide")
st.title("Air Quality Monitor — OpenAQ")


def fetch(path: str, **params) -> list[dict]:
    try:
        r = requests.get(f"{API_URL}{path}", params=params, timeout=10)
        return r.json() if r.ok else []
    except Exception:
        return []


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")
    countries    = fetch("/countries")
    sel_country  = st.selectbox("Country", ["(all)"] + countries)
    sel_param    = st.selectbox("Parameter", ["(all)", "pm25", "pm10", "o3", "no2", "so2", "co"])
    limit        = st.slider("Max records", 50, 2000, 500)
    auto_refresh = st.checkbox("Auto-refresh every 30 s", value=True)

country_arg   = sel_country  if sel_country  != "(all)" else None
parameter_arg = sel_param    if sel_param    != "(all)" else None

# ── Top stats ─────────────────────────────────────────────────────────────────
stats = fetch("/stats") or {}
col1, col2, col3, col4 = st.columns(4)
col1.metric("Raw records in memory",  stats.get("raw_records",  0))
col2.metric("Agg records in memory",  stats.get("agg_records",  0))
col3.metric("Countries",   len(stats.get("countries",  [])))
col4.metric("Parameters",  len(stats.get("parameters", [])))

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_raw, tab_agg, tab_cmp = st.tabs(["Raw measurements", "Aggregated windows", "Compression"])

# ── Raw tab ───────────────────────────────────────────────────────────────────
with tab_raw:
    raw_data = fetch("/measurements", limit=limit,
                     **({} if not country_arg   else {"country":   country_arg}),
                     **({} if not parameter_arg else {"parameter": parameter_arg}))
    df_raw = pd.DataFrame(raw_data)
    if df_raw.empty:
        st.info("No raw data yet.")
    else:
        df_raw["timestamp"] = pd.to_datetime(df_raw["timestamp"], errors="coerce", utc=True)
        df_raw["value"]     = pd.to_numeric(df_raw["value"], errors="coerce")
        df_raw = df_raw.dropna(subset=["value"])

        st.subheader("Measurements over time")
        fig = px.scatter(df_raw, x="timestamp", y="value", color="parameter",
                         hover_data=["location_name", "country_code", "unit"])
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Station map")
        map_df = df_raw.dropna(subset=["latitude","longitude"]).drop_duplicates("location_id")
        if not map_df.empty:
            fig_map = px.scatter_mapbox(map_df, lat="latitude", lon="longitude",
                                        hover_name="location_name",
                                        hover_data=["country_code","city"],
                                        color="country_code", zoom=1,
                                        mapbox_style="open-street-map")
            st.plotly_chart(fig_map, use_container_width=True)

        with st.expander("Raw data table"):
            st.dataframe(df_raw.tail(200))

# ── Aggregated tab ────────────────────────────────────────────────────────────
with tab_agg:
    agg_data = fetch("/aggregated", limit=limit,
                     **({} if not country_arg   else {"country":   country_arg}),
                     **({} if not parameter_arg else {"parameter": parameter_arg}))
    df_agg = pd.DataFrame(agg_data)
    if df_agg.empty:
        st.info("No aggregated data yet — waiting for a window to close.")
    else:
        for col in ("window_start", "window_end"):
            df_agg[col] = pd.to_datetime(df_agg[col], errors="coerce", utc=True)
        for col in ("mean_value", "min_value", "max_value", "std_value"):
            df_agg[col] = pd.to_numeric(df_agg[col], errors="coerce")

        st.subheader("Mean value per window (by parameter)")
        fig_mean = px.line(df_agg.sort_values("window_end"),
                           x="window_end", y="mean_value", color="parameter",
                           hover_data=["country_code","count","min_value","max_value","std_value"],
                           markers=True)
        st.plotly_chart(fig_mean, use_container_width=True)

        st.subheader("Min / Mean / Max band per parameter")
        params_available = df_agg["parameter"].dropna().unique().tolist()
        sel_p = st.selectbox("Parameter for band chart", params_available, key="band_param")
        df_p = df_agg[df_agg["parameter"] == sel_p].sort_values("window_end")
        if not df_p.empty:
            fig_band = go.Figure()
            fig_band.add_trace(go.Scatter(x=df_p["window_end"], y=df_p["max_value"],
                                          mode="lines", name="max",
                                          line=dict(width=0), showlegend=False))
            fig_band.add_trace(go.Scatter(x=df_p["window_end"], y=df_p["min_value"],
                                          mode="lines", name="min",
                                          fill="tonexty", fillcolor="rgba(68,114,196,0.15)",
                                          line=dict(width=0), showlegend=False))
            fig_band.add_trace(go.Scatter(x=df_p["window_end"], y=df_p["mean_value"],
                                          mode="lines+markers", name="mean",
                                          line=dict(color="rgb(68,114,196)", width=2)))
            fig_band.update_layout(title=f"{sel_p} — min/mean/max band",
                                   xaxis_title="window end", yaxis_title="value")
            st.plotly_chart(fig_band, use_container_width=True)

        st.subheader("Raw readings aggregated per window")
        fig_count = px.bar(df_agg.sort_values("window_end"),
                           x="window_end", y="count", color="parameter",
                           title="Raw readings compressed into each window")
        st.plotly_chart(fig_count, use_container_width=True)

        with st.expander("Aggregated data table"):
            st.dataframe(df_agg.tail(200))

# ── Compression tab ───────────────────────────────────────────────────────────
with tab_cmp:
    st.subheader("Data reduction by tumbling-window aggregation")
    agg_stat = fetch("/aggregated/stats") or {}
    if not agg_stat:
        st.info("No aggregated stats yet.")
    else:
        rows = [
            {"parameter": p,
             "windows":   v.get("windows", 0),
             "raw_total": v.get("raw_total", 0),
             "overall_mean": round(v.get("overall_mean") or 0, 3)}
            for p, v in agg_stat.items()
        ]
        df_stat = pd.DataFrame(rows)
        col_a, col_b = st.columns(2)
        with col_a:
            fig_raw = px.bar(df_stat, x="parameter", y="raw_total",
                             title="Total raw readings aggregated per parameter")
            st.plotly_chart(fig_raw, use_container_width=True)
        with col_b:
            fig_mean = px.bar(df_stat, x="parameter", y="overall_mean",
                              title="Overall mean per parameter across all windows")
            st.plotly_chart(fig_mean, use_container_width=True)

        st.caption(
            "Each window compresses N raw readings into one aggregate row per (country, parameter). "
            "Fewer NATS messages → lower network traffic and simpler Python processing."
        )

if auto_refresh:
    time.sleep(REFRESH_SEC)
    st.rerun()
