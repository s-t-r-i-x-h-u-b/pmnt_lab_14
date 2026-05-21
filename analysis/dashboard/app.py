"""Streamlit dashboard — raw measurements, aggregated windows, Arrow Flight direct access."""
import os
import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

API_URL     = os.getenv("API_URL",     "http://localhost:8000")
REFRESH_SEC = 30

st.set_page_config(page_title="Air Quality Monitor", layout="wide")
st.title("Air Quality Monitor — OpenAQ")


def fetch(path: str, **params) -> list[dict]:
    try:
        r = requests.get(f"{API_URL}{path}", params={k: v for k, v in params.items() if v}, timeout=10)
        return r.json() if r.ok else []
    except Exception:
        return []


def fetch_obj(path: str) -> dict:
    try:
        r = requests.get(f"{API_URL}{path}", timeout=10)
        return r.json() if r.ok else {}
    except Exception:
        return {}


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")
    countries    = fetch("/countries") or []
    sel_country  = st.selectbox("Country",   ["(all)"] + countries)
    sel_param    = st.selectbox("Parameter", ["(all)", "pm25", "pm10", "o3", "no2", "so2", "co"])
    limit        = st.slider("Max records", 50, 2000, 500)
    auto_refresh = st.checkbox("Auto-refresh every 30 s", value=True)

country_arg   = sel_country if sel_country != "(all)" else None
parameter_arg = sel_param   if sel_param   != "(all)" else None

# ── Top stats ─────────────────────────────────────────────────────────────────
stats = fetch_obj("/stats")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Raw records (NATS)",  stats.get("raw_records",  0))
c2.metric("Agg records (NATS)",  stats.get("agg_records",  0))
c3.metric("Countries",  len(stats.get("countries",  [])))
c4.metric("Parameters", len(stats.get("parameters", [])))

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_raw, tab_agg, tab_flight, tab_cmp = st.tabs([
    "Raw measurements", "Aggregated windows", "Arrow Flight (direct)", "Compression",
])

# ── Raw tab ───────────────────────────────────────────────────────────────────
with tab_raw:
    raw_data = fetch("/measurements", limit=limit,
                     country=country_arg, parameter=parameter_arg)
    df_raw = pd.DataFrame(raw_data)
    if df_raw.empty:
        st.info("No raw data yet.")
    else:
        df_raw["timestamp"] = pd.to_datetime(df_raw["timestamp"], errors="coerce", utc=True)
        df_raw["value"]     = pd.to_numeric(df_raw["value"], errors="coerce")
        df_raw = df_raw.dropna(subset=["value"])
        fig = px.scatter(df_raw, x="timestamp", y="value", color="parameter",
                         hover_data=["location_name", "country_code", "unit"])
        st.plotly_chart(fig, use_container_width=True)
        map_df = df_raw.dropna(subset=["latitude", "longitude"]).drop_duplicates("location_id")
        if not map_df.empty:
            fig_map = px.scatter_mapbox(map_df, lat="latitude", lon="longitude",
                                        hover_name="location_name",
                                        color="country_code", zoom=1,
                                        mapbox_style="open-street-map")
            st.plotly_chart(fig_map, use_container_width=True)
        with st.expander("Raw data table"):
            st.dataframe(df_raw.tail(200))

# ── Aggregated tab ────────────────────────────────────────────────────────────
with tab_agg:
    agg_data = fetch("/aggregated", limit=limit,
                     country=country_arg, parameter=parameter_arg)
    df_agg = pd.DataFrame(agg_data)
    if df_agg.empty:
        st.info("No aggregated data yet — waiting for a window to close.")
    else:
        for col in ("window_start", "window_end"):
            df_agg[col] = pd.to_datetime(df_agg[col], errors="coerce", utc=True)
        for col in ("mean_value", "min_value", "max_value", "std_value"):
            df_agg[col] = pd.to_numeric(df_agg[col], errors="coerce")
        fig_mean = px.line(df_agg.sort_values("window_end"),
                           x="window_end", y="mean_value", color="parameter",
                           hover_data=["country_code","count","min_value","max_value","std_value"],
                           markers=True, title="Mean value per window")
        st.plotly_chart(fig_mean, use_container_width=True)

        sel_p = st.selectbox("Parameter for band chart",
                             df_agg["parameter"].dropna().unique().tolist(),
                             key="band_param")
        df_p = df_agg[df_agg["parameter"] == sel_p].sort_values("window_end")
        if not df_p.empty:
            fig_band = go.Figure()
            fig_band.add_trace(go.Scatter(x=df_p["window_end"], y=df_p["max_value"],
                                          mode="lines", fill=None, line=dict(width=0)))
            fig_band.add_trace(go.Scatter(x=df_p["window_end"], y=df_p["min_value"],
                                          mode="lines", fill="tonexty",
                                          fillcolor="rgba(68,114,196,0.15)",
                                          line=dict(width=0), name="min/max band"))
            fig_band.add_trace(go.Scatter(x=df_p["window_end"], y=df_p["mean_value"],
                                          mode="lines+markers", name="mean",
                                          line=dict(color="rgb(68,114,196)", width=2)))
            fig_band.update_layout(title=f"{sel_p} — min/mean/max band")
            st.plotly_chart(fig_band, use_container_width=True)

        with st.expander("Aggregated data table"):
            st.dataframe(df_agg.tail(200))

# ── Arrow Flight tab ──────────────────────────────────────────────────────────
with tab_flight:
    st.subheader("Direct Arrow Flight connection to Go collector")
    st.caption(
        "Data fetched via gRPC Arrow Flight — bypasses NATS, zero-copy columnar transfer."
    )

    fl_datasets = fetch("/flight/datasets")
    if not fl_datasets:
        st.warning("Flight server not reachable yet (wait for a collect cycle or check FLIGHT_ENDPOINT).")
    else:
        st.write("**Available datasets:**")
        for d in fl_datasets:
            st.write(f"- **{d['name']}** — {d['total_batches']} batches stored")
            with st.expander(f"Schema: {d['name']}"):
                for f in d["fields"]:
                    st.write(f"  `{f['name']}`: {f['type']}")

        col_a, col_b = st.columns(2)
        with col_a:
            fl_raw = fetch("/flight/raw", limit=limit,
                           country=country_arg, parameter=parameter_arg)
            df_fl_raw = pd.DataFrame(fl_raw)
            st.write(f"**Raw via Flight** ({len(df_fl_raw)} rows)")
            if not df_fl_raw.empty:
                df_fl_raw["value"] = pd.to_numeric(df_fl_raw["value"], errors="coerce")
                fig_fl = px.box(df_fl_raw, x="parameter", y="value", color="parameter",
                                title="Value distribution (Flight data)")
                st.plotly_chart(fig_fl, use_container_width=True)

        with col_b:
            fl_agg = fetch("/flight/aggregated", limit=limit,
                           country=country_arg, parameter=parameter_arg)
            df_fl_agg = pd.DataFrame(fl_agg)
            st.write(f"**Aggregated via Flight** ({len(df_fl_agg)} rows)")
            if not df_fl_agg.empty:
                df_fl_agg["mean_value"] = pd.to_numeric(df_fl_agg["mean_value"], errors="coerce")
                fig_fl_a = px.bar(df_fl_agg, x="parameter", y="mean_value",
                                  color="country_code",
                                  title="Mean value per parameter (Flight)")
                st.plotly_chart(fig_fl_a, use_container_width=True)

        # Benchmark
        st.subheader("Flight benchmark")
        if st.button("Run benchmark (3 × raw + 3 × agg)"):
            cols = st.columns(2)
            for i, ds in enumerate(("raw", "agg")):
                bm = fetch_obj(f"/flight/benchmark?dataset={ds}&runs=3")
                if bm:
                    cols[i].metric(f"{ds} avg latency", f"{bm.get('avg_latency_ms',0):.1f} ms")
                    cols[i].metric(f"{ds} throughput",  f"{bm.get('throughput_rows_s',0):.0f} rows/s")
                    cols[i].metric(f"{ds} bandwidth",   f"{bm.get('throughput_mb_s',0):.3f} MB/s")

# ── Compression tab ───────────────────────────────────────────────────────────
with tab_cmp:
    st.subheader("Data reduction by tumbling-window aggregation")
    agg_stat = fetch_obj("/aggregated/stats")
    if not agg_stat:
        st.info("No aggregated stats yet.")
    else:
        rows = [{"parameter": p,
                 "windows":   v.get("windows", 0),
                 "raw_total": v.get("raw_total", 0),
                 "overall_mean": round(v.get("overall_mean") or 0, 3)}
                for p, v in agg_stat.items()]
        df_stat = pd.DataFrame(rows)
        c_a, c_b = st.columns(2)
        with c_a:
            st.plotly_chart(px.bar(df_stat, x="parameter", y="raw_total",
                                   title="Raw readings aggregated per parameter"),
                            use_container_width=True)
        with c_b:
            st.plotly_chart(px.bar(df_stat, x="parameter", y="overall_mean",
                                   title="Overall mean per parameter"),
                            use_container_width=True)
        st.caption("Each window compresses N raw readings into one row per (country, parameter).")

if auto_refresh:
    time.sleep(REFRESH_SEC)
    st.rerun()
