"""Streamlit dashboard — raw measurements, aggregated windows, Arrow Flight direct access."""
import os
import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

API_URL        = os.getenv("API_URL",        "http://localhost:8000")
PUBLIC_API_URL = os.getenv("PUBLIC_API_URL", "http://localhost:8000")
REFRESH_SEC    = 30

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
tab_raw, tab_agg, tab_flight, tab_cmp, tab_kafka, tab_bench, tab_live = st.tabs([
    "Raw measurements", "Aggregated windows", "Arrow Flight (direct)", "Compression",
    "Kafka sliding window", "Go vs Python", "Live",
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

# ── Kafka sliding window tab ──────────────────────────────────────────────────
with tab_kafka:
    st.subheader("Kafka — скользящее окно 5 минут")

    kstats = fetch_obj("/kafka/stats")
    if not kstats.get("enabled"):
        st.warning(
            "Kafka-консьюмер не запущен.  Установите переменную окружения "
            "`KAFKA_BROKERS=kafka:9092` для analysis-api."
        )
    else:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Raw сообщений",      kstats.get("raw_total", 0))
        k2.metric("Agg сообщений",      kstats.get("agg_total", 0))
        k3.metric("Записей в окне",     kstats.get("window_entries", 0))
        k4.metric("Ошибок декодирования", kstats.get("errors", 0))

        st.caption(
            f"Скользящее окно: последние **{kstats.get('window_duration_s', 300) // 60} мин** "
            f"по event-time.  Последняя страна: **{kstats.get('last_raw_country', '—')}**"
        )

        # ── Window data table + charts ────────────────────────────────────────
        win_data = fetch("/kafka/window", country=country_arg, parameter=parameter_arg)
        df_win   = pd.DataFrame(win_data)

        if df_win.empty:
            st.info("Нет данных в скользящем окне — подождите первых сообщений от Kafka.")
        else:
            for col in ("mean_value", "min_value", "max_value", "std_value"):
                df_win[col] = pd.to_numeric(df_win[col], errors="coerce")

            st.markdown("#### Средние значения в скользящем окне")
            fig_mean = px.bar(
                df_win.sort_values("mean_value", ascending=False),
                x="parameter", y="mean_value", color="country_code",
                barmode="group",
                error_y="std_value",
                title="Среднее ± σ по параметрам (скользящее окно 5 мин)",
                labels={"mean_value": "Среднее значение", "parameter": "Параметр"},
            )
            st.plotly_chart(fig_mean, use_container_width=True)

            st.markdown("#### Количество измерений по (страна, параметр)")
            fig_cnt = px.bar(
                df_win.sort_values("count", ascending=False).head(30),
                x="parameter", y="count", color="country_code",
                barmode="stack",
                title="Измерений в окне по параметрам",
            )
            st.plotly_chart(fig_cnt, use_container_width=True)

            st.markdown("#### Min / Max разброс")
            fig_range = go.Figure()
            for _, row in df_win.iterrows():
                label = f"{row['country_code']}/{row['parameter']}"
                fig_range.add_trace(go.Scatter(
                    x=[label, label],
                    y=[row["min_value"], row["max_value"]],
                    mode="lines+markers",
                    name=label,
                    showlegend=False,
                    line=dict(width=3),
                ))
            fig_range.update_layout(
                title="Диапазон min–max по парам (страна, параметр)",
                xaxis_tickangle=-45,
                height=400,
            )
            st.plotly_chart(fig_range, use_container_width=True)

            with st.expander("Таблица скользящего окна"):
                st.dataframe(df_win)

        # ── Summary by parameter ──────────────────────────────────────────────
        win_sum = fetch_obj("/kafka/window/summary")
        if win_sum:
            st.markdown("#### Сводка по параметрам (все страны)")
            sum_rows = [
                {
                    "Параметр": p,
                    "Стран в окне": v.get("country_count", 0),
                    "Всего измерений": v.get("count_total", 0),
                    "Среднее": v.get("overall_mean"),
                }
                for p, v in win_sum.items()
            ]
            st.dataframe(pd.DataFrame(sum_rows).sort_values("Всего измерений", ascending=False))

        st.caption(
            "📌 Скользящее окно (sliding window): в любой момент времени содержит "
            "измерения с event-time в диапазоне [now − 5 мин, now].  "
            "В отличие от тамблинг-окна Go (фиксированные 60-секундные интервалы), "
            "скользящее окно даёт актуальную статистику без задержки."
        )


# ── Go vs Python benchmark tab ───────────────────────────────────────────────
with tab_bench:
    import glob
    import json as _json
    from pathlib import Path as _Path

    st.subheader("Go vs Python — сравнение производительности сборщика")
    st.caption(
        "Результаты запуска `python -m benchmark.runner`.  "
        "Файлы хранятся в `benchmark/results/`."
    )

    bench_dir = _Path("/app/benchmark/results")
    result_files = sorted(bench_dir.glob("benchmark_*.json"), reverse=True)

    if not result_files:
        st.info(
            "Результаты не найдены.  Запустите:\n"
            "```\npython -m benchmark.runner --lang python\n```\n"
            "или (при запущенном Go-сборщике):\n"
            "```\npython -m benchmark.runner --lang both --go-url http://collector:8080/metrics\n```"
        )
    else:
        sel_file = st.selectbox(
            "Файл результатов",
            result_files,
            format_func=lambda p: p.name,
        )
        data = _json.loads(sel_file.read_text())
        py_d = data.get("python", {})
        go_d = data.get("go",     {})

        # ── Summary metrics ────────────────────────────────────────────────────
        st.markdown("#### Сводные метрики")
        cols = st.columns(4)
        metric_pairs = [
            ("Ср. время / страна",  "avg_duration_ms",  "{:.1f} мс"),
            ("Записей / сек",       "records_per_sec",  "{:.1f}"),
            ("МБ / сек (IPC)",      "mb_per_sec",       "{:.4f}"),
            ("Пик памяти (МиБ)",    "peak_rss_mb",      "{:.1f}"),
        ]
        for i, (label, key, fmt) in enumerate(metric_pairs):
            pv = fmt.format(py_d.get(key, 0)) if py_d else "—"
            gv = fmt.format(go_d.get(key, 0)) if go_d else "—"
            cols[i].metric(f"🐍 {label}", pv, delta=f"Go: {gv}", delta_color="off")

        # ── Bar comparison ─────────────────────────────────────────────────────
        st.markdown("#### Сравнение ключевых показателей")
        bar_metrics = {
            "Ср. время / страна (мс)": "avg_duration_ms",
            "Записей / сек":           "records_per_sec",
            "Ср. память (МиБ)":        "avg_rss_mb",
            "Ср. CPU %":               "avg_cpu_percent",
        }
        bar_rows = []
        for label, key in bar_metrics.items():
            if py_d:
                bar_rows.append({"Метрика": label, "Значение": py_d.get(key, 0), "Язык": "Python"})
            if go_d:
                bar_rows.append({"Метрика": label, "Значение": go_d.get(key, 0), "Язык": "Go"})

        df_bar = pd.DataFrame(bar_rows)
        if not df_bar.empty:
            fig_bar = px.bar(
                df_bar, x="Метрика", y="Значение", color="Язык",
                barmode="group",
                color_discrete_map={"Python": "#3776AB", "Go": "#00ACD7"},
                title="Go vs Python — ключевые метрики",
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        # ── Per-country breakdown ──────────────────────────────────────────────
        st.markdown("#### Время выборки по странам (мс)")
        country_rows = []
        for lang, d in (("Python", py_d), ("Go", go_d)):
            for s in d.get("samples", []):
                country_rows.append({
                    "Страна":    s.get("country", "?"),
                    "Время (мс)": s.get("duration_ms", 0),
                    "Записей":   s.get("record_count", 0),
                    "Язык":      lang,
                })
        df_country = pd.DataFrame(country_rows)
        if not df_country.empty:
            df_avg = (
                df_country.groupby(["Страна", "Язык"])["Время (мс)"]
                .mean()
                .reset_index()
            )
            fig_c = px.bar(
                df_avg, x="Страна", y="Время (мс)", color="Язык",
                barmode="group",
                color_discrete_map={"Python": "#3776AB", "Go": "#00ACD7"},
                title="Среднее время выборки по странам",
            )
            st.plotly_chart(fig_c, use_container_width=True)

        # ── Memory over time ───────────────────────────────────────────────────
        st.markdown("#### Динамика памяти по выборкам")
        mem_rows = []
        for lang, d in (("Python (RSS МиБ)", py_d), ("Go (heap МиБ)", go_d)):
            for i, s in enumerate(d.get("samples", []), 1):
                mem_rows.append({"#": i, "МиБ": s.get("rss_mb", s.get("mem_alloc_mb", 0)), "Источник": lang})
        df_mem = pd.DataFrame(mem_rows)
        if not df_mem.empty:
            fig_mem = px.line(
                df_mem, x="#", y="МиБ", color="Источник",
                color_discrete_map={
                    "Python (RSS МиБ)": "#3776AB",
                    "Go (heap МиБ)":    "#00ACD7",
                },
                markers=True,
                title="Память по выборкам (Python=RSS, Go=heap alloc)",
            )
            st.plotly_chart(fig_mem, use_container_width=True)

        # ── Analysis text ──────────────────────────────────────────────────────
        with st.expander("Методологические примечания"):
            st.markdown("""
**Конкурентность:**
- Go-сборщик обходит локации **последовательно** с задержкой 150 мс между запросами.
- Python-сборщик использует `asyncio.gather` + семафор (5 параллельных запросов).
  При N=50 локациях это ~10× быстрее по I/O-задержке.

**Память:**
- Python: `psutil.Process().memory_info().rss` — RSS всего процесса CPython.
- Go: `runtime.ReadMemStats().Alloc` — только выделенная heap.
  Реальный RSS Go ~5–15 МиБ, что в разы меньше CPython.

**CPU:**
- Go: метрика CPU не измеряется на уровне отдельной выборки.
- Python: `psutil.Process().cpu_percent()` сразу после выборки.
""")

# ── Live real-time tab ────────────────────────────────────────────────────────
with tab_live:
    st.subheader("Мониторинг в реальном времени")
    st.caption(
        "Данные обновляются каждые 3 секунды.  "
        "Для по-настоящему потокового отображения без перезагрузки страницы "
        "используйте полноэкранный WebSocket-дашборд."
    )

    col_btn, col_ws = st.columns([1, 3])
    with col_btn:
        st.link_button(
            "Открыть Live WebSocket-дашборд",
            f"{PUBLIC_API_URL}/realtime",
            use_container_width=True,
        )
    with col_ws:
        st.code(f"WebSocket: ws://localhost:8000/ws/live  |  REST: {PUBLIC_API_URL}/ws/clients")

    st.divider()

    @st.fragment(run_every=3)
    def _live_fragment() -> None:
        live_stats  = fetch_obj("/stats")
        live_kstats = fetch_obj("/kafka/stats")

        # ── Metrics with delta ────────────────────────────────────────────────
        raw_now  = live_stats.get("raw_records", 0)
        agg_now  = live_stats.get("agg_records", 0)
        kwin_now = live_kstats.get("window_entries", 0) if live_kstats.get("enabled") else 0

        prev_raw  = st.session_state.get("_live_raw",  raw_now)
        prev_agg  = st.session_state.get("_live_agg",  agg_now)
        prev_kwin = st.session_state.get("_live_kwin", kwin_now)

        st.session_state["_live_raw"]  = raw_now
        st.session_state["_live_agg"]  = agg_now
        st.session_state["_live_kwin"] = kwin_now

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Raw records (NATS)",     raw_now,
                  delta=raw_now - prev_raw   if raw_now  != prev_raw  else None)
        m2.metric("Agg records (NATS)",     agg_now,
                  delta=agg_now - prev_agg   if agg_now  != prev_agg  else None)
        m3.metric("Kafka window entries",   kwin_now,
                  delta=kwin_now - prev_kwin if kwin_now != prev_kwin else None)
        m4.metric("WS clients",
                  fetch_obj("/ws/clients").get("connected_clients", 0))

        # ── Timeline: records per minute over last 30 min ────────────────────
        live_raw = fetch("/measurements", limit=5000)
        df_live  = pd.DataFrame(live_raw)
        if not df_live.empty and "timestamp" in df_live.columns:
            df_live["ts"] = pd.to_datetime(df_live["timestamp"], utc=True, errors="coerce")
            df_live = df_live.dropna(subset=["ts"])
            df_live["bucket"] = df_live["ts"].dt.floor("1min")
            now_utc = pd.Timestamp.now(tz="UTC")
            df_tl = (
                df_live[df_live["bucket"] >= now_utc - pd.Timedelta(minutes=30)]
                .groupby("bucket")
                .size()
                .reset_index(name="count")
            )
            if not df_tl.empty:
                fig_tl = go.Figure(go.Scatter(
                    x=df_tl["bucket"], y=df_tl["count"],
                    mode="lines", fill="tozeroy",
                    line=dict(color="#3b82f6", width=2),
                ))
                fig_tl.update_layout(
                    title="Измерений в минуту (последние 30 мин)",
                    height=200,
                    margin=dict(l=0, r=0, t=30, b=0),
                    showlegend=False,
                    xaxis=dict(showgrid=True),
                    yaxis=dict(showgrid=True),
                )
                st.plotly_chart(fig_tl, use_container_width=True)

        # ── Top countries + top parameters (side by side) ────────────────────
        countries_live = live_stats.get("countries", [])
        params_live    = live_stats.get("parameters", [])

        if not df_live.empty:
            ca, cb = st.columns(2)

            with ca:
                cc_counts = df_live["country_code"].value_counts().head(12).reset_index()
                cc_counts.columns = ["country_code", "count"]
                fig_cc = px.bar(
                    cc_counts, x="count", y="country_code", orientation="h",
                    title=f"Топ стран ({len(countries_live)} активных)",
                    labels={"count": "Измерений", "country_code": ""},
                )
                fig_cc.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0), showlegend=False)
                fig_cc.update_traces(marker_color="#3b82f6")
                st.plotly_chart(fig_cc, use_container_width=True)

            with cb:
                pm_counts = df_live["parameter"].value_counts().reset_index()
                pm_counts.columns = ["parameter", "count"]
                fig_pm = px.pie(
                    pm_counts, names="parameter", values="count",
                    title=f"Параметры ({len(params_live)} активных)",
                    hole=0.55,
                )
                fig_pm.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig_pm, use_container_width=True)

        # ── Last 20 raw measurements ──────────────────────────────────────────
        if not df_live.empty:
            show_cols = [c for c in
                         ["timestamp", "country_code", "parameter", "value", "unit", "location_name"]
                         if c in df_live.columns]
            st.dataframe(
                df_live[show_cols].tail(20).sort_values("timestamp", ascending=False),
                use_container_width=True,
                hide_index=True,
            )

        st.caption(f"Последнее обновление: {time.strftime('%H:%M:%S')}")

    _live_fragment()


if auto_refresh:
    time.sleep(REFRESH_SEC)
    st.rerun()
