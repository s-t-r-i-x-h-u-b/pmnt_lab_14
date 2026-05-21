"""Generate an interactive HTML benchmark report from benchmark/results/*.json.

Usage:
    # Latest results file:
    python -m benchmark.report

    # Specific input:
    python -m benchmark.report --input benchmark/results/benchmark_20260521.json

    # Custom output:
    python -m benchmark.report --input ... --output report.html
"""
import argparse
import json
import sys
from pathlib import Path

import plotly.graph_objects as go
from plotly.subplots import make_subplots

RESULTS_DIR = Path(__file__).parent / "results"

_COLORS = {"python": "#3776AB", "go": "#00ACD7"}  # Python blue, Go cyan


# ── Data helpers ──────────────────────────────────────────────────────────────

def _load_latest() -> dict:
    files = sorted(RESULTS_DIR.glob("benchmark_*.json"))
    if not files:
        sys.exit(
            "No benchmark results found.  Run:\n"
            "  python -m benchmark.runner --lang python"
        )
    return json.loads(files[-1].read_text(encoding="utf-8"))


def _per_country(data: dict, key: str) -> tuple[list[str], list[float]]:
    # Aggregate by country (mean) so multi-cycle Python samples collapse to one bar per country.
    sums: dict[str, float] = {}
    counts: dict[str, int] = {}
    order: list[str] = []
    for s in data.get("samples", []):
        c = s.get("country")
        if not c:
            continue
        if c not in sums:
            order.append(c)
            sums[c] = 0.0
            counts[c] = 0
        sums[c] += float(s.get(key, 0) or 0)
        counts[c] += 1
    return order, [sums[c] / counts[c] for c in order]


# ── Chart builders ────────────────────────────────────────────────────────────

def _bar(fig: go.Figure, row: int, col: int, results: dict, key: str, fmt: str = ".1f") -> None:
    for lang, color in _COLORS.items():
        if lang not in results:
            continue
        val = results[lang].get(key, 0)
        fig.add_trace(
            go.Bar(
                name=lang.capitalize(),
                x=[lang.capitalize()],
                y=[val],
                marker_color=color,
                text=[f"{val:{fmt}}"],
                textposition="outside",
                showlegend=(row == 1 and col == 1),
            ),
            row=row, col=col,
        )


def _country_bars(fig: go.Figure, row: int, col: int, results: dict, key: str) -> None:
    for lang, color in _COLORS.items():
        if lang not in results:
            continue
        countries, vals = _per_country(results[lang], key)
        fig.add_trace(
            go.Bar(
                name=lang.capitalize(),
                x=countries[:15],
                y=vals[:15],
                marker_color=color,
                showlegend=False,
            ),
            row=row, col=col,
        )


def _mem_line(fig: go.Figure, row: int, col: int, results: dict) -> None:
    for lang, color in _COLORS.items():
        if lang not in results:
            continue
        samples = results[lang].get("samples", [])
        if not samples:
            continue
        x = list(range(1, len(samples) + 1))
        y = [s.get("rss_mb", s.get("mem_alloc_mb", 0)) for s in samples]
        label = "MiB RSS" if lang == "python" else "MiB heap alloc"
        fig.add_trace(
            go.Scatter(
                name=f"{lang.capitalize()} ({label})",
                x=x, y=y,
                mode="lines+markers",
                line=dict(color=color, width=2),
                showlegend=True,
            ),
            row=row, col=col,
        )


def _duration_box(fig: go.Figure, row: int, col: int, results: dict) -> None:
    for lang, color in _COLORS.items():
        if lang not in results:
            continue
        durations = [s.get("duration_ms", 0) for s in results[lang].get("samples", [])]
        if not durations:
            continue
        fig.add_trace(
            go.Box(
                name=lang.capitalize(),
                y=durations,
                marker_color=color,
                boxmean=True,
                showlegend=False,
            ),
            row=row, col=col,
        )


# ── Summary table HTML ────────────────────────────────────────────────────────

def _summary_table(results: dict) -> str:
    # NOTE: use go_d / py_d locally — `go` is the imported plotly.graph_objects module.
    py_d = results.get("python", {})
    go_d = results.get("go", {})
    rows = [
        ("Всего записей",          py_d.get("total_records",   "-"), go_d.get("total_records",   "-")),
        ("Всего выборок",          py_d.get("fetch_count",      "-"), go_d.get("fetch_count",      "-")),
        ("Общее время (мс)",       f"{py_d.get('total_ms',0):.0f}"    if py_d else "-",
                                   f"{go_d.get('total_ms',0):.0f}"    if go_d else "-"),
        ("Ср. время / страна (мс)",f"{py_d.get('avg_duration_ms',0):.1f}" if py_d else "-",
                                   f"{go_d.get('avg_duration_ms',0):.1f}" if go_d else "-"),
        ("Записей / сек",          f"{py_d.get('records_per_sec',0):.1f}" if py_d else "-",
                                   f"{go_d.get('records_per_sec',0):.1f}" if go_d else "-"),
        ("МБ / сек (IPC-кодирование)", f"{py_d.get('mb_per_sec',0):.4f}" if py_d else "-",
                                       f"{go_d.get('mb_per_sec',0):.4f}" if go_d else "-"),
        ("Ср. память (МиБ)",       f"{py_d.get('avg_rss_mb',0):.1f} (RSS)"   if py_d else "-",
                                   f"{go_d.get('avg_rss_mb',0):.1f} (heap)"  if go_d else "-"),
        ("Пик памяти (МиБ)",       f"{py_d.get('peak_rss_mb',0):.1f}"        if py_d else "-",
                                   f"{go_d.get('peak_rss_mb',0):.1f}"        if go_d else "-"),
        ("Ср. CPU %",              f"{py_d.get('avg_cpu_percent',0):.1f}"     if py_d else "-",
                                   "—  (не измеряется на уровне выборки)"  if go_d else "-"),
    ]
    header = """
<table style="border-collapse:collapse;font-family:sans-serif;font-size:14px;margin:20px auto">
  <thead>
    <tr style="background:#222;color:#fff">
      <th style="padding:8px 16px;text-align:left">Метрика</th>
      <th style="padding:8px 16px;text-align:right;color:#3776AB">Python (asyncio)</th>
      <th style="padding:8px 16px;text-align:right;color:#00ACD7">Go (goroutines)</th>
    </tr>
  </thead><tbody>"""
    body = ""
    for i, (label, pv, gv) in enumerate(rows):
        bg = "#f9f9f9" if i % 2 == 0 else "#fff"
        body += (
            f'<tr style="background:{bg}">'
            f'<td style="padding:6px 16px">{label}</td>'
            f'<td style="padding:6px 16px;text-align:right;color:#3776AB"><b>{pv}</b></td>'
            f'<td style="padding:6px 16px;text-align:right;color:#00ACD7"><b>{gv}</b></td>'
            f"</tr>"
        )
    note_mem = go_d.get("note_mem", "")
    footer = (
        f'<tr><td colspan="3" style="padding:6px 16px;font-size:12px;color:#666">'
        f"⚠ {note_mem}</td></tr>"
        if note_mem else ""
    )
    return f"{header}{body}{footer}</tbody></table>"


_ANALYSIS = """
<div style="font-family:sans-serif;max-width:860px;margin:24px auto;line-height:1.7;font-size:15px">
<h2>Анализ результатов</h2>
<h3>I/O-bound нагрузка: Python async vs Go sequential</h3>
<p>Go-сборщик использует <b>последовательный</b> обход локаций с задержкой 150 мс между запросами
(стандартный rate-limit courtesy).  Python-сборщик использует <code>asyncio.gather</code> +
семафор на 5 одновременных запросов.  При большом числе локаций это даёт Python значительное
преимущество по времени: <em>N последовательных × 150 мс ≫ N/5 параллельных × T_response</em>.</p>

<h3>CPU и память</h3>
<p>Go значительно эффективнее по памяти: его рантайм занимает несколько МиБ heap, тогда как
CPython интерпретатор + сборщик мусора + импорты занимают 50–100 МиБ RSS даже без единой выборки.
По CPU Go также предпочтительнее для CPU-bound работы (Arrow-кодирование), но при I/O-bound
профиле разница несущественна.</p>

<h3>Масштабируемость</h3>
<p>Go идеально подходит для <em>горизонтального масштабирования</em> через goroutine-шарды
(как реализовано с etcd).  Python async хорош для <em>вертикального</em> масштабирования I/O
(много одновременных соединений), но ограничен GIL для CPU-параллелизма.</p>

<h3>Вывод</h3>
<ul>
  <li><b>Python async &gt; Go sequential</b> — для чистой I/O-latency при параллельных HTTP-запросах.</li>
  <li><b>Go &gt; Python</b> — по памяти, CPU и предсказуемой задержке (нет GC-пауз).</li>
  <li>В продакшн-пайплайне: Go-сборщик оптимален как <em>координируемый multi-instance</em>
      сервис; Python подходит для ad-hoc выборок и анализа.</li>
</ul>
</div>
"""


# ── Main report builder ───────────────────────────────────────────────────────

def generate_report(results: dict, output_path: Path) -> None:
    has_py = "python" in results
    has_go = "go"     in results

    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=[
            "Среднее время выборки / страна (мс) ↓ меньше = лучше",
            "Пропускная способность (записей / сек) ↑ больше = лучше",
            "Распределение длительности выборки (мс)",
            "Память (МиБ) по выборкам",
            "Суммарные байты Arrow IPC по странам",
            "Среднее время по странам (мс)",
        ],
        vertical_spacing=0.14,
        horizontal_spacing=0.1,
    )

    # Row 1: avg duration bar + records/sec bar
    _bar(fig, 1, 1, results, "avg_duration_ms", ".1f")
    _bar(fig, 1, 2, results, "records_per_sec", ".1f")

    # Row 2: duration box plot + memory line
    _duration_box(fig, 2, 1, results)
    _mem_line(fig,     2, 2, results)

    # Row 3: bytes per country bars + duration per country
    _country_bars(fig, 3, 1, results, "bytes_encoded")
    _country_bars(fig, 3, 2, results, "duration_ms")

    fig.update_layout(
        title_text=(
            f"Go vs Python — Сравнение производительности сборщика "
            f"({results.get('timestamp', '')})"
        ),
        title_font_size=18,
        height=1100,
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        barmode="group",
    )
    fig.update_yaxes(title_text="мс",       row=1, col=1)
    fig.update_yaxes(title_text="записей/с", row=1, col=2)
    fig.update_yaxes(title_text="мс",       row=2, col=1)
    fig.update_yaxes(title_text="МиБ",      row=2, col=2)
    fig.update_yaxes(title_text="байт",     row=3, col=1)
    fig.update_yaxes(title_text="мс",       row=3, col=2)

    chart_html = fig.to_html(include_plotlyjs="cdn", full_html=False)
    table_html = _summary_table(results)

    html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>Go vs Python — Benchmark Report</title>
  <style>
    body {{ margin: 0; background: #fafafa; }}
    h1   {{ text-align:center; font-family:sans-serif; padding:24px 0 0; }}
    p.sub{{ text-align:center; font-family:sans-serif; color:#555; margin:4px 0 16px; }}
  </style>
</head>
<body>
  <h1>Go vs Python — Сравнение производительности сборщика данных</h1>
  <p class="sub">
    Страны: {", ".join(results.get("countries", []))} &nbsp;|&nbsp;
    Циклов Python: {results.get("cycles", "?")}
  </p>
  {table_html}
  {chart_html}
  {_ANALYSIS}
</body>
</html>"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    print(f"[report] Report saved → {output_path}")
    print(f"         Open in browser: file://{output_path.resolve()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate benchmark HTML report")
    parser.add_argument("--input",  default="",
                        help="JSON results file (default: latest in benchmark/results/)")
    parser.add_argument("--output", default="",
                        help="HTML output file (default: same dir as input)")
    args = parser.parse_args()

    if args.input:
        results = json.loads(Path(args.input).read_text(encoding="utf-8"))
        in_path = Path(args.input)
    else:
        files = sorted(RESULTS_DIR.glob("benchmark_*.json"))
        if not files:
            sys.exit("No results found.  Run: python -m benchmark.runner")
        in_path = files[-1]
        results = json.loads(in_path.read_text(encoding="utf-8"))
        print(f"[report] Using latest results: {in_path}")

    out_path = Path(args.output) if args.output else in_path.with_suffix(".html")
    generate_report(results, out_path)


if __name__ == "__main__":
    main()
