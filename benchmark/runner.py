"""Benchmark runner — collects metrics from both Python and Go collectors.

Workflow:
  1. Run the Python collector in-process (pure async, measured with psutil).
  2. Read Go collector metrics from its HTTP /metrics endpoint
     (the Go collector must be running; start it with `make run-local` or
     deploy to Kubernetes first).
  3. Save combined results to benchmark/results/TIMESTAMP.json.

Usage:
    # Run Python benchmark only (Go collector not needed):
    python -m benchmark.runner --lang python --countries US,GB,DE,PL --cycles 3

    # Compare both (Go collector must be running):
    python -m benchmark.runner --lang both --go-url http://localhost:8081/metrics

    # Full comparison, save JSON, generate HTML report:
    python -m benchmark.runner --lang both && python -m benchmark.report
"""
import argparse
import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

RESULTS_DIR = Path(__file__).parent / "results"


# ── Go metrics reader ─────────────────────────────────────────────────────────

def collect_go_perf(metrics_url: str) -> dict:
    """Read performance summary from a running Go collector's /metrics endpoint."""
    try:
        resp = requests.get(metrics_url, timeout=10)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        print(f"[runner] Cannot reach Go metrics at {metrics_url}: {e}")
        return {}

    fetches = raw.get("fetches", [])
    if not fetches:
        print("[runner] Go /metrics returned no fetch samples yet.")
        return {}

    total_records  = sum(f.get("record_count",   0) for f in fetches)
    total_bytes    = sum(f.get("bytes_published", 0) for f in fetches)
    total_ms       = sum(f.get("duration_ms",    0.0) for f in fetches)
    n              = len(fetches)
    # mem_alloc_bytes is Go heap allocated (not RSS) — noted in report
    avg_mem_bytes  = sum(f.get("mem_alloc_bytes", 0) for f in fetches) / n
    avg_dur_ms     = total_ms / n
    records_per_s  = total_records / (total_ms / 1000) if total_ms else 0
    mb_per_s       = total_bytes   / (total_ms / 1000) / 1e6 if total_ms else 0

    # Per-country table
    by_country: dict[str, dict] = {}
    for f in fetches:
        c = f.get("country", "?")
        e = by_country.setdefault(c, {"duration_ms": 0.0, "record_count": 0, "count": 0})
        e["duration_ms"]  += f.get("duration_ms", 0.0)
        e["record_count"] += f.get("record_count", 0)
        e["count"]        += 1
    samples = [
        {
            "country":      c,
            "duration_ms":  round(v["duration_ms"] / v["count"], 2),
            "record_count": v["record_count"] // v["count"],
            "mem_alloc_mb": round(avg_mem_bytes / 1024 / 1024, 2),
            "cpu_percent":  0.0,   # Go doesn't export CPU% per-fetch
        }
        for c, v in by_country.items()
    ]

    return {
        "lang":            "go",
        "fetch_count":     n,
        "total_records":   total_records,
        "total_bytes":     total_bytes,
        "total_ms":        round(total_ms, 2),
        "avg_duration_ms": round(avg_dur_ms, 2),
        "avg_rss_mb":      round(avg_mem_bytes / 1024 / 1024, 2),  # heap alloc, not RSS
        "peak_rss_mb":     round(avg_mem_bytes / 1024 / 1024, 2),
        "avg_cpu_percent": 0.0,
        "records_per_sec": round(records_per_s, 2),
        "mb_per_sec":      round(mb_per_s, 4),
        "note_mem":        "Go reports heap alloc (runtime.ReadMemStats), not RSS",
        "samples":         samples,
    }


# ── Python benchmark ──────────────────────────────────────────────────────────

async def _collect_python_perf(
    countries:   list[str],
    cycles:      int,
    api_key:     str,
) -> dict:
    from collector_py.fetcher   import AsyncFetcher, make_session
    from collector_py.metrics   import PerfCollector
    from collector_py.publisher import encode_to_arrow

    perf    = PerfCollector()
    fetcher = AsyncFetcher(api_key=api_key, collector_id="python-bench")

    async with make_session(api_key) as session:
        for cycle in range(1, cycles + 1):
            print(f"  [py] cycle {cycle}/{cycles}", flush=True)

            async def fetch_one(country: str) -> None:
                t0 = time.perf_counter()
                ms = await fetcher.fetch_measurements(session, country)
                dur = (time.perf_counter() - t0) * 1000
                enc = encode_to_arrow(ms)
                s = perf.record(country, dur, len(ms), len(enc))
                print(
                    f"       {country}: {s.record_count:4d} rec  "
                    f"{s.duration_ms:7.1f} ms  "
                    f"{s.rss_mb:6.1f} MiB  {s.cpu_percent:.1f}% CPU",
                    flush=True,
                )

            await asyncio.gather(*[fetch_one(c) for c in countries])

    return perf.summary()


def collect_python_perf(countries: list[str], cycles: int, api_key: str) -> dict:
    return asyncio.run(_collect_python_perf(countries, cycles, api_key))


# ── Main ──────────────────────────────────────────────────────────────────────

def save_results(results: dict, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    print(f"\n[runner] Results saved → {output}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Go vs Python collector benchmark")
    parser.add_argument(
        "--lang", choices=["python", "go", "both"], default="both",
        help="Which collector to benchmark (default: both)",
    )
    parser.add_argument(
        "--countries", default="US,GB,DE,FR,PL,IN,AU,CA,BR,JP",
        help="Comma-separated country codes (same list used for both collectors)",
    )
    parser.add_argument(
        "--cycles", type=int, default=3,
        help="Python fetch cycles (Go samples are read from its live /metrics)",
    )
    parser.add_argument(
        "--go-url", default="http://localhost:8081/metrics",
        help="URL of the running Go collector's /metrics endpoint",
    )
    parser.add_argument(
        "--api-key", default="",
        help="OpenAQ API key (or set OPENAQ_API_KEY env var)",
    )
    parser.add_argument(
        "--output", default="",
        help="Output JSON path (default: benchmark/results/<timestamp>.json)",
    )
    args = parser.parse_args()

    countries = [c.strip().upper() for c in args.countries.split(",") if c.strip()]
    api_key   = args.api_key or os.getenv("OPENAQ_API_KEY", "")
    ts        = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path  = Path(args.output) if args.output else RESULTS_DIR / f"benchmark_{ts}.json"

    results: dict = {
        "timestamp":  ts,
        "countries":  countries,
        "cycles":     args.cycles,
    }

    if args.lang in ("python", "both"):
        print(f"\n{'='*60}")
        print(f"Python collector — {len(countries)} countries × {args.cycles} cycles")
        print(f"{'='*60}")
        results["python"] = collect_python_perf(countries, args.cycles, api_key)

    if args.lang in ("go", "both"):
        print(f"\n{'='*60}")
        print(f"Go collector — reading from {args.go_url}")
        print(f"{'='*60}")
        go_perf = collect_go_perf(args.go_url)
        if go_perf:
            results["go"] = go_perf
        else:
            print("[runner] Go metrics unavailable — skipping Go entry in results.")

    save_results(results, out_path)

    if "python" in results and "go" in results:
        _print_comparison(results["python"], results["go"])

    print(f"\nGenerate report: python -m benchmark.report --input {out_path}")


def _print_comparison(py: dict, go: dict) -> None:
    print(f"\n{'='*60}")
    print(f"{'Metric':<30} {'Python':>12} {'Go':>12}")
    print(f"{'-'*54}")
    metrics = [
        ("Avg duration / country (ms)",  "avg_duration_ms",  "{:.1f}"),
        ("Total records",                "total_records",     "{:d}"),
        ("Records / sec",                "records_per_sec",   "{:.1f}"),
        ("MB / sec (encoded)",           "mb_per_sec",        "{:.4f}"),
        ("Avg memory (MiB)",             "avg_rss_mb",        "{:.1f}"),
        ("Avg CPU %",                    "avg_cpu_percent",   "{:.1f}"),
    ]
    for label, key, fmt in metrics:
        pv = py.get(key, 0)
        gv = go.get(key, 0)
        pf = fmt.format(pv)
        gf = fmt.format(gv) if gv else "n/a"
        print(f"  {label:<28} {pf:>12} {gf:>12}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
