#!/usr/bin/env python3
"""Standalone demo: connects directly to the Go Arrow Flight server and
prints a performance summary.  Run after `make run-local` starts the stack.

    python flight_demo.py [grpc://localhost:5005]
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from flight_client import AirQualityFlightClient


def main(endpoint: str = "grpc://localhost:5005") -> None:
    print(f"Connecting to Arrow Flight server at {endpoint}\n")

    with AirQualityFlightClient(endpoint) as client:
        # 1. List available datasets
        print("─── Available datasets ───────────────────────────────────")
        datasets = client.list_datasets()
        if not datasets:
            print("  (no datasets yet – wait for the first fetch cycle)")
            return
        for d in datasets:
            print(f"  {d.name:6s}  batches={d.total_records:4d}  "
                  f"schema fields={len(d.schema)}")
            for f in d.schema:
                print(f"           {f.name}: {f.type}")
        print()

        # 2. Fetch raw measurements
        print("─── Raw measurements (all countries) ─────────────────────")
        df_raw = client.get_raw()
        if df_raw.empty:
            print("  (no data yet)")
        else:
            print(f"  rows={len(df_raw)}  columns={list(df_raw.columns)}")
            print(df_raw[["country_code", "parameter", "value", "unit"]].head(5).to_string())
        print()

        # 3. Fetch aggregated data
        print("─── Aggregated windows ───────────────────────────────────")
        df_agg = client.get_aggregated()
        if df_agg.empty:
            print("  (no windows closed yet)")
        else:
            print(f"  rows={len(df_agg)}  columns={list(df_agg.columns)}")
            print(df_agg[["country_code", "parameter", "count", "mean_value", "std_value"]].head(5).to_string())
        print()

        # 4. Benchmark
        print("─── Benchmark (3 runs each) ──────────────────────────────")
        for ds in ("raw", "agg"):
            bm = client.benchmark(ds, runs=3)
            print(f"  {ds:3s}  avg_rows={bm['avg_rows']:.0f}  "
                  f"avg_latency_ms={bm['avg_latency_ms']:.1f}  "
                  f"throughput={bm['throughput_rows_per_sec']:.0f} rows/s  "
                  f"{bm['throughput_mb_per_sec']:.3f} MB/s")
        print()

        # 5. Arrow-native use: zero-copy table
        print("─── Zero-copy PyArrow Table (no pandas conversion) ───────")
        table = client.get_raw_table(country="US")
        print(f"  country=US  rows={len(table)}  nbytes={table.nbytes:,}")
        if len(table):
            import pyarrow.compute as pc
            mean_pm25 = pc.mean(table.filter(
                pc.equal(table["parameter"], "pm25")
            )["value"]).as_py()
            print(f"  mean pm2.5 = {mean_pm25}")


if __name__ == "__main__":
    endpoint = sys.argv[1] if len(sys.argv) > 1 else "grpc://localhost:5005"
    main(endpoint)
