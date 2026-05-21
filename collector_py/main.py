"""Standalone Python collector — async OpenAQ fetcher with NATS publishing.

Usage:
    python -m collector_py.main \\
        --countries US,GB,DE,PL \\
        --cycles 3 \\
        --nats-url nats://localhost:4222 \\
        --output results/python_metrics.json
"""
import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

import aiohttp

from .fetcher   import AsyncFetcher, make_session
from .metrics   import PerfCollector
from .publisher import encode_to_arrow, publish

ALL_COUNTRIES = [
    "US", "GB", "DE", "FR", "PL", "NL", "IN", "AU", "CA", "BR",
    "JP", "KR", "ZA", "MX", "IT", "ES", "SE", "NO", "DK", "CZ",
]


async def run_cycle(
    session:    aiohttp.ClientSession,
    fetcher:    AsyncFetcher,
    perf:       PerfCollector,
    countries:  list[str],
    nc,
) -> None:
    """Fetch all countries concurrently (one asyncio.gather call per cycle)."""
    async def fetch_one(country: str) -> None:
        t0 = time.perf_counter()
        measurements = await fetcher.fetch_measurements(session, country)
        dur_ms = (time.perf_counter() - t0) * 1000
        encoded = encode_to_arrow(measurements)
        if nc:
            await publish(nc, country, measurements)
        s = perf.record(country, dur_ms, len(measurements), len(encoded))
        print(
            f"  {country}: {s.record_count:4d} records  "
            f"{s.duration_ms:7.1f} ms  "
            f"{s.rss_mb:6.1f} MiB RSS  "
            f"{s.cpu_percent:5.1f}% CPU",
            flush=True,
        )

    await asyncio.gather(*[fetch_one(c) for c in countries])


async def main_async(args: argparse.Namespace) -> dict:
    countries = [c.strip().upper() for c in args.countries.split(",") if c.strip()]
    api_key   = args.api_key or os.getenv("OPENAQ_API_KEY", "")
    perf      = PerfCollector()
    fetcher   = AsyncFetcher(api_key=api_key, collector_id="python")

    # Optional NATS connection
    nc = None
    if args.nats_url:
        try:
            import nats
            nc = await nats.connect(args.nats_url)
            print(f"[collector_py] NATS connected: {args.nats_url}")
        except Exception as e:
            print(f"[collector_py] NATS unavailable ({e}), publishing disabled.")

    async with make_session(api_key) as session:
        for cycle in range(1, args.cycles + 1):
            print(f"\n── Cycle {cycle}/{args.cycles} ──────────────────────────────")
            await run_cycle(session, fetcher, perf, countries, nc)

    if nc:
        await nc.drain()

    summary = perf.summary()
    print(f"\n── Summary ──────────────────────────────────────────────")
    print(f"  Fetches:        {summary['fetch_count']}")
    print(f"  Total records:  {summary['total_records']}")
    print(f"  Total time:     {summary['total_ms']:.0f} ms")
    print(f"  Avg duration:   {summary['avg_duration_ms']:.1f} ms / country")
    print(f"  Records/sec:    {summary['records_per_sec']:.1f}")
    print(f"  MB/sec (enc):   {summary['mb_per_sec']:.4f}")
    print(f"  Avg RSS:        {summary['avg_rss_mb']:.1f} MiB")
    print(f"  Peak RSS:       {summary['peak_rss_mb']:.1f} MiB")
    print(f"  Avg CPU:        {summary['avg_cpu_percent']:.1f}%")

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
        print(f"\n[collector_py] Metrics saved → {out}")

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Python async OpenAQ collector")
    parser.add_argument("--countries",  default=",".join(ALL_COUNTRIES[:10]),
                        help="Comma-separated country codes")
    parser.add_argument("--cycles",     type=int, default=3,
                        help="Number of fetch cycles")
    parser.add_argument("--nats-url",   default="",
                        help="NATS URL (empty = skip publishing)")
    parser.add_argument("--api-key",    default="",
                        help="OpenAQ API key (or set OPENAQ_API_KEY)")
    parser.add_argument("--output",     default="benchmark/results/python_metrics.json",
                        help="Path for JSON metrics output")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
