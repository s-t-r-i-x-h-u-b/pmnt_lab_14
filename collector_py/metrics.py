"""Runtime performance collector for the Python collector.

Measures wall-clock fetch time, heap RSS, and CPU% per fetch cycle using psutil.
Mirrors the fields exposed by Go's metrics.go (FetchResult JSON) so the benchmark
runner can compare them side-by-side.
"""
import os
import time
from dataclasses import dataclass, field

import psutil


@dataclass
class FetchSample:
    country:        str
    duration_ms:    float
    record_count:   int
    bytes_encoded:  int
    rss_mb:         float   # resident set size in MiB
    cpu_percent:    float   # process CPU% at sample time
    timestamp:      float   = field(default_factory=time.time)


class PerfCollector:
    """Collect per-fetch performance samples."""

    def __init__(self) -> None:
        self._proc    = psutil.Process(os.getpid())
        self._samples: list[FetchSample] = []
        # Warm up cpu_percent (first call always returns 0.0)
        self._proc.cpu_percent(interval=None)

    def record(
        self,
        country:       str,
        duration_ms:   float,
        record_count:  int,
        bytes_encoded: int,
    ) -> FetchSample:
        mem = self._proc.memory_info().rss / 1024 / 1024
        cpu = self._proc.cpu_percent(interval=None)
        s   = FetchSample(country, duration_ms, record_count, bytes_encoded, mem, cpu)
        self._samples.append(s)
        return s

    def samples(self) -> list[FetchSample]:
        return list(self._samples)

    def summary(self) -> dict:
        if not self._samples:
            return {}
        n               = len(self._samples)
        total_records   = sum(s.record_count  for s in self._samples)
        total_bytes     = sum(s.bytes_encoded for s in self._samples)
        total_ms        = sum(s.duration_ms   for s in self._samples)
        avg_rss_mb      = sum(s.rss_mb        for s in self._samples) / n
        peak_rss_mb     = max(s.rss_mb        for s in self._samples)
        avg_cpu         = sum(s.cpu_percent   for s in self._samples) / n
        avg_dur_ms      = total_ms / n
        records_per_sec = total_records / (total_ms / 1000) if total_ms else 0
        mb_per_sec      = total_bytes   / (total_ms / 1000) / 1e6 if total_ms else 0

        return {
            "lang":             "python",
            "fetch_count":      n,
            "total_records":    total_records,
            "total_bytes":      total_bytes,
            "total_ms":         round(total_ms,        2),
            "avg_duration_ms":  round(avg_dur_ms,      2),
            "avg_rss_mb":       round(avg_rss_mb,      2),
            "peak_rss_mb":      round(peak_rss_mb,     2),
            "avg_cpu_percent":  round(avg_cpu,         2),
            "records_per_sec":  round(records_per_sec, 2),
            "mb_per_sec":       round(mb_per_sec,      4),
            "samples": [
                {
                    "country":       s.country,
                    "duration_ms":   round(s.duration_ms, 2),
                    "record_count":  s.record_count,
                    "bytes_encoded": s.bytes_encoded,
                    "rss_mb":        round(s.rss_mb,  2),
                    "cpu_percent":   round(s.cpu_percent, 2),
                    "timestamp":     s.timestamp,
                }
                for s in self._samples
            ],
        }
