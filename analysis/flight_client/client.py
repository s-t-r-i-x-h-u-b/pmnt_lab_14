"""Arrow Flight client for the Go air-quality collector.

Usage
-----
>>> from flight_client import AirQualityFlightClient
>>> client = AirQualityFlightClient("grpc://localhost:5005")
>>> df_raw = client.get_raw()                         # all raw measurements
>>> df_raw_us = client.get_raw(country="US")          # filter by country
>>> df_agg = client.get_aggregated(parameter="pm25")  # aggregated windows
>>> print(client.list_datasets())
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight as flight


@dataclass
class TicketRequest:
    dataset: str                     # "raw" or "agg"
    country: Optional[str] = None
    parameter: Optional[str] = None

    def to_ticket(self) -> flight.Ticket:
        payload = {"dataset": self.dataset}
        if self.country:
            payload["country"] = self.country
        if self.parameter:
            payload["parameter"] = self.parameter
        return flight.Ticket(json.dumps(payload).encode())


@dataclass
class DatasetInfo:
    name: str
    schema: pa.Schema
    total_records: int


class AirQualityFlightClient:
    """Arrow Flight client connecting to the Go collector's Flight gRPC server."""

    def __init__(self, endpoint: str = "grpc://localhost:5005") -> None:
        self._endpoint = endpoint
        self._client = flight.FlightClient(endpoint)

    # ── Discovery ─────────────────────────────────────────────────────────────

    def list_datasets(self) -> list[DatasetInfo]:
        """Return metadata for every dataset the server advertises."""
        results = []
        for info in self._client.list_flights():
            name = info.descriptor.path[0] if info.descriptor.path else "unknown"
            schema = flight.deserialize_schema(info.schema_bytes)
            results.append(DatasetInfo(
                name=name,
                schema=schema,
                total_records=info.total_records,
            ))
        return results

    def get_schema(self, dataset: str) -> pa.Schema:
        """Return the Arrow schema for *dataset* without downloading any data."""
        desc = flight.FlightDescriptor.for_path(dataset)
        result = self._client.get_schema(desc)
        return flight.deserialize_schema(result.schema_bytes)

    # ── Data access ───────────────────────────────────────────────────────────

    def get_raw(
        self,
        country: Optional[str] = None,
        parameter: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch raw Measurement RecordBatches and return as a pandas DataFrame.

        Filtering is applied client-side on the Arrow Table before conversion.
        """
        table = self._fetch_table("raw")
        return self._filter_and_convert(table, country, parameter)

    def get_aggregated(
        self,
        country: Optional[str] = None,
        parameter: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch aggregated window RecordBatches and return as a pandas DataFrame."""
        table = self._fetch_table("agg")
        return self._filter_and_convert(table, country, parameter)

    def get_raw_table(
        self,
        country: Optional[str] = None,
        parameter: Optional[str] = None,
    ) -> pa.Table:
        """Like get_raw() but returns the native PyArrow Table (zero-copy)."""
        table = self._fetch_table("raw")
        return self._filter_table(table, country, parameter)

    def get_aggregated_table(
        self,
        country: Optional[str] = None,
        parameter: Optional[str] = None,
    ) -> pa.Table:
        """Like get_aggregated() but returns the native PyArrow Table (zero-copy)."""
        table = self._fetch_table("agg")
        return self._filter_table(table, country, parameter)

    # ── Benchmarking ──────────────────────────────────────────────────────────

    def benchmark(self, dataset: str = "raw", runs: int = 3) -> dict:
        """Measure round-trip latency and throughput for *dataset*.

        Returns a dict with keys:
          runs, rows_per_run, bytes_per_run, avg_latency_ms, throughput_rows_per_sec
        """
        latencies = []
        rows_list = []
        bytes_list = []
        for _ in range(runs):
            t0 = time.perf_counter()
            table = self._fetch_table(dataset)
            elapsed = time.perf_counter() - t0
            latencies.append(elapsed * 1000)
            rows_list.append(len(table))
            bytes_list.append(table.nbytes)

        avg_lat = sum(latencies) / len(latencies)
        avg_rows = sum(rows_list) / len(rows_list)
        avg_bytes = sum(bytes_list) / len(bytes_list)
        return {
            "dataset": dataset,
            "runs": runs,
            "avg_rows": avg_rows,
            "avg_bytes": avg_bytes,
            "avg_latency_ms": round(avg_lat, 2),
            "throughput_rows_per_sec": round(avg_rows / (avg_lat / 1000), 1) if avg_lat > 0 else 0,
            "throughput_mb_per_sec": round(avg_bytes / (avg_lat / 1000) / 1e6, 3) if avg_lat > 0 else 0,
        }

    # ── Internals ─────────────────────────────────────────────────────────────

    def _fetch_table(self, dataset: str) -> pa.Table:
        ticket = TicketRequest(dataset=dataset).to_ticket()
        reader = self._client.do_get(ticket)
        return reader.read_all()

    @staticmethod
    def _filter_table(
        table: pa.Table,
        country: Optional[str],
        parameter: Optional[str],
    ) -> pa.Table:
        mask = None
        if country and "country_code" in table.schema.names:
            expr = pc.equal(table["country_code"], country)
            mask = expr if mask is None else pc.and_(mask, expr)
        if parameter and "parameter" in table.schema.names:
            expr = pc.equal(table["parameter"], parameter)
            mask = expr if mask is None else pc.and_(mask, expr)
        return table.filter(mask) if mask is not None else table

    @staticmethod
    def _filter_and_convert(
        table: pa.Table,
        country: Optional[str],
        parameter: Optional[str],
    ) -> pd.DataFrame:
        filtered = AirQualityFlightClient._filter_table(table, country, parameter)
        return filtered.to_pandas()

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
