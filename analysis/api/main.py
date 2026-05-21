"""FastAPI service.

Data sources:
  - NATS  air.quality.*  → raw measurements store
  - NATS  air.agg.*      → aggregated window store
  - Arrow Flight         → direct pull from Go collector (via /flight/* endpoints)
"""
import asyncio
import io
import os
from collections import deque
from contextlib import asynccontextmanager
from typing import Any, Optional

import nats
import pyarrow as pa
from fastapi import FastAPI, Query

NATS_URL      = os.getenv("NATS_URL",        "nats://localhost:4222")
FLIGHT_ENDPOINT = os.getenv("FLIGHT_ENDPOINT", "grpc://localhost:5005")
MAX_RECORDS   = int(os.getenv("MAX_RECORDS",  "50000"))

raw_store: deque[dict[str, Any]] = deque(maxlen=MAX_RECORDS)
agg_store: deque[dict[str, Any]] = deque(maxlen=MAX_RECORDS)
raw_lock = asyncio.Lock()
agg_lock = asyncio.Lock()

nc_handle: nats.aio.client.Client | None = None


def _decode_arrow(data: bytes) -> list[dict[str, Any]]:
    reader = pa.ipc.open_stream(io.BytesIO(data))
    rows: list[dict[str, Any]] = []
    for batch in reader:
        rows.extend(batch.to_pandas().to_dict(orient="records"))
    return rows


async def handle_raw(msg: nats.aio.msg.Msg) -> None:
    try:
        async with raw_lock:
            raw_store.extend(_decode_arrow(msg.data))
    except Exception as exc:
        print(f"[raw] decode error: {exc}")


async def handle_agg(msg: nats.aio.msg.Msg) -> None:
    try:
        async with agg_lock:
            agg_store.extend(_decode_arrow(msg.data))
    except Exception as exc:
        print(f"[agg] decode error: {exc}")


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ANN001
    global nc_handle
    nc_handle = await nats.connect(NATS_URL)
    await nc_handle.subscribe("air.quality.*", cb=handle_raw)
    await nc_handle.subscribe("air.agg.*",     cb=handle_agg)
    print(f"[startup] NATS connected: {NATS_URL}")
    print(f"[startup] Flight endpoint: {FLIGHT_ENDPOINT}")
    yield
    if nc_handle:
        await nc_handle.drain()


app = FastAPI(title="Air Quality API", lifespan=lifespan)


# ── NATS-based endpoints ──────────────────────────────────────────────────────

@app.get("/measurements")
async def get_measurements(
    country:   Optional[str] = Query(None),
    parameter: Optional[str] = Query(None),
    limit:     int            = Query(200, le=5000),
) -> list[dict[str, Any]]:
    async with raw_lock:
        data = list(raw_store)
    if country:
        data = [r for r in data if r.get("country_code") == country]
    if parameter:
        data = [r for r in data if r.get("parameter") == parameter]
    return data[-limit:]


@app.get("/aggregated")
async def get_aggregated(
    country:   Optional[str] = Query(None),
    parameter: Optional[str] = Query(None),
    limit:     int            = Query(200, le=5000),
) -> list[dict[str, Any]]:
    async with agg_lock:
        data = list(agg_store)
    if country:
        data = [r for r in data if r.get("country_code") == country]
    if parameter:
        data = [r for r in data if r.get("parameter") == parameter]
    return data[-limit:]


@app.get("/countries")
async def get_countries() -> list[str]:
    async with raw_lock:
        return sorted({r.get("country_code", "") for r in raw_store if r.get("country_code")})


@app.get("/stats")
async def get_stats() -> dict[str, Any]:
    async with raw_lock:
        raw_total  = len(raw_store)
        countries  = sorted({r.get("country_code") for r in raw_store if r.get("country_code")})
        parameters = sorted({r.get("parameter")    for r in raw_store if r.get("parameter")})
    async with agg_lock:
        agg_total = len(agg_store)
    return {"raw_records": raw_total, "agg_records": agg_total,
            "countries": countries, "parameters": parameters}


@app.get("/aggregated/stats")
async def get_agg_stats() -> dict[str, Any]:
    async with agg_lock:
        data = list(agg_store)
    by_param: dict[str, dict[str, Any]] = {}
    for r in data:
        p = r.get("parameter", "unknown")
        e = by_param.setdefault(p, {"windows": 0, "raw_total": 0, "mean_values": []})
        e["windows"]   += 1
        e["raw_total"] += r.get("count", 0)
        mv = r.get("mean_value")
        if mv is not None:
            e["mean_values"].append(mv)
    for v in by_param.values():
        mv = v.pop("mean_values")
        v["overall_mean"] = sum(mv) / len(mv) if mv else None
    return by_param


# ── Arrow Flight endpoints ────────────────────────────────────────────────────

def _flight_client():
    import pyarrow.flight as fl
    return fl.FlightClient(FLIGHT_ENDPOINT)


@app.get("/flight/datasets")
async def flight_datasets() -> list[dict[str, Any]]:
    """List datasets advertised by the Go Flight server."""
    import pyarrow.flight as fl
    client = _flight_client()
    results = []
    try:
        for info in client.list_flights():
            name = info.descriptor.path[0] if info.descriptor.path else "?"
            schema = fl.deserialize_schema(info.schema_bytes)
            results.append({
                "name":          name,
                "total_batches": info.total_records,
                "fields":        [{"name": f.name, "type": str(f.type)} for f in schema],
            })
    finally:
        client.close()
    return results


@app.get("/flight/raw")
async def flight_raw(
    country:   Optional[str] = Query(None),
    parameter: Optional[str] = Query(None),
    limit:     int            = Query(500, le=10000),
) -> list[dict[str, Any]]:
    """Fetch raw measurements directly from the Go Arrow Flight server."""
    import json
    import pyarrow.compute as pc
    import pyarrow.flight as fl

    client = _flight_client()
    try:
        payload = json.dumps({"dataset": "raw"}).encode()
        reader = client.do_get(fl.Ticket(payload))
        table = reader.read_all()
    finally:
        client.close()

    if country and "country_code" in table.schema.names:
        table = table.filter(pc.equal(table["country_code"], country))
    if parameter and "parameter" in table.schema.names:
        table = table.filter(pc.equal(table["parameter"], parameter))
    table = table.slice(max(0, len(table) - limit))
    return table.to_pandas().to_dict(orient="records")


@app.get("/flight/aggregated")
async def flight_aggregated(
    country:   Optional[str] = Query(None),
    parameter: Optional[str] = Query(None),
    limit:     int            = Query(500, le=10000),
) -> list[dict[str, Any]]:
    """Fetch aggregated windows directly from the Go Arrow Flight server."""
    import json
    import pyarrow.compute as pc
    import pyarrow.flight as fl

    client = _flight_client()
    try:
        payload = json.dumps({"dataset": "agg"}).encode()
        reader = client.do_get(fl.Ticket(payload))
        table = reader.read_all()
    finally:
        client.close()

    if country and "country_code" in table.schema.names:
        table = table.filter(pc.equal(table["country_code"], country))
    if parameter and "parameter" in table.schema.names:
        table = table.filter(pc.equal(table["parameter"], parameter))
    table = table.slice(max(0, len(table) - limit))
    return table.to_pandas().to_dict(orient="records")


@app.get("/flight/benchmark")
async def flight_benchmark(
    dataset: str = Query("raw", description="raw or agg"),
    runs:    int = Query(3,     ge=1, le=10),
) -> dict[str, Any]:
    """Run a latency/throughput benchmark against the Go Flight server."""
    import json
    import time
    import pyarrow.flight as fl

    client = _flight_client()
    latencies, rows_list, bytes_list = [], [], []
    try:
        for _ in range(runs):
            payload = json.dumps({"dataset": dataset}).encode()
            t0 = time.perf_counter()
            reader = client.do_get(fl.Ticket(payload))
            table = reader.read_all()
            latencies.append((time.perf_counter() - t0) * 1000)
            rows_list.append(len(table))
            bytes_list.append(table.nbytes)
    finally:
        client.close()

    avg_lat   = sum(latencies) / len(latencies)
    avg_rows  = sum(rows_list) / len(rows_list)
    avg_bytes = sum(bytes_list) / len(bytes_list)
    return {
        "dataset":              dataset,
        "runs":                 runs,
        "avg_rows":             avg_rows,
        "avg_bytes":            avg_bytes,
        "avg_latency_ms":       round(avg_lat, 2),
        "throughput_rows_s":    round(avg_rows  / (avg_lat / 1000), 1) if avg_lat else 0,
        "throughput_mb_s":      round(avg_bytes / (avg_lat / 1000) / 1e6, 3) if avg_lat else 0,
    }
