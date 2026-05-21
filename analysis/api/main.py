"""FastAPI service that subscribes to both raw (air.quality.*) and aggregated
(air.agg.*) NATS subjects and exposes REST endpoints for each."""
import asyncio
import io
import os
from collections import deque
from contextlib import asynccontextmanager
from typing import Any

import nats
import pyarrow as pa
from fastapi import FastAPI, Query

NATS_URL   = os.getenv("NATS_URL",    "nats://localhost:4222")
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "50000"))

# Separate stores for raw and aggregated data.
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
        rows = _decode_arrow(msg.data)
        async with raw_lock:
            raw_store.extend(rows)
    except Exception as exc:
        print(f"[raw handler] decode error: {exc}")


async def handle_agg(msg: nats.aio.msg.Msg) -> None:
    try:
        rows = _decode_arrow(msg.data)
        async with agg_lock:
            agg_store.extend(rows)
    except Exception as exc:
        print(f"[agg handler] decode error: {exc}")


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ANN001
    global nc_handle
    nc_handle = await nats.connect(NATS_URL)
    await nc_handle.subscribe("air.quality.*", cb=handle_raw)
    await nc_handle.subscribe("air.agg.*",     cb=handle_agg)
    print(f"[startup] Connected to NATS {NATS_URL}")
    yield
    if nc_handle:
        await nc_handle.drain()


app = FastAPI(title="Air Quality API", lifespan=lifespan)


# ── Raw endpoints ─────────────────────────────────────────────────────────────

@app.get("/measurements")
async def get_measurements(
    country:   str | None = Query(None),
    parameter: str | None = Query(None),
    limit:     int        = Query(200, le=5000),
) -> list[dict[str, Any]]:
    async with raw_lock:
        data = list(raw_store)
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
        raw_total = len(raw_store)
        countries  = sorted({r.get("country_code") for r in raw_store if r.get("country_code")})
        parameters = sorted({r.get("parameter")    for r in raw_store if r.get("parameter")})
    async with agg_lock:
        agg_total = len(agg_store)
    return {
        "raw_records":  raw_total,
        "agg_records":  agg_total,
        "countries":    countries,
        "parameters":   parameters,
    }


# ── Aggregated endpoints ──────────────────────────────────────────────────────

@app.get("/aggregated")
async def get_aggregated(
    country:   str | None = Query(None),
    parameter: str | None = Query(None),
    limit:     int        = Query(200, le=5000),
) -> list[dict[str, Any]]:
    async with agg_lock:
        data = list(agg_store)
    if country:
        data = [r for r in data if r.get("country_code") == country]
    if parameter:
        data = [r for r in data if r.get("parameter") == parameter]
    return data[-limit:]


@app.get("/aggregated/stats")
async def get_agg_stats() -> dict[str, Any]:
    """Returns per-parameter aggregation statistics across all stored windows."""
    async with agg_lock:
        data = list(agg_store)
    if not data:
        return {}
    # Group by parameter and compute simple summary.
    by_param: dict[str, dict[str, Any]] = {}
    for r in data:
        p = r.get("parameter", "unknown")
        if p not in by_param:
            by_param[p] = {"windows": 0, "raw_total": 0, "mean_values": []}
        by_param[p]["windows"]    += 1
        by_param[p]["raw_total"]  += r.get("count", 0)
        mv = r.get("mean_value")
        if mv is not None:
            by_param[p]["mean_values"].append(mv)
    for p, v in by_param.items():
        mv = v.pop("mean_values")
        v["overall_mean"] = sum(mv) / len(mv) if mv else None
    return by_param
