"""FastAPI service that subscribes to NATS air.quality.> and exposes a REST API."""
import asyncio
import io
import os
from collections import deque
from contextlib import asynccontextmanager
from typing import Any

import nats
import pyarrow as pa
from fastapi import FastAPI, Query

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "50000"))

store: deque[dict[str, Any]] = deque(maxlen=MAX_RECORDS)
store_lock = asyncio.Lock()
nc_handle: nats.aio.client.Client | None = None


async def handle_message(msg: nats.aio.msg.Msg) -> None:
    try:
        reader = pa.ipc.open_stream(io.BytesIO(msg.data))
        rows: list[dict[str, Any]] = []
        for batch in reader:
            df = batch.to_pandas()
            rows.extend(df.to_dict(orient="records"))
        async with store_lock:
            store.extend(rows)
    except Exception as exc:  # noqa: BLE001
        print(f"[handler] decode error: {exc}")


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ANN001
    global nc_handle
    nc_handle = await nats.connect(NATS_URL)
    await nc_handle.subscribe("air.quality.>", cb=handle_message)
    print(f"[startup] Connected to NATS {NATS_URL}")
    yield
    if nc_handle:
        await nc_handle.drain()


app = FastAPI(title="Air Quality API", lifespan=lifespan)


@app.get("/measurements")
async def get_measurements(
    country: str | None = Query(None, description="ISO-2 country code"),
    parameter: str | None = Query(None, description="Parameter, e.g. pm25"),
    limit: int = Query(200, le=5000),
) -> list[dict[str, Any]]:
    async with store_lock:
        data = list(store)
    if country:
        data = [r for r in data if r.get("country_code") == country]
    if parameter:
        data = [r for r in data if r.get("parameter") == parameter]
    return data[-limit:]


@app.get("/countries")
async def get_countries() -> list[str]:
    async with store_lock:
        return sorted({r.get("country_code", "") for r in store if r.get("country_code")})


@app.get("/stats")
async def get_stats() -> dict[str, Any]:
    async with store_lock:
        total = len(store)
        countries = sorted({r.get("country_code") for r in store if r.get("country_code")})
        parameters = sorted({r.get("parameter") for r in store if r.get("parameter")})
    return {"total_records": total, "countries": countries, "parameters": parameters}
