"""Arrow IPC encoder + optional NATS publisher for the Python collector."""
import io

import pyarrow as pa
import pyarrow.ipc as ipc

from .fetcher import Measurement

# Schema must match collector/internal/schema/air_quality.go exactly.
AIR_QUALITY_SCHEMA = pa.schema([
    pa.field("location_id",   pa.int64()),
    pa.field("location_name", pa.utf8()),
    pa.field("country_code",  pa.utf8()),
    pa.field("city",          pa.utf8()),
    pa.field("latitude",      pa.float64()),
    pa.field("longitude",     pa.float64()),
    pa.field("parameter",     pa.utf8()),
    pa.field("value",         pa.float64()),
    pa.field("unit",          pa.utf8()),
    pa.field("timestamp",     pa.timestamp("us", tz="UTC")),
    pa.field("collector_id",  pa.utf8()),
])


def encode_to_arrow(measurements: list[Measurement]) -> bytes:
    """Encode measurements to Arrow IPC stream bytes (same format as Go EncodeToArrow)."""
    if not measurements:
        return b""
    arrays = [
        pa.array([m.location_id   for m in measurements], type=pa.int64()),
        pa.array([m.location_name for m in measurements], type=pa.utf8()),
        pa.array([m.country_code  for m in measurements], type=pa.utf8()),
        pa.array([m.city          for m in measurements], type=pa.utf8()),
        pa.array([m.latitude      for m in measurements], type=pa.float64()),
        pa.array([m.longitude     for m in measurements], type=pa.float64()),
        pa.array([m.parameter     for m in measurements], type=pa.utf8()),
        pa.array([m.value         for m in measurements], type=pa.float64()),
        pa.array([m.unit          for m in measurements], type=pa.utf8()),
        pa.array([m.timestamp_us  for m in measurements], type=pa.timestamp("us", tz="UTC")),
        pa.array([m.collector_id  for m in measurements], type=pa.utf8()),
    ]
    batch = pa.record_batch(arrays, schema=AIR_QUALITY_SCHEMA)
    sink  = io.BytesIO()
    with ipc.new_stream(sink, AIR_QUALITY_SCHEMA) as writer:
        writer.write_batch(batch)
    return sink.getvalue()


async def publish(nc, country: str, measurements: list[Measurement]) -> int:
    """Publish Arrow IPC bytes to NATS subject air.quality.<country>.
    Returns the number of bytes published, or 0 if nc is None."""
    if nc is None or not measurements:
        return 0
    data = encode_to_arrow(measurements)
    await nc.publish(f"air.quality.{country}", data)
    return len(data)
