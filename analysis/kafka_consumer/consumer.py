"""Kafka consumer with 5-minute sliding window aggregation.

Architecture
------------
A daemon thread runs the kafka-python synchronous consumer and pushes decoded
JSON messages onto a threading.Queue.  An asyncio background task drains that
queue every 100 ms and feeds the SlidingWindow.

This avoids bridging the async event loop with the blocking Kafka poll loop —
they communicate through the thread-safe queue.

Topics consumed
---------------
  air.measurements.raw  — one JSON dict per Measurement
  air.measurements.agg  — one JSON dict per tumbling-window AggRecord
"""
import asyncio
import json
import math
import queue as _queue
import threading
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from typing import Any, Optional

WINDOW_SECONDS: int = 5 * 60  # 5-minute sliding window

# ── Sliding Window ────────────────────────────────────────────────────────────

@dataclass
class WindowStats:
    country_code:    str
    parameter:       str
    count:           int
    mean_value:      float
    min_value:       float
    max_value:       float
    std_value:       float
    window_duration_s: int
    window_start_us: int   # µs since epoch
    window_end_us:   int   # µs since epoch


class SlidingWindow:
    """Event-time sliding window backed by per-(country, parameter) deques.

    Each deque entry is (event_timestamp_us, value).  Entries older than
    WINDOW_SECONDS from *current wall clock* are evicted on each compute() call.
    """

    def __init__(self, window_seconds: int = WINDOW_SECONDS) -> None:
        self._win_us  = window_seconds * 1_000_000
        self._win_sec = window_seconds
        # (country_code, parameter) → deque[(ts_us, value)]
        self._buckets: dict[tuple, deque] = defaultdict(deque)
        self._lock    = threading.Lock()

    def add(self, country: str, parameter: str, value: float, ts_us: int) -> None:
        key = (country, parameter)
        with self._lock:
            self._buckets[key].append((ts_us, value))

    def compute(self) -> list[WindowStats]:
        now_us  = int(time.time() * 1_000_000)
        cutoff  = now_us - self._win_us
        results: list[WindowStats] = []

        with self._lock:
            for (country, param), dq in list(self._buckets.items()):
                # Evict expired entries from the left (oldest first)
                while dq and dq[0][0] < cutoff:
                    dq.popleft()
                vals = [v for _, v in dq]
                if not vals:
                    continue
                n    = len(vals)
                mean = sum(vals) / n
                var  = sum((v - mean) ** 2 for v in vals) / n
                results.append(WindowStats(
                    country_code     = country,
                    parameter        = param,
                    count            = n,
                    mean_value       = round(mean, 4),
                    min_value        = round(min(vals), 4),
                    max_value        = round(max(vals), 4),
                    std_value        = round(math.sqrt(var), 4),
                    window_duration_s = self._win_sec,
                    window_start_us  = cutoff,
                    window_end_us    = now_us,
                ))

        return sorted(results, key=lambda s: (s.country_code, s.parameter))

    def entry_count(self) -> int:
        with self._lock:
            return sum(len(dq) for dq in self._buckets.values())

    def to_dict_list(self) -> list[dict[str, Any]]:
        return [asdict(s) for s in self.compute()]


# ── Global state ──────────────────────────────────────────────────────────────

sliding_window = SlidingWindow()

kafka_stats: dict[str, Any] = {
    "enabled":          False,
    "raw_total":        0,
    "agg_total":        0,
    "errors":           0,
    "last_raw_country": None,
    "last_raw_ts":      None,
}

_msg_queue:  _queue.Queue = _queue.Queue(maxsize=20_000)
_stop_event: threading.Event = threading.Event()
_consumer_thread: Optional[threading.Thread] = None


# ── Blocking consumer thread ──────────────────────────────────────────────────

def _consumer_thread_fn(brokers: list[str]) -> None:
    """Runs in a daemon thread: pulls from Kafka, puts onto _msg_queue."""
    try:
        from kafka import KafkaConsumer
        from kafka.errors import KafkaError
    except ImportError:
        print("[kafka_consumer] kafka-python not installed — consumer disabled.")
        return

    print(f"[kafka_consumer] connecting to brokers: {brokers}")
    while not _stop_event.is_set():
        consumer = None
        try:
            consumer = KafkaConsumer(
                "air.measurements.raw",
                "air.measurements.agg",
                bootstrap_servers=brokers,
                group_id="analysis-sliding-window",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8", errors="replace")),
                session_timeout_ms=30_000,
                heartbeat_interval_ms=10_000,
                max_poll_records=500,
            )
            print("[kafka_consumer] consumer connected")
            for msg in consumer:
                if _stop_event.is_set():
                    break
                try:
                    _msg_queue.put_nowait((msg.topic, msg.value))
                except _queue.Full:
                    pass  # drop when backpressured
        except Exception as exc:
            kafka_stats["errors"] += 1
            print(f"[kafka_consumer] error: {exc}")
            if consumer:
                try:
                    consumer.close()
                except Exception:
                    pass
            if not _stop_event.is_set():
                time.sleep(5)


# ── Async drain loop ──────────────────────────────────────────────────────────

def _process_message(topic: str, value: dict) -> None:
    """Decode one Kafka message and feed the sliding window."""
    try:
        if topic == "air.measurements.raw":
            country = value.get("country_code", "")
            param   = value.get("parameter",    "")
            val     = float(value.get("value",  0.0) or 0.0)
            ts_us   = int(value.get("timestamp_us", int(time.time() * 1_000_000)))
            if country and param:
                sliding_window.add(country, param, val, ts_us)
            kafka_stats["raw_total"]        += 1
            kafka_stats["last_raw_country"]  = country
            kafka_stats["last_raw_ts"]       = ts_us
        elif topic == "air.measurements.agg":
            kafka_stats["agg_total"] += 1
    except Exception as exc:
        kafka_stats["errors"] += 1
        print(f"[kafka_consumer] process error: {exc}")


async def _drain_loop() -> None:
    """Async task: drains _msg_queue every 100 ms and feeds the sliding window."""
    while not _stop_event.is_set():
        drained = 0
        try:
            while drained < 1000:
                topic, value = _msg_queue.get_nowait()
                _process_message(topic, value)
                drained += 1
        except _queue.Empty:
            pass
        await asyncio.sleep(0.1)


# ── Public start / stop ───────────────────────────────────────────────────────

async def start(brokers: list[str]) -> None:
    """Start the consumer thread and async drain loop."""
    global _consumer_thread
    kafka_stats["enabled"] = True
    _stop_event.clear()

    _consumer_thread = threading.Thread(
        target=_consumer_thread_fn,
        args=(brokers,),
        daemon=True,
        name="kafka-consumer",
    )
    _consumer_thread.start()
    asyncio.ensure_future(_drain_loop())
    print(f"[kafka_consumer] started (brokers={brokers}, window={WINDOW_SECONDS}s)")


def stop() -> None:
    """Signal the consumer thread and drain loop to stop."""
    _stop_event.set()
    kafka_stats["enabled"] = False
