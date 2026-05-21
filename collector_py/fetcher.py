"""Async OpenAQ v3 fetcher — mirrors collector/internal/fetcher/openaq.go.

Key difference from Go:
  Go   — sequential, 150 ms inter-location delay (rate-limit courtesy).
  Here — concurrent via asyncio.gather + semaphore (MAX_CONCURRENT requests
         in-flight at once).  This means Python wins on I/O latency while Go
         wins on per-request CPU overhead.
"""
import asyncio
import time
from dataclasses import dataclass
from datetime import datetime

import aiohttp

BASE_URL       = "https://api.openaq.org/v3"
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
MAX_CONCURRENT  = 5   # semaphore cap — respects OpenAQ rate limits


@dataclass
class Measurement:
    location_id:   int
    location_name: str
    country_code:  str
    city:          str
    latitude:      float
    longitude:     float
    parameter:     str
    value:         float
    unit:          str
    timestamp_us:  int    # µs since Unix epoch
    collector_id:  str


class AsyncFetcher:
    def __init__(self, api_key: str = "", collector_id: str = "python"):
        self._api_key      = api_key
        self._collector_id = collector_id
        self._sem          = asyncio.Semaphore(MAX_CONCURRENT)

    def _headers(self) -> dict:
        h = {"Accept": "application/json"}
        if self._api_key:
            h["X-API-Key"] = self._api_key
        return h

    async def fetch_measurements(
        self,
        session: aiohttp.ClientSession,
        country: str,
    ) -> list[Measurement]:
        """Fetch all latest measurements for a country (mirrors Go FetchMeasurements)."""
        locations = await self._fetch_locations(session, country)
        if not locations:
            return []

        tasks = [
            self._fetch_location_latest(session, loc, country)
            for loc in locations
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        measurements: list[Measurement] = []
        for r in results:
            if isinstance(r, list):
                measurements.extend(r)
        return measurements

    async def _fetch_locations(
        self,
        session: aiohttp.ClientSession,
        country: str,
    ) -> list[dict]:
        url = f"{BASE_URL}/locations"
        params = {"countries_id": country, "limit": 50, "page": 1}
        async with self._sem:
            try:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        return []
                    data = await resp.json(content_type=None)
                    return data.get("results", [])
            except Exception:
                return []

    async def _fetch_location_latest(
        self,
        session: aiohttp.ClientSession,
        loc: dict,
        country: str,
    ) -> list[Measurement]:
        loc_id = loc.get("id")
        if not loc_id:
            return []

        url = f"{BASE_URL}/locations/{loc_id}/latest"
        async with self._sem:
            try:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        return []
                    data = await resp.json(content_type=None)
            except Exception:
                return []

        coords = loc.get("coordinates") or {}
        lat    = float(coords.get("latitude")  or 0.0)
        lon    = float(coords.get("longitude") or 0.0)
        cobj   = loc.get("country") or {}
        cc     = cobj.get("code") or country

        measurements: list[Measurement] = []
        for r in data.get("results", []):
            param_obj = r.get("parameter") or {}
            param     = param_obj.get("name", "")   if isinstance(param_obj, dict) else str(param_obj)
            unit      = param_obj.get("units", "")  if isinstance(param_obj, dict) else ""
            dt_obj    = r.get("datetime") or {}
            dt_str    = dt_obj.get("utc", "") if isinstance(dt_obj, dict) else ""
            try:
                ts_us = int(
                    datetime.fromisoformat(dt_str.replace("Z", "+00:00")).timestamp()
                    * 1_000_000
                )
            except Exception:
                ts_us = int(time.time() * 1_000_000)

            measurements.append(Measurement(
                location_id   = int(loc_id),
                location_name = str(loc.get("name", "")),
                country_code  = cc,
                city          = str(loc.get("city", "") or loc.get("locality", "")),
                latitude      = lat,
                longitude     = lon,
                parameter     = param,
                value         = float(r.get("value") or 0.0),
                unit          = unit,
                timestamp_us  = ts_us,
                collector_id  = self._collector_id,
            ))
        return measurements


def make_session(api_key: str = "") -> aiohttp.ClientSession:
    headers = {"Accept": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key
    return aiohttp.ClientSession(headers=headers, timeout=REQUEST_TIMEOUT)
