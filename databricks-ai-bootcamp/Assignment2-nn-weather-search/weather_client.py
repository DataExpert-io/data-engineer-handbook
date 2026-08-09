"""
National Weather Service (api.weather.gov) client.

Mirrors the shape of massive_client.py from the day-2 reference app:
resolve locations, pull raw records from the source API, normalize each
into a flat document dict ready to upsert into weather_documents.

No API key required. NWS asks that every client send an identifying
User-Agent header (contact info), which we do below.

Two source types are harvested per location:
  - "alert"    -> GET /alerts/active?area={state}         (active watches/warnings)
  - "forecast" -> GET /gridpoints/{office}/{x},{y}/forecast (multi-day narrative forecast)
"""

import hashlib
import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger("weather_client")

NWS_BASE = "https://api.weather.gov"
HEADERS = {
    "User-Agent": "novanega-capstone-weather-search (rachel@rachel-martz.com)",
    "Accept": "application/geo+json",
}
REQUEST_TIMEOUT = 15

# NWS doesn't do city/state -> lat/lon geocoding, and adding a full geocoder
# is out of scope for this assignment. We resolve a small set of common US
# cities locally; callers can also pass "lat,lon" directly for any location.
# See README_WEATHER.md "Known limitations" for the tradeoff.
_CITY_COORDS = {
    "chicago, il": (41.8781, -87.6298),
    "austin, tx": (30.2672, -97.7431),
    "new york, ny": (40.7128, -74.0060),
    "miami, fl": (25.7617, -80.1918),
    "denver, co": (39.7392, -104.9903),
    "seattle, wa": (47.6062, -122.3321),
    "los angeles, ca": (34.0522, -118.2437),
    "houston, tx": (29.7604, -95.3698),
    "phoenix, az": (33.4484, -112.0740),
    "boston, ma": (42.3601, -71.0589),
    "atlanta, ga": (33.7490, -84.3880),
    "minneapolis, mn": (44.9778, -93.2650),
    "new orleans, la": (29.9511, -90.0715),
    "oklahoma city, ok": (35.4676, -97.5164),
    "kansas city, mo": (39.0997, -94.5786),
}

_STATE_ABBR = {
    "chicago, il": "IL", "austin, tx": "TX", "new york, ny": "NY",
    "miami, fl": "FL", "denver, co": "CO", "seattle, wa": "WA",
    "los angeles, ca": "CA", "houston, tx": "TX", "phoenix, az": "AZ",
    "boston, ma": "MA", "atlanta, ga": "GA", "minneapolis, mn": "MN",
    "new orleans, la": "LA", "oklahoma city, ok": "OK", "kansas city, mo": "MO",
}


def _resolve_location(location: str):
    """Resolve a location string to (lat, lon, state_abbr_or_None)."""
    key = location.strip().lower()
    if key in _CITY_COORDS:
        lat, lon = _CITY_COORDS[key]
        return lat, lon, _STATE_ABBR.get(key)

    if "," in location and any(c.isdigit() for c in location):
        # Treat as "lat,lon"
        parts = [p.strip() for p in location.split(",")]
        if len(parts) == 2:
            try:
                lat, lon = float(parts[0]), float(parts[1])
                return lat, lon, None
            except ValueError:
                pass

    raise ValueError(
        f"Could not resolve location '{location}'. Use a supported 'City, ST' "
        f"name (see weather_client._CITY_COORDS) or a 'lat,lon' pair."
    )


def _get(url: str) -> dict:
    resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _grid_point(lat: float, lon: float) -> dict:
    """GET /points/{lat},{lon} -> gridId/gridX/gridY/forecast URLs."""
    data = _get(f"{NWS_BASE}/points/{lat},{lon}")
    props = data.get("properties", {})
    return {
        "grid_id": props.get("gridId"),
        "grid_x": props.get("gridX"),
        "grid_y": props.get("gridY"),
        "forecast_url": props.get("forecast"),
        "state": (props.get("relativeLocation", {}).get("properties", {}) or {}).get("state"),
    }


def _stable_id(*parts) -> str:
    raw = "|".join(str(p) for p in parts if p is not None)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_alerts(location: str, state: str) -> list:
    """GET /alerts/active?area={state} -> normalized alert documents."""
    if not state:
        logger.warning("No state resolved for %s; skipping alerts", location)
        return []
    data = _get(f"{NWS_BASE}/alerts/active?area={state}")
    docs = []
    for feature in data.get("features", []):
        props = feature.get("properties", {}) or {}
        description = (props.get("description") or "").strip()
        instruction = (props.get("instruction") or "").strip()
        narrative = "\n\n".join(p for p in [description, instruction] if p)
        if not narrative:
            continue
        docs.append({
            "id": props.get("id") or _stable_id(location, "alert", props.get("headline")),
            "location": location,
            "source_type": "alert",
            "headline": props.get("event") or props.get("headline") or "Weather Alert",
            "narrative_text": narrative,
            "issued_at": props.get("sent") or props.get("effective"),
            "payload": feature,
            "synced_at": _now_iso(),
        })
    return docs


def fetch_forecast(location: str, grid: dict, limit: int = 50) -> list:
    """GET /gridpoints/{office}/{x},{y}/forecast -> normalized forecast-period documents."""
    if not grid.get("forecast_url"):
        logger.warning("No forecast URL resolved for %s; skipping forecast", location)
        return []
    data = _get(grid["forecast_url"])
    periods = (data.get("properties", {}) or {}).get("periods", [])
    docs = []
    for period in periods[:limit]:
        narrative = (period.get("detailedForecast") or "").strip()
        if not narrative:
            continue
        doc_id = _stable_id(
            location, "forecast", grid.get("grid_id"), grid.get("grid_x"),
            grid.get("grid_y"), period.get("number"), period.get("startTime"),
        )
        docs.append({
            "id": doc_id,
            "location": location,
            "source_type": "forecast",
            "headline": period.get("name") or "Forecast Period",
            "narrative_text": narrative,
            "issued_at": period.get("startTime"),
            "payload": period,
            "synced_at": _now_iso(),
        })
    return docs


def sync_locations(locations: list, limit: int = 50) -> list:
    """Harvest alerts + forecast for each location, return a flat list of
    normalized document dicts ready to upsert into weather_documents."""
    all_docs = []
    for location in locations:
        try:
            lat, lon, state = _resolve_location(location)
            grid = _grid_point(lat, lon)
            state = state or grid.get("state")

            all_docs.extend(fetch_alerts(location, state))
            all_docs.extend(fetch_forecast(location, grid, limit=limit))
        except Exception:
            logger.exception("Failed to sync location '%s'; skipping", location)
    return all_docs
