"""
Open-Meteo weather API adapter.

All HTTP calls and response parsing live here, kept out of the MCP tool
functions themselves (weather_mcp_server.py just calls into this module
and returns clean dicts). Mirrors the role of alpaca_broker.py in the
day-3 reference repo.

No API key required -- Open-Meteo's forecast and geocoding endpoints are
free and open, no signup, no credit card. That's also why it was picked
over WeatherAPI.com/NWS for this assignment: it lets the whole pipeline
get built and tested before any secrets management is needed at all.
"""

import logging
from typing import Optional

import requests

logger = logging.getLogger("weather_broker")

GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
REQUEST_TIMEOUT = 15

# WMO weather interpretation codes -> human-readable description.
# https://open-meteo.com/en/docs ("WMO Weather interpretation codes")
WMO_CODES = {
    0: "clear sky",
    1: "mainly clear",
    2: "partly cloudy",
    3: "overcast",
    45: "fog",
    48: "depositing rime fog",
    51: "light drizzle",
    53: "moderate drizzle",
    55: "dense drizzle",
    56: "light freezing drizzle",
    57: "dense freezing drizzle",
    61: "slight rain",
    63: "moderate rain",
    65: "heavy rain",
    66: "light freezing rain",
    67: "heavy freezing rain",
    71: "slight snow fall",
    73: "moderate snow fall",
    75: "heavy snow fall",
    77: "snow grains",
    80: "slight rain showers",
    81: "moderate rain showers",
    82: "violent rain showers",
    85: "slight snow showers",
    86: "heavy snow showers",
    95: "thunderstorm",
    96: "thunderstorm with slight hail",
    99: "thunderstorm with heavy hail",
}


class WeatherLookupError(Exception):
    """Raised for any user-facing weather lookup failure (bad location,
    API outage, etc.). Tool functions catch this and return a clean
    {"error": "..."} dict instead of letting a stack trace reach the
    agent -- matches the assignment's "clean error, not a stack trace"
    requirement."""


def describe_weather_code(code: Optional[int]) -> str:
    if code is None:
        return "unknown"
    return WMO_CODES.get(code, f"unknown (code {code})")


def geocode_location(location: str) -> dict:
    """Resolve a free-text location (city name, 'City, ST', etc.) to
    lat/lon + a canonical display name via Open-Meteo's geocoding API."""
    try:
        resp = requests.get(
            GEOCODE_URL,
            params={"name": location, "count": 1, "language": "en", "format": "json"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        raise WeatherLookupError(f"Could not reach the geocoding service: {exc}") from exc

    results = data.get("results") or []
    if not results:
        raise WeatherLookupError(
            f"Could not resolve location '{location}'. Try a more specific name, "
            f"e.g. 'Chicago, US' instead of just a neighborhood."
        )

    top = results[0]
    return {
        "latitude": top["latitude"],
        "longitude": top["longitude"],
        "resolved_name": ", ".join(
            part for part in [top.get("name"), top.get("admin1"), top.get("country")] if part
        ),
    }


def fetch_current_conditions(latitude: float, longitude: float) -> dict:
    """Current temperature, humidity, wind, and conditions for a lat/lon."""
    try:
        resp = requests.get(
            FORECAST_URL,
            params={
                "latitude": latitude,
                "longitude": longitude,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
                "temperature_unit": "fahrenheit",
                "wind_speed_unit": "mph",
                "timezone": "auto",
            },
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        raise WeatherLookupError(f"Could not reach the weather service: {exc}") from exc

    current = data.get("current") or {}
    return {
        "temperature_f": current.get("temperature_2m"),
        "humidity_pct": current.get("relative_humidity_2m"),
        "wind_mph": current.get("wind_speed_10m"),
        "conditions": describe_weather_code(current.get("weather_code")),
        "observed_at": current.get("time"),
    }


def fetch_daily_forecast(latitude: float, longitude: float, days: int) -> list:
    """Multi-day forecast: high/low temp, precip probability, conditions --
    one dict per day, for the next `days` days (clamped 1-16, Open-Meteo's
    max forecast horizon)."""
    days = max(1, min(days, 16))
    try:
        resp = requests.get(
            FORECAST_URL,
            params={
                "latitude": latitude,
                "longitude": longitude,
                "daily": (
                    "temperature_2m_max,temperature_2m_min,"
                    "precipitation_probability_max,weather_code"
                ),
                "temperature_unit": "fahrenheit",
                "timezone": "auto",
                "forecast_days": days,
            },
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        raise WeatherLookupError(f"Could not reach the weather service: {exc}") from exc

    daily = data.get("daily") or {}
    dates = daily.get("time", [])
    highs = daily.get("temperature_2m_max", [])
    lows = daily.get("temperature_2m_min", [])
    precip = daily.get("precipitation_probability_max", [])
    codes = daily.get("weather_code", [])

    forecast = []
    for i, date in enumerate(dates):
        forecast.append({
            "date": date,
            "high_f": highs[i] if i < len(highs) else None,
            "low_f": lows[i] if i < len(lows) else None,
            "precipitation_probability_pct": precip[i] if i < len(precip) else None,
            "conditions": describe_weather_code(codes[i] if i < len(codes) else None),
        })
    return forecast
