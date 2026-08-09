"""
Weather MCP server (FastMCP, streamable-HTTP transport).

Exposes weather tools backed by Open-Meteo (free, no API key). Tool
functions here stay thin (docstring + a couple of calls); all HTTP
requests and response parsing live in weather_broker.py.

Run locally:
    python mcp_server/weather_mcp_server.py
"""

import logging
import os

from mcp.server.fastmcp import FastMCP

import weather_broker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("weather_mcp_server")

# In this SDK version, FastMCP.run() does not accept host/port kwargs --
# they must be set on the FastMCP instance itself at construction time.
_MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
_MCP_PORT = int(os.getenv("MCP_PORT", 8000))

mcp = FastMCP("weather-server", host=_MCP_HOST, port=_MCP_PORT)

# Precipitation-probability threshold used by predict_umbrella_needed.
# Documented here (not buried in the tool body) so it's easy to find/tune,
# and referenced in the tool's own docstring/reasoning output.
UMBRELLA_PRECIP_THRESHOLD_PCT = 40


@mcp.tool()
def get_current_weather(location: str) -> dict:
    """Get current conditions for a location.

    Args:
        location: City name, "City, ST"/"City, Country", or similar
            free-text place name (e.g. "Chicago", "Austin, TX").

    Returns:
        dict with: location (resolved display name), temperature_f,
        humidity_pct, wind_mph, conditions (human-readable description),
        observed_at (ISO timestamp). On failure (bad location, API
        outage), returns {"error": "<clean message>"} instead of raising,
        so the agent can react sensibly rather than crash.
    """
    try:
        place = weather_broker.geocode_location(location)
        conditions = weather_broker.fetch_current_conditions(
            place["latitude"], place["longitude"]
        )
        return {"location": place["resolved_name"], **conditions}
    except weather_broker.WeatherLookupError as exc:
        logger.warning("get_current_weather failed for %r: %s", location, exc)
        return {"error": str(exc)}


@mcp.tool()
def get_forecast(location: str, days: int = 5) -> dict:
    """Get a multi-day forecast for a location.

    Args:
        location: City name or "City, ST"/"City, Country".
        days: Number of days to forecast, 1-16 (default 5). Values
            outside this range are clamped rather than rejected.

    Returns:
        dict with: location (resolved display name), forecast (list of
        per-day dicts: date, high_f, low_f, precipitation_probability_pct,
        conditions). On failure, returns {"error": "<clean message>"}.
    """
    try:
        place = weather_broker.geocode_location(location)
        forecast = weather_broker.fetch_daily_forecast(
            place["latitude"], place["longitude"], days
        )
        return {"location": place["resolved_name"], "forecast": forecast}
    except weather_broker.WeatherLookupError as exc:
        logger.warning("get_forecast failed for %r: %s", location, exc)
        return {"error": str(exc)}


@mcp.tool()
def predict_umbrella_needed(location: str, date: str) -> dict:
    """Recommend whether to bring an umbrella on a given date, based on
    forecast precipitation probability.

    This is a derived judgment call, not a raw passthrough: it fetches
    the forecast, finds the requested date, and applies a fixed threshold
    (precipitation probability > 40%) to produce a yes/no recommendation
    with reasoning -- satisfying the assignment's requirement that this
    tool "show reasoning, not just a passthrough of the raw API response."

    Args:
        location: City name or "City, ST"/"City, Country".
        date: Date to check, in YYYY-MM-DD format. Must fall within the
            next 16 days (Open-Meteo's forecast horizon) -- the agent's
            system prompt is responsible for resolving relative dates
            like "tomorrow" or "this weekend" into an actual date before
            calling this tool.

    Returns:
        dict with: location, date, umbrella_recommended (bool),
        precipitation_probability_pct, conditions, reasoning (str
        explaining the threshold applied). On failure (bad location, date
        outside the forecast range, API error), returns
        {"error": "<clean message>"}.
    """
    try:
        place = weather_broker.geocode_location(location)
        # Fetch the full 16-day horizon so any valid requested date is
        # covered, then pick out the specific day asked about.
        forecast = weather_broker.fetch_daily_forecast(
            place["latitude"], place["longitude"], days=16
        )
    except weather_broker.WeatherLookupError as exc:
        logger.warning("predict_umbrella_needed failed for %r: %s", location, exc)
        return {"error": str(exc)}

    day = next((d for d in forecast if d["date"] == date), None)
    if day is None:
        return {
            "error": (
                f"No forecast available for {date} in {place['resolved_name']}. "
                f"Forecasts are only available for today through the next 16 days."
            )
        }

    precip_pct = day["precipitation_probability_pct"] or 0
    recommended = precip_pct > UMBRELLA_PRECIP_THRESHOLD_PCT

    return {
        "location": place["resolved_name"],
        "date": date,
        "umbrella_recommended": recommended,
        "precipitation_probability_pct": precip_pct,
        "conditions": day["conditions"],
        "reasoning": (
            f"Precipitation probability is {precip_pct}%, which is "
            f"{'above' if recommended else 'at or below'} the "
            f"{UMBRELLA_PRECIP_THRESHOLD_PCT}% threshold used for this "
            f"recommendation, so an umbrella is "
            f"{'recommended' if recommended else 'not recommended'}."
        ),
    }


if __name__ == "__main__":
    # host/port are already set on the FastMCP instance above (this SDK
    # version's run() does not accept them directly).
    mcp.run(transport="streamable-http")
