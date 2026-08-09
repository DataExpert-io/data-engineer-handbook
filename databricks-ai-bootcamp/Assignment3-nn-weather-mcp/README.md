# Weather-Prediction MCP Server

An MCP (Model Context Protocol) server exposing three weather tools, built for a Databricks Agent Bricks agent to call. Modeled on the Alpaca Markets paper-trading MCP server pattern from the course's day-3 reference repo (`databricks-lakebase-app-day-3`).

## Architecture

```
Agent Bricks agent
      |
      | MCP (streamable-HTTP)
      v
weather_mcp_server.py   <- thin @mcp.tool() functions only
      |
      | plain function calls
      v
weather_broker.py       <- all requests/HTTP calls + response parsing
      |
      | HTTPS
      v
Open-Meteo API (geocoding + forecast)
```

Tool functions in `weather_mcp_server.py` contain no raw `requests` calls — every HTTP request and response-parsing detail lives in `weather_broker.py`. Tools only orchestrate calls into the broker and shape the return dict.

## Weather API / auth choice

**Open-Meteo** (`open-meteo.com`) — free, no API key or signup required. Chosen specifically so the whole pipeline could be built and tested end-to-end without any secrets management (no Databricks secret scope needed for the weather API itself). Two endpoints are used:

- Geocoding API (`geocoding-api.open-meteo.com/v1/search`) — resolves a free-text location string to latitude/longitude.
- Forecast API (`api.open-meteo.com/v1/forecast`) — current conditions and up to a 16-day daily forecast.

Weather condition codes are WMO codes, translated to human-readable strings via a lookup table in `weather_broker.py`.

## Tools

### `get_current_weather(location: str) -> dict`
Current temperature (°F), humidity (%), wind (mph), and conditions for a location.

### `get_forecast(location: str, days: int = 5) -> dict`
Daily forecast (high/low temp, precipitation probability, conditions) for 1-16 days out.

### `predict_umbrella_needed(location: str, date: str) -> dict`
Derived recommendation, not a raw passthrough. Fetches the forecast, finds the requested date (`YYYY-MM-DD`), and applies a fixed threshold (precipitation probability > 40%) to return `umbrella_recommended` plus a `reasoning` string explaining the call.

All three tools catch lookup failures (bad location, API outage, date outside the forecast horizon) and return `{"error": "<message>"}` instead of raising, so the agent gets a clean signal to work with rather than a stack trace.

## Files

- `mcp_server/weather_broker.py` — Open-Meteo adapter (geocoding, current conditions, daily forecast, WMO code lookup).
- `mcp_server/weather_mcp_server.py` — FastMCP server definition, the three tools, streamable-HTTP entry point.
- `mcp_server/requirements.txt` — `mcp`, `requests`.
- `mcp_server/app.yaml` — Databricks App run config.

## Setup — local

```bash
cd mcp_server
pip install -r requirements.txt
python weather_mcp_server.py
```

Runs on `http://0.0.0.0:8000` by default (override with `MCP_HOST` / `MCP_PORT` env vars).

## Setup — Databricks deployment (TODO, not yet done)

1. Push this folder to GitHub, pull it into a Databricks Git folder (same pattern as Assignments 1 and 2).
2. Create a new Databricks App pointed at `mcp_server/` as the source path.
3. Confirm the app comes up and the MCP endpoint responds (no external API key needed, so no secret wiring required for this app specifically).
4. In Agent Bricks, register the deployed app's URL as an external MCP tool source.
5. Add the system prompt (see `SYSTEM_PROMPT.md`) to the agent.
6. Test with 3+ natural-language questions covering all three tools; capture screenshots.

## System prompt

See `SYSTEM_PROMPT.md` for the drafted Agent Bricks system prompt.
