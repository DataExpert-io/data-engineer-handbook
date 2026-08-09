"""
Novanega Confirmation Agent MCP server (FastMCP, streamable-HTTP transport).

Two tools:
  - search_providers: retrieve — semantic search over provider descriptions
  - confirm_provider_info: write — records a real confirmation/flag against
    a provider's data in Lakebase. This is the "confirmation agent" piece:
    the agent doesn't just answer questions, it can take a real action that
    embodies the actual Novanega product thesis (providers' data stays
    current because someone/something is checking it).

Same SDK-version lessons already solved in Assignment 3 tonight:
  - requirements.txt pins mcp<2.0.0 (2.0 renamed FastMCP -> MCPServer)
  - host/port are set on the FastMCP() constructor, not passed to run()

Run locally:
    python mcp_server/confirmation_mcp_server.py
"""

import logging
import os

from mcp.server.fastmcp import FastMCP

import provider_broker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("confirmation_mcp_server")

_MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
_MCP_PORT = int(os.getenv("MCP_PORT", 8000))

mcp = FastMCP("novanega-confirmation-agent", host=_MCP_HOST, port=_MCP_PORT)


@mcp.tool()
def search_providers(query: str, limit: int = 10) -> dict:
    """Search behavioral health providers by specialty, name, or location.

    Args:
        query: Free-text search, e.g. "child psychiatrist on the north side"
            or "addiction counseling downtown". Matches on meaning, not just
            exact keywords.
        limit: Max results to return, 1-20 (default 10).

    Returns:
        dict with: results (list of dicts: npi, display_name, credential,
        taxonomy_desc, city, state, zip5, similarity). On failure (blank
        query, DB error), returns {"error": "<clean message>"} instead of
        raising.
    """
    try:
        results = provider_broker.search_providers(query, limit)
        return {"results": results}
    except provider_broker.ProviderLookupError as exc:
        logger.warning("search_providers failed for %r: %s", query, exc)
        return {"error": str(exc)}


@mcp.tool()
def confirm_provider_info(npi: str, status: str, note: str = "") -> dict:
    """Record a confirmation or flag against a provider's current data. This
    is a real write, not a simulated one — it inserts a permanent row into
    the provider_confirmations table, tied to the product's core mechanic:
    provider data stays trustworthy because it's actively checked.

    Args:
        npi: The provider's 10-digit NPI number (from a prior search_providers
            call — never guess or fabricate an NPI).
        status: Either "confirmed_current" (the info looks accurate/up to
            date) or "flagged_outdated" (the info looks wrong or stale).
        note: Optional short explanation for the confirmation or flag (e.g.
            "phone number appears disconnected" or "verified via callback").

    Returns:
        dict with: npi, display_name, status, note, confirmation_id,
        confirmed_at. On failure (unknown NPI, invalid status, DB error),
        returns {"error": "<clean message>"} instead of raising.
    """
    try:
        return provider_broker.confirm_provider(npi, status, note or None)
    except provider_broker.ProviderLookupError as exc:
        logger.warning("confirm_provider_info failed for npi=%r: %s", npi, exc)
        return {"error": str(exc)}


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
