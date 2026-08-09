# Capstone Scope — Novaveza (Nv) Mental Health Desert Pipeline + Confirmation Agent

Databricks AI Bootcamp Capstone. Deadline: end of day, August 9, 2026. Time available at start of this doc: ~11 hours. Status: nothing built yet.

## Official hard requirements (from course capstone doc)

Every project must include:

1. A data pipeline in Spark.
2. Integration with at least one third-party API.
3. Processing of unstructured data (embedding text for semantic retrieval).
4. A Databricks App with a frontend.
5. An AI agent with tools that can search/retrieve AND take real actions (writes) against the data.

## What's cut from the original vision

The original planning docs described a much larger build: four stitched datasets (NPPES, Chicago community boundaries, ACS 5-Year data, CMAP Community Data Snapshots, Chicago Health Atlas API), all 77 Chicago community areas, a full mental-health-desert heat map, and a polished three-act narrative presentation.

That scope is not achievable to a working state in the remaining time, starting from zero. Cut for tonight:

- ACS 5-Year data — not used.
- CMAP Community Data Snapshots — not used.
- Downloading the full 20GB NPPES CSV dump — not used.
- Coverage of all 77 community areas — scoped down to whatever the filtered API pull naturally returns (likely a meaningful subset, not all 77).
- Any UI polish, map visualization, or heat-map graphics — a simple data table/summary view is the target, not a designed product screen.

These get one line each in the final README under "Future work" — not attempted tonight.

## What's in scope

### 1. Spark pipeline + third-party API (one build satisfies both requirements)

- Source: NPPES Registry API (`npiregistry.cms.hhs.gov/api`) — free, no key required, returns JSON.
- Query filtered to Illinois + the ~13-code "minimal high-accuracy" behavioral health taxonomy list already defined in project docs (counselors, psychologists, social workers, MFTs, psychiatric NPs/physicians).
- Pull result set into a Spark DataFrame in a Databricks notebook.
- Bronze → Silver → Gold pass:
  - Bronze: raw API response landed as-is.
  - Silver: cleaned/deduped provider records (name, taxonomy, address, ZIP).
  - Gold: aggregated provider counts by Chicago community area (joined via ZIP or lat/long to a community area boundary reference).
- This is the full extent of the "data pipeline" and "third-party API" requirements — no additional datasets needed.

### 2. Unstructured data processing

- Embed short text per community area or per provider taxonomy — either Chicago Health Atlas indicator descriptions (if fast to pull) or provider specialty/taxonomy descriptions (fallback, zero external dependency).
- Reuse the exact sentence-transformers + pgvector pattern from Assignment 2 (`all-MiniLM-L6-v2`, Lakebase pgvector table, cosine similarity search) — already proven working end to end.
- Enables a semantic query like "which areas have the least addiction counseling access."

### 3. Databricks App frontend

- Reuse the Flask + Lakebase deploy pattern from Assignments 1 and 2 (known working click-path: GitHub → Git folder → Configure Git → Deploy).
- Minimal page: provider counts / desert indicator by community area, read from the Gold table. No map graphics, no design polish.

### 4. AI agent — search + real write action ("Confirmation Agent")

- Reuse the FastMCP + Agent Bricks pattern from tonight's MCP assignment (proven end to end, including the OAuth-token and Unity Catalog permissions steps already solved once).
- Retrieve tool: search providers / community areas by query.
- Write tool: a real action tied to the actual Novanega thesis — e.g., "flag provider info as outdated" or "confirm current," writing to a small Lakebase table. This is the "confirmation agent" piece referenced in prior planning — not a token write, it's the product's core trust mechanic in miniature.

## Rough time budget (~11 hrs)

| Piece | Estimate |
|---|---|
| Spark pipeline + NPPES API pull, bronze/silver/gold | 2–3 hrs |
| Unstructured embeddings | ~1 hr |
| Databricks App frontend | 1.5–2 hrs |
| Agent (MCP server + Agent Bricks registration) | ~2 hrs |
| Buffer, README, screenshots, submission | 2–3 hrs |

Tight but realistic if scope holds and no new categories of Databricks friction show up.

## Tech stack (all reused from tonight/this week — no new unknowns)

- Spark notebook (Databricks native)
- Lakebase (Postgres + pgvector)
- Flask + Lakebase Databricks App deploy pattern
- sentence-transformers embeddings
- FastMCP server + Agent Bricks external tool registration

## Out of scope / explicitly deferred

- Full 4-dataset integration
- All 77 community areas
- Heat map visualization / UI design

## Other deadlines

AI Engineering course capstone is separately due ~Aug 18 — not a conflict with tonight's plan, plenty of runway later.
