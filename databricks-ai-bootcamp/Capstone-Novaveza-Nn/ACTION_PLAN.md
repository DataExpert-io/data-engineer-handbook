# Capstone Action Plan

## How this works

Same division of labor as tonight's MCP assignment: I write all the code (notebook cells, scripts, app files, agent config) — you don't need to write or understand code yourself. Your job is running notebook cells and clicking through the Databricks UI steps, most of which you've now done at least once already tonight.

## Quick plain-language primer (just enough to follow along, not a lecture)

**Spark** — the way Databricks notebooks process data. You'll run cells I write; each cell reads data in, transforms it, and saves it to a table. Think of it as a supercharged spreadsheet processed in code instead of by hand.

**Bronze / Silver / Gold** — three stages of cleaning data, standard Databricks pattern. Bronze = raw, exactly as pulled from the API. Silver = cleaned and deduplicated. Gold = summarized into whatever the app and agent actually read (here: provider counts by Chicago community area).

**Embeddings** — turning a sentence into a list of numbers that captures its *meaning*, not just its exact words. Two sentences about similar topics end up with number-lists that are mathematically close together. That's what lets someone search "addiction counseling access" and match text that never uses those exact words. You already have a working example of this — Assignment 2's weather search worked exactly this way.

**MCP server / Agent** — same pattern as tonight's weather assignment. A small server exposes "tools" (functions); an agent decides when to call them. Here: one tool searches providers, one tool writes a real action (the "confirm this info is current" flag) — that write is the "confirmation agent" piece.

## Architecture note (one real design decision)

The Spark pipeline will land Bronze/Silver/Gold as proper Delta tables (the native Databricks/Spark pattern — satisfies the "Spark pipeline" requirement cleanly). Since Gold is small (provider counts per community area, likely well under 100 rows), I'll also write that summary into a Lakebase Postgres table so the Flask App and agent can read it the same way your ticket tracker and weather apps already do — no new connection pattern to learn.

## Phase 1 — Data pipeline (Spark + NPPES API)

1. I write a notebook (or set of notebooks) that: queries the NPPES registry API for Illinois using your existing behavioral-health taxonomy code list → lands raw JSON as Bronze → cleans/dedupes as Silver → aggregates provider counts by Chicago community area as Gold (Delta table + mirrored into a small Lakebase table).
2. You run the notebook cells in order — I'll tell you exactly which cells, in what order.
3. Verify: query the Gold table, confirm provider counts show up per community area.

## Phase 2 — Embeddings (unstructured data)

1. I write an ingest script (same shape as Assignment 2's) that embeds short text — provider taxonomy/specialty descriptions, zero extra dependencies — into a pgvector table in Lakebase.
2. You run it (same as Assignment 2: `python3 ingest_embeddings.py`).
3. Verify: a semantic search query returns ranked results.

## Phase 3 — Databricks App frontend

1. I write `app.py` (Flask + Lakebase): a page showing provider counts by community area, and a search route hitting the embeddings.
2. You: GitHub upload → Databricks Git folder pull → Create app (Configure Git) → Deploy. Same click-path as tonight, twice proven.
3. Verify via `/logz` and the live app URL.

## Phase 4 — Confirmation Agent (MCP + Agent Bricks)

1. I write the MCP server: a search tool + a "confirm/flag provider info" write tool (writes to a small Lakebase table).
2. You: same deploy drill as tonight (GitHub push, pull, Create app, skip Resources since no API key needed, Deploy).
3. Register in AI Gateway → MCPs — same lesson from tonight: OAuth token from `databricks auth token`, not a plain PAT; append `/mcp` to the URL.
4. Build the Agent Bricks Supervisor Agent, add the MCP tool, paste the system prompt (I'll write it), test with 3 questions — including one that triggers the write/confirm tool.

## Phase 5 — Wrap up

1. README: architecture, data sources, how each of the 5 requirements is satisfied, "future work" section (already drafted in SCOPE.md).
2. Screenshots: Gold table output, embedding search results, App running, agent test transcripts (both the search and the write action).
3. Zip and submit, same process as tonight.

## What I need from you before I start writing Phase 1

- Which Lakebase database to use — same `new-database` / `student_two` connection from tonight, or a fresh one?
- Green light to start writing the Phase 1 notebook now.
