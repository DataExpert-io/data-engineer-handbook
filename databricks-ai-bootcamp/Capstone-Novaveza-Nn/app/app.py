"""
Novanega™ (Nv) — Chicago Behavioral Health Provider Access
Capstone Databricks App: Flask frontend reading from the Gold/embedding
tables populated by the Phase 1 Spark pipeline and Phase 2 embeddings.

Two things live here:
  - a dashboard showing provider counts by ZIP (from the Gold pipeline)
  - a semantic search box over provider descriptions (from the embeddings)

Same stack and deploy pattern as Assignment 1 (tickets) and Assignment 2
(weather): Flask + Lakebase Postgres, deployed as a Databricks App.
"""

import os

from flask import Flask, render_template_string, request

import lakebase

app = Flask(__name__)

EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384

_embedding_model = None


def get_embedding_model():
    global _embedding_model
    if _embedding_model is None:
        from sentence_transformers import SentenceTransformer
        _embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    return _embedding_model


def _vector_literal(embedding) -> str:
    """Format a Python list of floats as a pgvector literal string."""
    return "[" + ",".join(str(float(x)) for x in embedding) + "]"


PAGE_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Novanega™ — Chicago Behavioral Health Access</title>
  <style>
    body { font-family: -apple-system, sans-serif; max-width: 900px; margin: 40px auto; padding: 0 20px; color: #222; }
    h1 { font-size: 1.5rem; }
    h2 { font-size: 1.1rem; margin-top: 2rem; color: #444; }
    table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
    th, td { text-align: left; padding: 6px 10px; border-bottom: 1px solid #eee; }
    th { color: #666; font-weight: 600; font-size: 0.85rem; text-transform: uppercase; }
    .similarity { color: #888; font-size: 0.85rem; }
    form { margin-top: 1rem; }
    input[type=text] { width: 70%; padding: 8px; font-size: 1rem; }
    button { padding: 8px 16px; font-size: 1rem; }
    .note { color: #888; font-size: 0.85rem; margin-top: 2rem; }
  </style>
</head>
<body>
  <h1>Novanega™ — Chicago Behavioral Health Provider Access</h1>
  <p>Provider data sourced from the NPPES registry, filtered to behavioral health
  taxonomies in the Chicago area (ZIP prefix 606).</p>

  <h2>Provider counts by ZIP code ({{ zip_rows|length }} ZIPs, {{ total_providers }} providers)</h2>
  <table>
    <tr><th>ZIP</th><th>Provider count</th></tr>
    {% for row in zip_rows %}
    <tr><td>{{ row.zip5 }}</td><td>{{ row.provider_count }}</td></tr>
    {% endfor %}
  </table>

  <h2>Search providers</h2>
  <form method="get" action="/search">
    <input type="text" name="q" placeholder="e.g. child psychiatrist downtown" value="{{ query or '' }}">
    <button type="submit">Search</button>
  </form>

  {% if results is not none %}
    <h2>Results for "{{ query }}"</h2>
    {% if results %}
    <table>
      <tr><th>Provider</th><th>Specialty</th><th>Location</th><th>Similarity</th></tr>
      {% for r in results %}
      <tr>
        <td>{{ r.display_name }}{% if r.credential %}, {{ r.credential }}{% endif %}</td>
        <td>{{ r.taxonomy_desc }}</td>
        <td>{{ r.city }}, {{ r.state }} {{ r.zip5 }}</td>
        <td class="similarity">{{ "%.3f"|format(r.similarity) }}</td>
      </tr>
      {% endfor %}
    </table>
    {% else %}
    <p>No results.</p>
    {% endif %}
  {% endif %}

  <p class="note">Novaveza — the consumer-facing portal of Novanega™, under EthicaWorks.</p>
</body>
</html>
"""


@app.route("/")
def index():
    zip_rows = lakebase.run_query(
        "SELECT zip5, provider_count FROM provider_zip_counts ORDER BY provider_count DESC"
    )
    total = sum(row["provider_count"] for row in zip_rows)
    return render_template_string(
        PAGE_TEMPLATE, zip_rows=zip_rows, total_providers=total, query=None, results=None
    )


@app.route("/search")
def search():
    query = (request.args.get("q") or "").strip()
    zip_rows = lakebase.run_query(
        "SELECT zip5, provider_count FROM provider_zip_counts ORDER BY provider_count DESC"
    )
    total = sum(row["provider_count"] for row in zip_rows)

    if not query:
        return render_template_string(
            PAGE_TEMPLATE, zip_rows=zip_rows, total_providers=total, query=query, results=[]
        )

    model = get_embedding_model()
    query_embedding = model.encode(query).tolist()
    vector_literal = _vector_literal(query_embedding)

    results = lakebase.run_query(
        """
        SELECT p.npi, p.display_name, p.credential, p.taxonomy_desc,
               p.city, p.state, p.zip5,
               1 - (pe.embedding <=> %s::vector) AS similarity
        FROM provider_embeddings pe
        JOIN providers p ON p.npi = pe.npi
        ORDER BY pe.embedding <=> %s::vector
        LIMIT 10
        """,
        (vector_literal, vector_literal),
    )

    return render_template_string(
        PAGE_TEMPLATE, zip_rows=zip_rows, total_providers=total, query=query, results=results
    )


@app.route("/api/zip-counts")
def api_zip_counts():
    rows = lakebase.run_query(
        "SELECT zip5, provider_count FROM provider_zip_counts ORDER BY provider_count DESC"
    )
    return {"zip_counts": rows}


@app.route("/api/search")
def api_search():
    query = (request.args.get("q") or "").strip()
    if not query:
        return {"error": "query parameter 'q' is required"}, 400

    model = get_embedding_model()
    query_embedding = model.encode(query).tolist()
    vector_literal = _vector_literal(query_embedding)

    results = lakebase.run_query(
        """
        SELECT p.npi, p.display_name, p.credential, p.taxonomy_desc,
               p.city, p.state, p.zip5,
               1 - (pe.embedding <=> %s::vector) AS similarity
        FROM provider_embeddings pe
        JOIN providers p ON p.npi = pe.npi
        ORDER BY pe.embedding <=> %s::vector
        LIMIT 10
        """,
        (vector_literal, vector_literal),
    )
    return {"query": query, "results": results}


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
