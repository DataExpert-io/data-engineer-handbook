# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Output
# MAGIC Aggregates Silver into provider counts by ZIP (Gold Delta table), then
# MAGIC mirrors both the provider detail and the ZIP counts into Lakebase so the
# MAGIC Databricks App and the agent can read them the same way your ticket
# MAGIC tracker and weather app already do.
# MAGIC
# MAGIC Run after 02_silver_transform. Before running, make sure you've run
# MAGIC `lakebase/capstone_schema.sql` against your postgres-vector Lakebase
# MAGIC database once (same way you ran weather_schema.sql for Assignment 2).

# COMMAND ----------

# MAGIC %pip install psycopg2-binary

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

silver_df = spark.table("ethicaworks.capg_silver.providers_clean")

gold_df = (
    silver_df
    .groupBy("zip5")
    .agg(F.count("*").alias("provider_count"))
    .orderBy(F.desc("provider_count"))
)

print(f"Gold row count (distinct ZIPs): {gold_df.count()}")
display(gold_df)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS ethicaworks.capg_gold")

gold_df.write.format("delta").mode("overwrite").saveAsTable(
    "ethicaworks.capg_gold.provider_counts_by_zip"
)

print("Gold table written: ethicaworks.capg_gold.provider_counts_by_zip")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mirror to Lakebase
# MAGIC Both tables are small (Silver is at most a few thousand rows, Gold is one
# MAGIC row per ZIP) so we collect them to the driver and write via psycopg2 —
# MAGIC same pattern as the weather sync in Assignment 2, just simpler since
# MAGIC there's no upsert-on-rerun logic needed (we truncate + reinsert each run).

# COMMAND ----------

dbutils.widgets.text("lakebase_url", "", "Lakebase connection URL")
LAKEBASE_URL = dbutils.widgets.get("lakebase_url")

if not LAKEBASE_URL:
    raise ValueError(
        "Paste your postgres-vector Lakebase connection URL into the "
        "'lakebase_url' widget at the top of this notebook before running "
        "this cell — same connection string used in Assignment 2's .env."
    )

# COMMAND ----------

import psycopg2
from psycopg2.extras import execute_values

silver_rows = [tuple(r) for r in silver_df.select(
    "npi", "display_name", "credential", "taxonomy_code",
    "taxonomy_desc", "address_line", "city", "state", "zip5", "phone",
).collect()]

gold_rows = [tuple(r) for r in gold_df.select("zip5", "provider_count").collect()]

print(f"Writing {len(silver_rows)} providers and {len(gold_rows)} ZIP rows to Lakebase...")

conn = psycopg2.connect(LAKEBASE_URL)
try:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE providers")
        execute_values(
            cur,
            """
            INSERT INTO providers
                (npi, display_name, credential, taxonomy_code, taxonomy_desc,
                 address_line, city, state, zip5, phone)
            VALUES %s
            ON CONFLICT (npi) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                credential = EXCLUDED.credential,
                taxonomy_code = EXCLUDED.taxonomy_code,
                taxonomy_desc = EXCLUDED.taxonomy_desc,
                address_line = EXCLUDED.address_line,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                zip5 = EXCLUDED.zip5,
                phone = EXCLUDED.phone,
                synced_at = now()
            """,
            silver_rows,
        )

        cur.execute("TRUNCATE TABLE provider_zip_counts")
        execute_values(
            cur,
            """
            INSERT INTO provider_zip_counts (zip5, provider_count)
            VALUES %s
            ON CONFLICT (zip5) DO UPDATE SET
                provider_count = EXCLUDED.provider_count,
                synced_at = now()
            """,
            gold_rows,
        )
    conn.commit()
    print("Lakebase sync complete.")
finally:
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC Run a quick check that both Lakebase tables have data before moving on
# MAGIC to Phase 2 (embeddings).

# COMMAND ----------

conn = psycopg2.connect(LAKEBASE_URL)
try:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM providers")
        print(f"providers table row count: {cur.fetchone()[0]}")
        cur.execute("SELECT COUNT(*) FROM provider_zip_counts")
        print(f"provider_zip_counts table row count: {cur.fetchone()[0]}")
        cur.execute("SELECT zip5, provider_count FROM provider_zip_counts ORDER BY provider_count DESC LIMIT 5")
        print("Top 5 ZIPs by provider count:")
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]}")
finally:
    conn.close()
