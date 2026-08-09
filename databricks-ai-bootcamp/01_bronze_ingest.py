# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingest
# MAGIC Pulls Illinois behavioral health providers from the NPPES Registry API
# MAGIC (free, no key) and lands the raw JSON as a Bronze Delta table.
# MAGIC
# MAGIC Run this notebook first, top to bottom. It creates the `ethicaworks.capg_bronze`
# MAGIC schema if it doesn't exist yet.

# COMMAND ----------

import json
import time
from datetime import datetime, timezone

import requests
from pyspark.sql import Row

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config
# MAGIC We query NPPES by profession category (broader search term) rather than
# MAGIC one call per exact taxonomy code, then filter precisely client-side in
# MAGIC 02_silver_transform against the exact code list. This keeps the number of
# MAGIC API calls small while guaranteeing precision regardless of how fuzzy the
# MAGIC API's own text matching is.

# COMMAND ----------

NPPES_BASE_URL = "https://npiregistry.cms.hhs.gov/api/"
NPPES_VERSION = "2.1"

# Broader search terms used to query NPPES (server-side, imprecise on purpose —
# we filter precisely afterward). Illinois individual providers only.
SEARCH_CATEGORIES = [
    "Counselor",
    "Psychologist",
    "Social Worker",
    "Marriage & Family Therapist",
    "Nurse Practitioner",
    "Psychiatry & Neurology",
]

# Exact taxonomy codes we actually want to keep (the "minimal high-accuracy"
# behavioral health clinician set from the project's taxonomy reference doc).
TARGET_TAXONOMY_CODES = {
    "101YA0400X",  # Counselor, Addiction (Substance Use Disorder)
    "101YM0800X",  # Counselor, Mental Health
    "101YP2500X",  # Counselor, Professional
    "103TC0700X",  # Psychologist, Clinical
    "103TC2200X",  # Psychologist, Clinical Child & Adolescent
    "103TA0400X",  # Psychologist, Addiction (Substance Use Disorder)
    "103TB0200X",  # Psychologist, Cognitive & Behavioral
    "103TC1900X",  # Psychologist, Counseling
    "1041C0700X",  # Social Worker, Clinical
    "106H00000X",  # Marriage & Family Therapist
    "363LP0808X",  # Nurse Practitioner, Psychiatric/Mental Health
    "2084P0800X",  # Psychiatry & Neurology, Psychiatry
    "2084P0804X",  # Psychiatry & Neurology, Child & Adolescent Psychiatry
}

PAGE_LIMIT = 200          # NPPES max per request
MAX_PAGES_PER_CATEGORY = 3  # up to 600 records per category before de-dup/filter
REQUEST_DELAY_SECONDS = 0.3  # be polite to the free API

# COMMAND ----------

def fetch_category(search_term: str) -> list:
    """Page through NPPES results for one search category, Illinois individual
    providers only. Returns the raw list of result dicts as NPPES returns them
    (no parsing/cleaning here — that happens in Silver)."""
    all_results = []
    for page in range(MAX_PAGES_PER_CATEGORY):
        params = {
            "version": NPPES_VERSION,
            "state": "IL",
            "enumeration_type": "NPI-1",  # individual providers only
            "taxonomy_description": search_term,
            "limit": PAGE_LIMIT,
            "skip": page * PAGE_LIMIT,
        }
        resp = requests.get(NPPES_BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if not results:
            break
        all_results.extend(results)
        if len(results) < PAGE_LIMIT:
            break  # last page
        time.sleep(REQUEST_DELAY_SECONDS)
    return all_results

# COMMAND ----------

raw_records = []
fetched_at = datetime.now(timezone.utc)

for category in SEARCH_CATEGORIES:
    print(f"Fetching category: {category}")
    category_results = fetch_category(category)
    print(f"  -> {len(category_results)} raw results")
    for record in category_results:
        raw_records.append({
            "npi": record.get("number"),
            "raw_json": json.dumps(record),
            "source_category": category,
            "fetched_at": fetched_at,
        })
    time.sleep(REQUEST_DELAY_SECONDS)

print(f"\nTotal raw records pulled (pre-dedup): {len(raw_records)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land as Bronze Delta table
# MAGIC Raw as pulled — no filtering, no dedup. Downstream notebooks handle cleaning.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS ethicaworks.capg_bronze")

rows = [Row(**r) for r in raw_records]
bronze_df = spark.createDataFrame(rows)

bronze_df.write.format("delta").mode("overwrite").saveAsTable(
    "ethicaworks.capg_bronze.nppes_providers_raw"
)

print("Bronze table written: ethicaworks.capg_bronze.nppes_providers_raw")
display(spark.table("ethicaworks.capg_bronze.nppes_providers_raw").limit(10))
