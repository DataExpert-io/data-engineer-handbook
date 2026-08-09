# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Transform
# MAGIC Parses the raw NPPES JSON from Bronze, keeps only providers that match our
# MAGIC exact behavioral-health taxonomy codes, filters to the Chicago area,
# MAGIC dedupes by NPI, and lands a clean Silver Delta table.
# MAGIC
# MAGIC Run after 01_bronze_ingest.

# COMMAND ----------

import json

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------

TARGET_TAXONOMY_CODES = {
    "101YA0400X", "101YM0800X", "101YP2500X",
    "103TC0700X", "103TC2200X", "103TA0400X", "103TB0200X", "103TC1900X",
    "1041C0700X",
    "106H00000X",
    "363LP0808X",
    "2084P0800X", "2084P0804X",
}

# COMMAND ----------

def parse_provider(raw_json: str):
    """Pull the fields we care about out of one raw NPPES record. Returns None
    if the record doesn't match one of our target taxonomy codes, so it can be
    filtered out with a simple notnull check after the UDF runs."""
    try:
        record = json.loads(raw_json)
    except (TypeError, ValueError):
        return None

    taxonomies = record.get("taxonomies") or []
    matched_code = None
    matched_desc = None
    for t in taxonomies:
        code = t.get("code")
        if code in TARGET_TAXONOMY_CODES:
            matched_code = code
            matched_desc = t.get("desc")
            if t.get("primary"):
                break  # prefer the primary taxonomy if it's one of our targets
    if matched_code is None:
        return None  # doesn't match any target taxonomy, drop it

    basic = record.get("basic") or {}
    display_name = basic.get("organization_name") or " ".join(
        part for part in [basic.get("first_name"), basic.get("last_name")] if part
    ).strip()
    credential = basic.get("credential")

    addresses = record.get("addresses") or []
    practice_address = next(
        (a for a in addresses if a.get("address_purpose") == "LOCATION"),
        addresses[0] if addresses else {},
    )

    zip_raw = practice_address.get("postal_code") or ""
    zip5 = zip_raw[:5] if zip_raw else None
    city = (practice_address.get("city") or "").upper()

    return json.dumps({
        "npi": record.get("number"),
        "display_name": display_name,
        "credential": credential,
        "taxonomy_code": matched_code,
        "taxonomy_desc": matched_desc,
        "address_line": practice_address.get("address_1"),
        "city": city,
        "state": practice_address.get("state"),
        "zip5": zip5,
        "phone": practice_address.get("telephone_number"),
    })

parse_provider_udf = F.udf(parse_provider, StringType())

# COMMAND ----------

bronze_df = spark.table("ethicaworks.capg_bronze.nppes_providers_raw")

parsed_df = bronze_df.withColumn("parsed_json", parse_provider_udf(F.col("raw_json")))
parsed_df = parsed_df.filter(F.col("parsed_json").isNotNull())

# Expand the parsed JSON into real columns
from pyspark.sql.types import StructType, StructField

parsed_schema = StructType([
    StructField("npi", StringType()),
    StructField("display_name", StringType()),
    StructField("credential", StringType()),
    StructField("taxonomy_code", StringType()),
    StructField("taxonomy_desc", StringType()),
    StructField("address_line", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("zip5", StringType()),
    StructField("phone", StringType()),
])

silver_df = parsed_df.withColumn(
    "fields", F.from_json(F.col("parsed_json"), parsed_schema)
).select("fields.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter to Chicago area
# MAGIC Chicago proper's ZIP codes start with "606" — used as a fast, defensible
# MAGIC proxy for "Chicago area" tonight. A true community-area join would need a
# MAGIC lat/long-to-polygon spatial join against Chicago's official community area
# MAGIC boundaries — flagged as future work, not attempted here.

# COMMAND ----------

chicago_df = silver_df.filter(
    (F.col("zip5").startswith("606")) | (F.col("city") == "CHICAGO")
)

# Drop rows with no usable ZIP (can't aggregate them in Gold)
chicago_df = chicago_df.filter(F.col("zip5").isNotNull())

# Dedup by NPI (a provider can show up in multiple search categories)
deduped_df = chicago_df.dropDuplicates(["npi"])

print(f"Silver row count after filtering + dedup: {deduped_df.count()}")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS ethicaworks.capg_silver")

deduped_df.write.format("delta").mode("overwrite").saveAsTable(
    "ethicaworks.capg_silver.providers_clean"
)

print("Silver table written: ethicaworks.capg_silver.providers_clean")
display(spark.table("ethicaworks.capg_silver.providers_clean").limit(20))
