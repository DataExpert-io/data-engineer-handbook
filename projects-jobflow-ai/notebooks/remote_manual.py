# Databricks notebook source
# MAGIC %md
# MAGIC # JobFlow AI — Carga Manual RemoteOK
# MAGIC
# MAGIC Este notebook lê o arquivo JSON da RemoteOK salvo manualmente no Volume,
# MAGIC grava os dados na tabela Bronze e registra a execução em ingestion_runs.

# COMMAND ----------

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import types as T

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "jobflow_ai"

SOURCE = "remoteok_manual"

RAW_FILE_PATH = (
    f"/Volumes/{CATALOG}/{SCHEMA}/raw/"
    f"source=remoteok_manual/remoteokapi.json"
)

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_remoteok_jobs"
INGESTION_RUNS_TABLE = f"{CATALOG}.{SCHEMA}.ingestion_runs"

MAX_JOBS_TO_BRONZE = 500

print("=" * 70)
print("JOBFLOW AI — CARGA MANUAL REMOTEOK")
print("=" * 70)
print(f"Arquivo raw: {RAW_FILE_PATH}")
print(f"Tabela Bronze: {BRONZE_TABLE}")
print(f"Tabela ingestão: {INGESTION_RUNS_TABLE}")
print("=" * 70)

# COMMAND ----------

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

display(spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{SCHEMA}`"))

# COMMAND ----------

def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def safe_int(value: Any) -> int | None:
    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def split_remoteok_response(
    data: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not data:
        return {}, []

    first_item = data[0]

    if isinstance(first_item, dict) and "legal" in first_item:
        metadata = first_item
        jobs = data[1:]
    else:
        metadata = {}
        jobs = data

    clean_jobs = [
        job
        for job in jobs
        if isinstance(job, dict) and job.get("id") is not None
    ]

    return metadata, clean_jobs


def normalize_job_row(
    *,
    run_id: str,
    job: dict[str, Any],
    file_path: str,
) -> dict[str, Any]:
    tags = job.get("tags")

    if tags is None:
        tags_as_json = None
    else:
        tags_as_json = compact_json(tags)

    return {
        "run_id": run_id,
        "source": "remoteok",
        "source_job_id": str(job.get("id")) if job.get("id") is not None else None,
        "slug": job.get("slug"),
        "company": job.get("company"),
        "position": job.get("position"),
        "location": job.get("location"),
        "date": job.get("date"),
        "tags": tags_as_json,
        "salary_min": safe_int(job.get("salary_min")),
        "salary_max": safe_int(job.get("salary_max")),
        "apply_url": job.get("apply_url"),
        "job_url": job.get("url"),
        "payload": compact_json(job),
        "ingested_at": utc_now_naive(),
        "file_path": file_path,
    }

# COMMAND ----------

run_id = str(uuid.uuid4())
started_at = utc_now_naive()

status = "started"
error_message = None
records_raw = 0
records_bronze = 0
bronze_rows = []

print(f"run_id: {run_id}")

try:
    with open(RAW_FILE_PATH, "r", encoding="utf-8") as file:
        remoteok_data = json.load(file)

    if not isinstance(remoteok_data, list):
        raise TypeError(
            f"O arquivo deveria conter uma lista JSON. "
            f"Tipo recebido: {type(remoteok_data).__name__}"
        )

    metadata, jobs = split_remoteok_response(remoteok_data)

    print(f"Itens no arquivo: {len(remoteok_data)}")
    print(f"Vagas identificadas: {len(jobs)}")

    if metadata:
        print("Metadata encontrada no primeiro item.")
        print(f"Campos de metadata: {list(metadata.keys())}")

    jobs_to_write = jobs[:MAX_JOBS_TO_BRONZE]

    bronze_rows = [
        normalize_job_row(
            run_id=run_id,
            job=job,
            file_path=RAW_FILE_PATH,
        )
        for job in jobs_to_write
    ]

    records_raw = 1
    records_bronze = len(bronze_rows)

    status = "success"
    finished_at = utc_now_naive()

except Exception as exc:
    status = "failed"
    finished_at = utc_now_naive()
    error_message = f"{type(exc).__name__}: {exc}"

    print("ERRO NA CARGA MANUAL REMOTEOK")
    print(error_message)

# COMMAND ----------

bronze_schema = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("source", T.StringType(), False),
        T.StructField("source_job_id", T.StringType(), True),
        T.StructField("slug", T.StringType(), True),
        T.StructField("company", T.StringType(), True),
        T.StructField("position", T.StringType(), True),
        T.StructField("location", T.StringType(), True),
        T.StructField("date", T.StringType(), True),
        T.StructField("tags", T.StringType(), True),
        T.StructField("salary_min", T.LongType(), True),
        T.StructField("salary_max", T.LongType(), True),
        T.StructField("apply_url", T.StringType(), True),
        T.StructField("job_url", T.StringType(), True),
        T.StructField("payload", T.StringType(), False),
        T.StructField("ingested_at", T.TimestampType(), False),
        T.StructField("file_path", T.StringType(), True),
    ]
)

if bronze_rows:
    bronze_df = spark.createDataFrame(
        bronze_rows,
        schema=bronze_schema,
    )

    bronze_df.write.mode("append").saveAsTable(BRONZE_TABLE)

    print(f"OK: {records_bronze} registros gravados em {BRONZE_TABLE}")
else:
    print("Nenhum registro Bronze para gravar.")

# COMMAND ----------

ingestion_schema = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("source", T.StringType(), False),
        T.StructField("query", T.StringType(), True),
        T.StructField("location", T.StringType(), True),
        T.StructField("started_at", T.TimestampType(), False),
        T.StructField("finished_at", T.TimestampType(), True),
        T.StructField("status", T.StringType(), False),
        T.StructField("records_raw", T.LongType(), True),
        T.StructField("records_bronze", T.LongType(), True),
        T.StructField("error_message", T.StringType(), True),
        T.StructField("created_at", T.TimestampType(), False),
    ]
)

ingestion_rows = [
    {
        "run_id": run_id,
        "source": SOURCE,
        "query": "manual remoteok json file",
        "location": "remote",
        "started_at": started_at,
        "finished_at": finished_at,
        "status": status,
        "records_raw": records_raw,
        "records_bronze": records_bronze,
        "error_message": error_message,
        "created_at": utc_now_naive(),
    }
]

ingestion_df = spark.createDataFrame(
    ingestion_rows,
    schema=ingestion_schema,
)

ingestion_df.write.mode("append").saveAsTable(INGESTION_RUNS_TABLE)

display(
    spark.sql(
        f"""
        SELECT *
        FROM {INGESTION_RUNS_TABLE}
        WHERE run_id = '{run_id}'
        """
    )
)

# COMMAND ----------

validation_df = spark.sql(
    f"""
    SELECT
        source,
        COUNT(*) AS records,
        COUNT(DISTINCT source_job_id) AS distinct_jobs,
        COUNT(DISTINCT company) AS distinct_companies,
        MIN(ingested_at) AS first_ingested_at,
        MAX(ingested_at) AS last_ingested_at
    FROM {BRONZE_TABLE}
    WHERE run_id = '{run_id}'
    GROUP BY source
    """
)

display(validation_df)

sample_df = spark.sql(
    f"""
    SELECT
        run_id,
        source,
        source_job_id,
        company,
        position,
        location,
        salary_min,
        salary_max,
        substring(payload, 1, 500) AS payload_preview,
        file_path
    FROM {BRONZE_TABLE}
    WHERE run_id = '{run_id}'
    LIMIT 10
    """
)

display(sample_df)

# COMMAND ----------

print()
print("=" * 70)
print("RESULTADO: CARGA MANUAL REMOTEOK CONCLUÍDA")
print("=" * 70)
print(f"run_id: {run_id}")
print(f"status: {status}")
print(f"raw files: {records_raw}")
print(f"bronze records: {records_bronze}")
print(f"raw file: {RAW_FILE_PATH}")
print(f"bronze table: {BRONZE_TABLE}")
print(f"ingestion table: {INGESTION_RUNS_TABLE}")
print("=" * 70)

if status == "failed":
    raise RuntimeError(error_message)