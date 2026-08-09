# Databricks notebook source
# MAGIC %md
# MAGIC # JobFlow AI — Ingestão RemoteOK
# MAGIC
# MAGIC Este notebook faz a primeira ingestão real de dados externos do projeto:
# MAGIC
# MAGIC - chama a API pública da RemoteOK
# MAGIC - salva o JSON bruto no Unity Catalog Volume
# MAGIC - cria uma tabela Bronze para vagas da RemoteOK
# MAGIC - grava os registros retornados pela API
# MAGIC - registra a execução na tabela de controle ingestion_runs

# COMMAND ----------

import json
import os
import socket
import uuid
from datetime import datetime, timezone
from typing import Any

import requests
from pyspark.sql import types as T
# COMMAND ----------

hosts = [
    "remoteok.com",
    "google.com",
    "databricks.com",
]

print("=" * 70)
print("TESTE DE DNS")
print("=" * 70)

for host in hosts:
    try:
        ip = socket.gethostbyname(host)
        print(f"OK: {host} -> {ip}")
    except Exception as exc:
        print(f"ERRO: {host} -> {type(exc).__name__}: {exc}")

print()
print("=" * 70)
print("TESTE HTTP")
print("=" * 70)

urls = [
    "https://remoteok.com/api",
    "https://www.databricks.com",
]

for url in urls:
    try:
        response = requests.get(
            url,
            timeout=20,
            headers={
                "User-Agent": "JobFlowAI/0.1 educational capstone",
                "Accept": "application/json,text/html",
            },
        )
        print(f"OK: {url} -> status_code={response.status_code}")
    except Exception as exc:
        print(f"ERRO: {url} -> {type(exc).__name__}: {exc}")

CATALOG = "workspace"
SCHEMA = "jobflow_ai"

SOURCE = "remoteok"

RAW_VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_remoteok_jobs"
INGESTION_RUNS_TABLE = f"{CATALOG}.{SCHEMA}.ingestion_runs"

REMOTEOK_API_URL = "https://remoteok.com/api"

MAX_JOBS_TO_BRONZE = 500

print("=" * 70)
print("JOBFLOW AI — INGESTÃO REMOTEOK")
print("=" * 70)
print(f"Catálogo: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Fonte: {SOURCE}")
print(f"API: {REMOTEOK_API_URL}")
print(f"Volume raw: {RAW_VOLUME_PATH}")
print(f"Tabela Bronze: {BRONZE_TABLE}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validar contexto do Databricks

# COMMAND ----------

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

context_df = spark.sql(
    """
    SELECT
        current_user() AS current_user,
        current_catalog() AS current_catalog,
        current_schema() AS current_schema
    """
)

display(context_df)

display(spark.sql(f"SHOW VOLUMES IN `{CATALOG}`.`{SCHEMA}`"))
display(spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{SCHEMA}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criar tabela Bronze da RemoteOK

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`bronze_remoteok_jobs` (
        run_id STRING NOT NULL,
        source STRING NOT NULL,
        source_job_id STRING,
        slug STRING,
        company STRING,
        position STRING,
        location STRING,
        date STRING,
        tags STRING,
        salary_min BIGINT,
        salary_max BIGINT,
        apply_url STRING,
        job_url STRING,
        payload STRING NOT NULL,
        ingested_at TIMESTAMP NOT NULL,
        file_path STRING
    )
    USING DELTA
    """
)

print(f"Tabela pronta: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Funções auxiliares

# COMMAND ----------

def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def safe_int(value: Any) -> int | None:
    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def fetch_remoteok_jobs() -> list[dict[str, Any]]:
    headers = {
        "User-Agent": "JobFlowAI/0.1 educational capstone",
        "Accept": "application/json",
    }

    response = requests.get(
        REMOTEOK_API_URL,
        headers=headers,
        timeout=60,
    )

    if response.status_code >= 400:
        raise RuntimeError(
            f"Erro ao chamar RemoteOK. "
            f"status_code={response.status_code}, "
            f"response={response.text[:500]}"
        )

    data = response.json()

    if not isinstance(data, list):
        raise TypeError(
            f"Resposta inesperada da RemoteOK. "
            f"Tipo recebido: {type(data).__name__}"
        )

    return data


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


def write_raw_json(
    *,
    run_id: str,
    payload: list[dict[str, Any]],
    metadata: dict[str, Any],
) -> str:
    raw_dir = f"{RAW_VOLUME_PATH}/source={SOURCE}/run_id={run_id}"

    ensure_dir(raw_dir)

    file_path = f"{raw_dir}/remoteok_jobs.json"

    envelope = {
        "source": SOURCE,
        "run_id": run_id,
        "requested_at_utc": utc_now_naive().isoformat(),
        "api_url": REMOTEOK_API_URL,
        "metadata": metadata,
        "payload": payload,
    }

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(envelope, file, ensure_ascii=False, indent=2)

    return file_path


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
        "source": SOURCE,
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

# MAGIC %md
# MAGIC ## 4. Chamar API e salvar JSON bruto

# COMMAND ----------

run_id = str(uuid.uuid4())
started_at = utc_now_naive()

status = "started"
error_message = None
raw_file_count = 0
bronze_rows = []

print(f"run_id: {run_id}")

try:
    remoteok_data = fetch_remoteok_jobs()

    metadata, jobs = split_remoteok_response(remoteok_data)

    print(f"Itens recebidos da API: {len(remoteok_data)}")
    print(f"Vagas identificadas: {len(jobs)}")

    if metadata:
        print("Metadata encontrada no primeiro item da resposta.")
        print(f"Campos de metadata: {list(metadata.keys())}")

    file_path = write_raw_json(
        run_id=run_id,
        payload=remoteok_data,
        metadata=metadata,
    )

    raw_file_count = 1

    print(f"Arquivo bruto salvo em: {file_path}")

    jobs_to_write = jobs[:MAX_JOBS_TO_BRONZE]

    bronze_rows = [
        normalize_job_row(
            run_id=run_id,
            job=job,
            file_path=file_path,
        )
        for job in jobs_to_write
    ]

    finished_at = utc_now_naive()
    status = "success"

except Exception as exc:
    finished_at = utc_now_naive()
    status = "failed"
    error_message = f"{type(exc).__name__}: {exc}"

    print("ERRO NA INGESTÃO REMOTEOK")
    print(error_message)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gravar dados na tabela Bronze

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

    records_bronze = bronze_df.count()

    print(f"OK: {records_bronze} registros gravados em {BRONZE_TABLE}")
else:
    records_bronze = 0
    print("Nenhum registro Bronze para gravar.")



# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Registrar execução na ingestion_runs

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
        "query": "remoteok full feed",
        "location": "remote",
        "started_at": started_at,
        "finished_at": finished_at,
        "status": status,
        "records_raw": raw_file_count,
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

print(f"OK: execução registrada em {INGESTION_RUNS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validar dados ingeridos

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

# MAGIC %md
# MAGIC ## 8. Resultado final

# COMMAND ----------

print()
print("=" * 70)
print("RESULTADO: PRIMEIRA INGESTÃO REMOTEOK CONCLUÍDA")
print("=" * 70)
print(f"run_id: {run_id}")
print(f"status: {status}")
print(f"raw files: {raw_file_count}")
print(f"bronze records: {records_bronze}")
print(f"raw path: {RAW_VOLUME_PATH}/source={SOURCE}/run_id={run_id}")
print(f"bronze table: {BRONZE_TABLE}")
print(f"ingestion table: {INGESTION_RUNS_TABLE}")
print("=" * 70)

if status == "failed":
    raise RuntimeError(error_message)