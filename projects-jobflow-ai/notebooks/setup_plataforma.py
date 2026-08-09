# Databricks notebook source
# MAGIC %md
# MAGIC # JobFlow AI — Platform Setup
# MAGIC
# MAGIC Este notebook cria a fundação de dados do projeto:
# MAGIC
# MAGIC - schema no Unity Catalog
# MAGIC - volumes para dados brutos, checkpoints e documentos
# MAGIC - tabela de controle de ingestão
# MAGIC - tabela Bronze inicial da Adzuna

# COMMAND ----------

from datetime import datetime, timezone

PROJECT_NAME = "jobflow-ai"

CATALOG = "workspace"
SCHEMA = "jobflow_ai"

RAW_VOLUME = "raw"
CHECKPOINT_VOLUME = "checkpoints"
DOCUMENTS_VOLUME = "documents"

print("=" * 70)
print("JOBFLOW AI — PLATFORM SETUP")
print("=" * 70)
print(f"Projeto: {PROJECT_NAME}")
print(f"Catálogo planejado: {CATALOG}")
print(f"Schema planejado: {SCHEMA}")
print(f"Horário UTC: {datetime.now(timezone.utc).isoformat()}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validar contexto atual

# COMMAND ----------

context_df = spark.sql(
    """
    SELECT
        current_user() AS current_user,
        current_catalog() AS current_catalog,
        current_schema() AS current_schema
    """
)

display(context_df)

context = context_df.first()

print(f"Usuário atual: {context['current_user']}")
print(f"Catálogo atual: {context['current_catalog']}")
print(f"Schema atual: {context['current_schema']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criar schema do projeto

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

print(f"Schema pronto: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criar volumes do projeto

# COMMAND ----------

volumes = [
    RAW_VOLUME,
    CHECKPOINT_VOLUME,
    DOCUMENTS_VOLUME,
]

for volume in volumes:
    spark.sql(
        f"""
        CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{volume}`
        """
    )
    print(f"Volume pronto: {CATALOG}.{SCHEMA}.{volume}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Mostrar volumes criados

# COMMAND ----------

volumes_df = spark.sql(f"SHOW VOLUMES IN `{CATALOG}`.`{SCHEMA}`")
display(volumes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Criar tabela de controle de ingestão

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`ingestion_runs` (
        run_id STRING NOT NULL,
        source STRING NOT NULL,
        query STRING,
        location STRING,
        started_at TIMESTAMP NOT NULL,
        finished_at TIMESTAMP,
        status STRING NOT NULL,
        records_raw BIGINT,
        records_bronze BIGINT,
        error_message STRING,
        created_at TIMESTAMP NOT NULL
    )
    USING DELTA
    """
)

print(f"Tabela pronta: {CATALOG}.{SCHEMA}.ingestion_runs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Criar tabela Bronze inicial da Adzuna

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`bronze_adzuna_jobs` (
        run_id STRING NOT NULL,
        source STRING NOT NULL,
        query STRING,
        location STRING,
        page BIGINT,
        source_job_id STRING,
        payload STRING NOT NULL,
        ingested_at TIMESTAMP NOT NULL,
        file_path STRING
    )
    USING DELTA
    """
)

print(f"Tabela pronta: {CATALOG}.{SCHEMA}.bronze_adzuna_jobs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validar tabelas criadas

# COMMAND ----------

tables_df = spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{SCHEMA}`")
display(tables_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Inserir registro de teste controlado

# COMMAND ----------

test_run_id = "platform_setup_test"

spark.sql(
    f"""
    DELETE FROM `{CATALOG}`.`{SCHEMA}`.`ingestion_runs`
    WHERE run_id = '{test_run_id}'
    """
)

spark.sql(
    f"""
    INSERT INTO `{CATALOG}`.`{SCHEMA}`.`ingestion_runs`
    SELECT
        '{test_run_id}' AS run_id,
        'system' AS source,
        'environment validation' AS query,
        'workspace' AS location,
        current_timestamp() AS started_at,
        current_timestamp() AS finished_at,
        'success' AS status,
        0 AS records_raw,
        0 AS records_bronze,
        NULL AS error_message,
        current_timestamp() AS created_at
    """
)

validation_df = spark.sql(
    f"""
    SELECT *
    FROM `{CATALOG}`.`{SCHEMA}`.`ingestion_runs`
    WHERE run_id = '{test_run_id}'
    """
)

display(validation_df)

assert validation_df.count() == 1, "O registro de teste não foi criado corretamente."

print("OK: registro de teste criado com sucesso.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resultado final

# COMMAND ----------

print()
print("=" * 70)
print("RESULTADO: FUNDAÇÃO DE DADOS CRIADA")
print("=" * 70)
print(f"Catálogo: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Volume raw: /Volumes/{CATALOG}/{SCHEMA}/{RAW_VOLUME}")
print(f"Volume checkpoints: /Volumes/{CATALOG}/{SCHEMA}/{CHECKPOINT_VOLUME}")
print(f"Volume documents: /Volumes/{CATALOG}/{SCHEMA}/{DOCUMENTS_VOLUME}")
print(f"Tabela: {CATALOG}.{SCHEMA}.ingestion_runs")
print(f"Tabela: {CATALOG}.{SCHEMA}.bronze_adzuna_jobs")
print("=" * 70)