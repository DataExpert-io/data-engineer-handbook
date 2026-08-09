# Databricks notebook source
# MAGIC %md
# MAGIC # JobFlow AI — Verificação do ambiente
# MAGIC
# MAGIC Este notebook valida se o ambiente Databricks está pronto
# MAGIC para iniciar o projeto.

# COMMAND ----------

import platform
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

PROJECT_NAME = "jobflow-ai"

print("=" * 70)
print("JOBFLOW AI — ENVIRONMENT CHECK")
print("=" * 70)
print(f"Projeto: {PROJECT_NAME}")
print(f"Python: {sys.version.split()[0]}")
print(f"Plataforma Python: {platform.platform()}")
print(f"Spark: {spark.version}")
print(f"Horário UTC: {datetime.now(timezone.utc).isoformat()}")
print("=" * 70)

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

current_user = context["current_user"]
current_catalog = context["current_catalog"]
current_schema = context["current_schema"]

print(f"Usuário atual: {current_user}")
print(f"Catálogo atual: {current_catalog}")
print(f"Schema atual: {current_schema}")

# COMMAND ----------

catalogs_df = spark.sql("SHOW CATALOGS")
display(catalogs_df)

catalog_count = catalogs_df.count()

print(f"Quantidade de catálogos encontrados: {catalog_count}")

assert catalog_count > 0, "Nenhum catálogo encontrado."

# COMMAND ----------

schemas_df = spark.sql(f"SHOW SCHEMAS IN `{current_catalog}`")
display(schemas_df)

print(f"Schemas encontrados em {current_catalog}: {schemas_df.count()}")

# COMMAND ----------

test_df = (
    spark.range(1, 6)
    .withColumn("project", F.lit(PROJECT_NAME))
    .withColumn("execution_user", F.lit(current_user))
    .withColumn("checked_at", F.current_timestamp())
)

display(test_df)

assert test_df.count() == 5, "O teste Spark não retornou 5 linhas."

print("OK: Spark DataFrame executado com sucesso.")

# COMMAND ----------

test_df.createOrReplaceTempView("jobflow_environment_check")

sql_test_df = spark.sql(
    """
    SELECT
        project,
        COUNT(*) AS row_count,
        MAX(checked_at) AS checked_at
    FROM jobflow_environment_check
    GROUP BY project
    """
)

display(sql_test_df)

result = sql_test_df.first()

assert result["project"] == PROJECT_NAME
assert result["row_count"] == 5

print("OK: Spark SQL executado com sucesso.")

# COMMAND ----------

print()
print("=" * 70)
print("RESULTADO: AMBIENTE SPARK VALIDADO")
print("=" * 70)
print(f"Usuário: {current_user}")
print(f"Catálogo atual: {current_catalog}")
print(f"Schema atual: {current_schema}")
print(f"Spark: {spark.version}")
print("=" * 70)