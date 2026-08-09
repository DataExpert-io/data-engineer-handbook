import uuid
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config


CATALOG = "workspace"
SCHEMA = "jobflow_ai"
DEMO_USER_ID = "demo_user_001"

APP_JOB_POSTINGS_TABLE = f"{CATALOG}.{SCHEMA}.app_job_postings"
APP_SAVED_JOBS_TABLE = f"{CATALOG}.{SCHEMA}.app_saved_jobs"
APP_APPLICATIONS_TABLE = f"{CATALOG}.{SCHEMA}.app_applications"
APP_INTERVIEW_NOTES_TABLE = f"{CATALOG}.{SCHEMA}.app_interview_notes"
MATCH_SCORES_TABLE = f"{CATALOG}.{SCHEMA}.gold_job_match_scores"

cfg = Config()


def clean_hostname(host: str) -> str:
    return host.replace("https://", "").replace("http://", "").rstrip("/")


@st.cache_resource
def get_connection(http_path: str):
    server_hostname = clean_hostname(cfg.host)

    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
        _use_arrow_native_complex_types=False,
    )


def read_df(conn, query: str) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()


def execute_sql(conn, query: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(query)


def sql_literal(value) -> str:
    if value is None:
        return "NULL"

    return "'" + str(value).replace("'", "''") + "'"


st.set_page_config(
    page_title="JobFlow AI",
    page_icon="💼",
    layout="wide",
)

st.title("💼 JobFlow AI")
st.caption("Copiloto de busca, recomendação e acompanhamento de vagas.")

st.info(
    "Este app foi preparado para Databricks Apps com Streamlit. "
    "Ele usa Databricks SQL Connector para consultar tabelas do Unity Catalog."
)

with st.sidebar:
    st.header("Configuração")

    http_path = st.text_input(
        "SQL Warehouse HTTP Path",
        placeholder="/sql/1.0/warehouses/xxxxxxxxxxxxxxx",
        help="Cole aqui o HTTP Path de um SQL Warehouse do Databricks.",
    )

    st.divider()
    st.write("Usuário demo:")
    st.code(DEMO_USER_ID)

    st.write("Schema:")
    st.code(f"{CATALOG}.{SCHEMA}")


if not http_path:
    st.warning(
        "Informe o HTTP Path do SQL Warehouse na barra lateral para carregar os dados."
    )
    st.stop()


try:
    conn = get_connection(http_path)
except Exception as exc:
    st.error("Não foi possível conectar ao Databricks SQL Warehouse.")
    st.exception(exc)
    st.stop()


st.success("Conexão criada com sucesso.")

tabs = st.tabs(
    [
        "📊 Dashboard",
        "🎯 Recomendações",
        "⭐ Vagas salvas",
        "📝 Aplicações",
        "✍️ Ação: salvar vaga",
    ]
)


with tabs[0]:
    st.subheader("Dashboard do JobFlow AI")

    metrics_query = f"""
    SELECT 'Vagas publicadas' AS metric, COUNT(*) AS value
    FROM {APP_JOB_POSTINGS_TABLE}

    UNION ALL

    SELECT 'Vagas ranqueadas' AS metric, COUNT(*) AS value
    FROM {MATCH_SCORES_TABLE}
    WHERE user_id = {sql_literal(DEMO_USER_ID)}

    UNION ALL

    SELECT 'Vagas salvas' AS metric, COUNT(*) AS value
    FROM {APP_SAVED_JOBS_TABLE}
    WHERE user_id = {sql_literal(DEMO_USER_ID)}

    UNION ALL

    SELECT 'Aplicações' AS metric, COUNT(*) AS value
    FROM {APP_APPLICATIONS_TABLE}
    WHERE user_id = {sql_literal(DEMO_USER_ID)}

    UNION ALL

    SELECT 'Notas de entrevista' AS metric, COUNT(*) AS value
    FROM {APP_INTERVIEW_NOTES_TABLE}
    WHERE user_id = {sql_literal(DEMO_USER_ID)}
    """

    metrics_df = read_df(conn, metrics_query)

    cols = st.columns(len(metrics_df))

    for col, row in zip(cols, metrics_df.itertuples(index=False)):
        col.metric(row.metric, int(row.value))

    st.dataframe(metrics_df, use_container_width=True)


with tabs[1]:
    st.subheader("Top vagas recomendadas")

    recommended_query = f"""
    SELECT
        m.job_id,
        j.job_title,
        j.company_name,
        j.job_url,
        m.match_score
    FROM {MATCH_SCORES_TABLE} m
    LEFT JOIN {APP_JOB_POSTINGS_TABLE} j
        ON m.job_id = j.job_id
    WHERE m.user_id = {sql_literal(DEMO_USER_ID)}
    ORDER BY m.match_score DESC
    LIMIT 20
    """

    recommended_df = read_df(conn, recommended_query)

    st.dataframe(recommended_df, use_container_width=True)


with tabs[2]:
    st.subheader("Vagas salvas")

    saved_query = f"""
    SELECT
        s.saved_job_id,
        s.job_id,
        j.job_title,
        j.company_name,
        s.priority,
        s.notes,
        s.saved_at,
        s.updated_at
    FROM {APP_SAVED_JOBS_TABLE} s
    LEFT JOIN {APP_JOB_POSTINGS_TABLE} j
        ON s.job_id = j.job_id
    WHERE s.user_id = {sql_literal(DEMO_USER_ID)}
    ORDER BY s.updated_at DESC
    """

    saved_df = read_df(conn, saved_query)

    st.dataframe(saved_df, use_container_width=True)


with tabs[3]:
    st.subheader("Aplicações")

    applications_query = f"""
    SELECT
        a.application_id,
        a.job_id,
        j.job_title,
        j.company_name,
        a.stage,
        a.notes,
        a.created_at,
        a.updated_at
    FROM {APP_APPLICATIONS_TABLE} a
    LEFT JOIN {APP_JOB_POSTINGS_TABLE} j
        ON a.job_id = j.job_id
    WHERE a.user_id = {sql_literal(DEMO_USER_ID)}
    ORDER BY a.updated_at DESC
    """

    applications_df = read_df(conn, applications_query)

    st.dataframe(applications_df, use_container_width=True)


with tabs[4]:
    st.subheader("Salvar vaga recomendada")

    jobs_for_action_query = f"""
    SELECT
        m.job_id,
        j.job_title,
        j.company_name,
        m.match_score
    FROM {MATCH_SCORES_TABLE} m
    LEFT JOIN {APP_JOB_POSTINGS_TABLE} j
        ON m.job_id = j.job_id
    WHERE m.user_id = {sql_literal(DEMO_USER_ID)}
    ORDER BY m.match_score DESC
    LIMIT 20
    """

    jobs_for_action_df = read_df(conn, jobs_for_action_query)

    if jobs_for_action_df.empty:
        st.warning("Nenhuma vaga recomendada encontrada.")
    else:
        jobs_for_action_df["label"] = (
            jobs_for_action_df["job_title"].fillna("Sem título")
            + " | "
            + jobs_for_action_df["company_name"].fillna("Empresa não informada")
            + " | score="
            + jobs_for_action_df["match_score"].astype(str)
        )

        selected_label = st.selectbox(
            "Escolha uma vaga para salvar",
            jobs_for_action_df["label"].tolist(),
        )

        selected_row = jobs_for_action_df[
            jobs_for_action_df["label"] == selected_label
        ].iloc[0]

        priority = st.selectbox("Prioridade", ["high", "medium", "low"])

        notes = st.text_area(
            "Notas",
            value="Salva pelo Streamlit app do JobFlow AI.",
        )

        if st.button("Salvar vaga"):
            saved_job_id = f"{DEMO_USER_ID}_{uuid.uuid4().hex}"
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            insert_query = f"""
            INSERT INTO {APP_SAVED_JOBS_TABLE}
                (saved_job_id, user_id, job_id, priority, notes, saved_at, updated_at)
            VALUES
                (
                    {sql_literal(saved_job_id)},
                    {sql_literal(DEMO_USER_ID)},
                    {sql_literal(selected_row["job_id"])},
                    {sql_literal(priority)},
                    {sql_literal(notes)},
                    TIMESTAMP {sql_literal(now)},
                    TIMESTAMP {sql_literal(now)}
                )
            """

            execute_sql(conn, insert_query)

            st.success(f"Vaga salva com sucesso: {saved_job_id}")
            st.rerun()
