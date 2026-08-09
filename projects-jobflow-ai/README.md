# JobFlow AI

JobFlow AI is a Databricks-based job hunting copilot.

The project ingests job postings from an external source, processes them through a Spark Bronze/Silver/Gold pipeline, extracts information from unstructured job descriptions, ranks jobs against a demo user profile, stores user actions in transactional Delta tables, and exposes agent-style tools that can both read and write data.

The goal is to help a candidate discover relevant jobs, understand match quality, save opportunities, create applications, track pipeline status, and manage interview notes.

---

## Capstone Requirements Coverage

| Requirement | Status | Implementation |
|---|---:|---|
| Spark data pipeline | Complete | Bronze, Silver, and Gold Delta tables built with Spark |
| Third-party API integration | Complete | RemoteOK API integration, with audited fallback due to DNS issues in the compute environment |
| Unstructured data processing | Complete | Job descriptions are processed into chunks, requirement sentences, and skill matches |
| Databricks App / frontend | Complete with fallback | Simulated frontend notebook plus a Streamlit app prepared for Databricks Apps |
| AI agent with tools | Complete | Read tools and write tools implemented over Delta tables |
| Search / retrieval capability | Complete | Recommended jobs, job details, applications, and stale applications can be retrieved |
| Real write actions | Complete | The copilot can save jobs, create applications, update stages, and add interview notes |

---

## Project Overview

JobFlow AI was designed as an AI Job Hunting Copilot.

Users can describe their skills, target roles, and preferences. The system then ranks job postings against the user profile and provides tools to manage the job application pipeline.

The project demonstrates the following capabilities:

- ingesting external job data;
- transforming raw data with Spark;
- processing unstructured job descriptions;
- extracting skills and requirement sentences;
- matching a user profile against job postings;
- persisting user actions in Delta tables;
- simulating an AI agent with read and write tools;
- presenting the result through a frontend notebook;
- preparing a Streamlit app for future Databricks Apps deployment;
- validating the full project with a final validation notebook.

---

## Architecture

```text
RemoteOK API / manual JSON fallback
        ↓
Bronze Delta table
        ↓
Silver Delta table
        ↓
Gold Delta table
        ↓
Job description chunks
        ↓
Requirement and skill extraction
        ↓
Profile-to-job matching
        ↓
Transactional Delta app tables
        ↓
Agent tools
        ↓
Mini text copilot
        ↓
Simulated frontend notebook
        ↓
Final validation notebook
```

---

## Databricks Environment

The project uses:

```text
Catalog: workspace
Schema: jobflow_ai
```

Volumes:

```text
/Volumes/workspace/jobflow_ai/raw
/Volumes/workspace/jobflow_ai/checkpoints
/Volumes/workspace/jobflow_ai/documents
```

Main technologies:

```text
Databricks
Apache Spark
Delta Tables
Unity Catalog
Python
SQL
Streamlit
Git / GitHub
```

---

## External Data Source

The selected external source is the RemoteOK API.

RemoteOK provides remote job listings in JSON format and does not require an API key.

During development, the Databricks compute environment was not able to resolve some external domains, including `remoteok.com`. The direct API call failed because of DNS/network restrictions.

This failure was intentionally captured in the ingestion audit table.

To continue the project, the RemoteOK JSON response was downloaded manually and uploaded into the raw volume. The fallback kept the project traceable while still using data from the selected third-party source.

---

## Ingestion Audit

The project uses the following table to track ingestion runs:

```text
workspace.jobflow_ai.ingestion_runs
```

This table records:

```text
source
status
started_at
finished_at
records_raw
records_bronze
error_message
```

It includes both the failed direct API attempt and the successful manual fallback run.

---

## Data Pipeline

The project follows a Bronze, Silver, and Gold architecture.

### Bronze

```text
workspace.jobflow_ai.bronze_remoteok_jobs
```

Stores raw job records from RemoteOK, including the original payload.

### Silver

```text
workspace.jobflow_ai.silver_remoteok_jobs
```

Cleans and normalizes the main job fields.

### Gold

```text
workspace.jobflow_ai.gold_job_postings
```

Stores the analytical job posting dataset used by downstream matching, app tables, and agent tools.

Validated result:

```text
Bronze records: 100
Silver records: 100
Gold job postings: 100
```

---

## Unstructured Data Processing

Job descriptions are free-text fields, so the project processes them as unstructured data.

The main table for this step is:

```text
workspace.jobflow_ai.gold_job_description_chunks
```

The process includes:

```text
text cleaning
description chunking
requirement sentence extraction
skill matching
job-to-skill relationship creation
```

Validated result:

```text
Description chunks: 210
Jobs with chunks: 100
Requirement sentences extracted: 1255
Skill catalog rows: 36
Skill matches saved: 37
```

Related tables:

```text
workspace.jobflow_ai.gold_job_description_chunks
workspace.jobflow_ai.skills_catalog
workspace.jobflow_ai.gold_job_requirement_sentences
workspace.jobflow_ai.gold_job_skill_matches
```

---

## Profile-to-Job Matching

The project creates a demo user profile and compares it against the available job postings.

Related tables:

```text
workspace.jobflow_ai.demo_user_profiles
workspace.jobflow_ai.demo_user_profile_skills
workspace.jobflow_ai.gold_job_match_scores
```

The matching process ranks jobs according to the user profile and extracted job skills.

Validated result:

```text
Match scores: 100
Top score: 86.5
```

---

## Transactional App Tables

Lakebase was not visible in the workspace during development. To keep the project functional, transactional app-style tables were implemented with Delta Tables.

These tables store user-facing entities and actions:

```text
workspace.jobflow_ai.app_users
workspace.jobflow_ai.app_profiles
workspace.jobflow_ai.app_skills
workspace.jobflow_ai.app_job_postings
workspace.jobflow_ai.app_saved_jobs
workspace.jobflow_ai.app_applications
workspace.jobflow_ai.app_interview_notes
workspace.jobflow_ai.app_contacts
```

This layer supports real write actions such as:

```text
saving a job
creating an application
updating an application stage
adding interview notes
```

---

## Agent Tools

The project implements agent-style tools as Python functions.

### Read Tools

```text
buscar_vagas_recomendadas
obter_detalhes_vaga
listar_aplicacoes
encontrar_aplicacoes_paradas
```

### Write Tools

```text
salvar_vaga
criar_aplicacao
atualizar_etapa_aplicacao
adicionar_nota_entrevista
```

These tools demonstrate that the agent can retrieve data and also perform real write actions against the project tables.

---

## Mini Text Copilot

The notebook `copilot_textual.py` implements a simple natural-language command router through the function:

```text
responder_comando
```

Validated commands:

```text
mostrar vagas recomendadas
detalhes da primeira vaga
salvar primeira vaga com prioridade alta
criar aplicacao para primeira vaga
listar aplicacoes
mover aplicacao para entrevista
adicionar nota de entrevista
encontrar aplicacoes paradas
```

This validates the basic agent loop:

```text
natural-language command
        ↓
intent detection
        ↓
tool selection
        ↓
read or write operation
        ↓
result displayed to user
```

---

## Frontend

### Simulated Frontend Notebook

Because Databricks Apps was not visible in the workspace, the project includes a notebook-based simulated frontend:

```text
projects-jobflow-ai/notebooks/frontend_simulado.py.ipynb
```

The simulated frontend displays:

```text
user profile
published jobs
ranked jobs
saved jobs
applications
interview notes
top recommended jobs
```

Validated metrics shown in the frontend:

```text
100 published jobs
100 ranked jobs
saved jobs
applications
interview notes
```

### Streamlit App Prepared for Databricks Apps

The project also includes a Streamlit app prepared for future deployment as a Databricks App:

```text
projects-jobflow-ai/app/app.py
projects-jobflow-ai/app/app.yaml
projects-jobflow-ai/app/requirements.txt
projects-jobflow-ai/app/README_APP.md
```

The Streamlit app is designed to:

```text
show JobFlow AI metrics
list recommended jobs
list saved jobs
list applications
save a recommended job into app_saved_jobs
```

The app was not deployed because Databricks Apps was not available in the workspace UI, but the app structure is ready for deployment when that feature is available.

---

## Final Validation

The project includes a final validation notebook:

```text
projects-jobflow-ai/notebooks/validacao_final.py
```

The validation checks:

```text
table existence
main row counts
ingestion audit
job description chunks
requirement and skill extraction
profile-to-job matching
transactional app tables
copilot actions
frontend data readiness
```

Validated result:

```text
RESULTADO: VALIDAÇÃO FINAL CONCLUÍDA
Requisitos validados: 6/6
```

---

## Repository Structure

```text
projects-jobflow-ai/
├── README.md
├── README.pt-BR.md
├── README_APP.md
├── RESUMO_APRESENTACAO.md
├── roteiro.md
├── app/
│   ├── app.py
│   ├── app.yaml
│   ├── requirements.txt
│   └── README_APP.md
└── notebooks/
    ├── first_environment.py
    ├── setup_plataforma.py
    ├── remoteok.py
    ├── remote_manual.py
    ├── silver_remoteok.py
    ├── transformacao_gold.py
    ├── chunks_descricao_vagas.py
    ├── extrai_requisitos.py
    ├── matching_perfil_vagas.py
    ├── tabelas_delta.py
    ├── funcoes_agente.py
    ├── copilot_textual.py
    ├── frontend_simulado.py.ipynb
    └── validacao_final.py
```

Note: depending on how Databricks exports notebooks, some files may appear with `.py.ipynb` extensions.

---

## Main Notebooks

| Notebook | Purpose |
|---|---|
| `first_environment.py` | Validates Spark, user, catalog, and schema context |
| `setup_plataforma.py` | Creates schema, volumes, and ingestion audit table |
| `remoteok.py` | Attempts direct RemoteOK API ingestion and records failure if needed |
| `remote_manual.py` | Loads manually uploaded RemoteOK JSON into Bronze |
| `silver_remoteok.py` | Creates the cleaned Silver layer |
| `transformacao_gold.py` | Creates the analytical Gold job postings table |
| `chunks_descricao_vagas.py` | Processes job descriptions into text chunks |
| `extrai_requisitos.py` | Extracts requirement sentences and matches skills |
| `matching_perfil_vagas.py` | Scores jobs against the demo user profile |
| `tabelas_delta.py` | Creates transactional Delta app tables |
| `funcoes_agente.py` | Defines read and write tools for the agent |
| `copilot_textual.py` | Implements the mini natural-language copilot |
| `frontend_simulado.py.ipynb` | Provides a notebook-based frontend simulation |
| `validacao_final.py` | Validates the project end-to-end |

---

## Known Limitations and Mitigations

| Limitation | Mitigation |
|---|---|
| Direct RemoteOK API call failed because of DNS/network restrictions | Failure was audited, and a manually downloaded RemoteOK JSON file was used as fallback |
| Lakebase was not visible in the workspace | Delta Tables were used as a transactional app layer |
| Databricks Apps was not visible in the workspace | A simulated frontend notebook was created, and a Streamlit app was prepared for future deployment |
| Embeddings and Vector Search were not implemented | Text chunks and retrieval-ready tables were created as a foundation for future semantic search |
| The copilot uses rule-based intent detection | The tools are structured so they can be connected to a full LLM agent later |

---

## Evidence Summary

Validated project evidence:

```text
100 Bronze records
100 Silver records
100 Gold job postings
210 job description chunks
1255 extracted requirement sentences
36 skills in the skills catalog
37 job-skill matches
100 job match scores
saved jobs created by the copilot
applications created by the copilot
interview notes created by the copilot
6/6 final validation checks passed
```

---

## How to Review the Project

Recommended review order:

```text
1. README.md
2. roteiro.md
3. notebooks/validacao_final.py
4. notebooks/frontend_simulado.py.ipynb
5. notebooks/copilot_textual.py
6. notebooks/funcoes_agente.py
7. app/
```

Recommended demo order:

```text
1. Explain the problem and architecture
2. Show the Bronze/Silver/Gold pipeline
3. Show unstructured description chunks
4. Show skill extraction and match scores
5. Show agent tools
6. Run or show the mini text copilot
7. Show the simulated frontend
8. Show final validation: 6/6
9. Explain limitations and mitigations
10. Show the prepared Streamlit app folder
```

---

## Future Improvements

Potential next steps:

```text
add embeddings
add Databricks Vector Search
deploy the Streamlit app as a Databricks App
add more job APIs such as Adzuna or USAJobs
support multiple users
connect the tools to a full LLM agent
generate cover letter snippets
generate resume bullet suggestions
add alerts for stale applications
add follow-up reminders
```

---

## Final Status

JobFlow AI is functionally complete for the capstone submission.

It demonstrates a full Databricks data project with Spark ingestion, transformation, unstructured text processing, profile-to-job matching, transactional persistence, agent tools, frontend simulation, and final validation.
