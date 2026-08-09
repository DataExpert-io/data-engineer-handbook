# Roteiro de Demo — JobFlow AI

Este documento define a ordem recomendada para apresentar o projeto JobFlow AI.

O objetivo é mostrar, de forma clara, como o projeto atende aos requisitos do capstone usando Databricks, Spark, Delta Tables, processamento de texto, matching de vagas, ferramentas de agente e frontend simulado.

---

## 1. Abertura

### Mensagem principal

JobFlow AI é um copiloto de busca e acompanhamento de vagas.

Ele ajuda uma pessoa candidata a:

- encontrar vagas relevantes;
- entender o grau de compatibilidade com o perfil;
- salvar vagas de interesse;
- criar aplicações;
- acompanhar o status das candidaturas;
- registrar notas de entrevista.

### Problema

Buscar emprego envolve muitas etapas manuais:

- procurar vagas em diferentes fontes;
- ler descrições longas;
- identificar requisitos;
- comparar com o próprio perfil;
- salvar oportunidades;
- acompanhar candidaturas;
- lembrar follow-ups e entrevistas.

O JobFlow AI organiza esse processo em uma plataforma de dados com apoio de um agente.

---

## 2. Visão geral da arquitetura

### Fluxo principal

```text
RemoteOK API / JSON manual
        ↓
Bronze
        ↓
Silver
        ↓
Gold
        ↓
Chunks de descrição das vagas
        ↓
Extração de requisitos e skills
        ↓
Matching perfil x vagas
        ↓
Tabelas transacionais Delta
        ↓
Ferramentas do agente
        ↓
Mini copiloto textual
        ↓
Frontend simulado
        ↓
Validação final
```

### Tecnologias usadas

```text
Databricks
Spark
Delta Tables
Unity Catalog
Python
SQL
Notebook como frontend simulado
Git/GitHub
```

---

## 3. Estrutura do projeto

Mostrar no repositório a pasta:

```text
projects-jobflow-ai/
```

Principais notebooks:

```text
first_environment.py
setup_plataforma.py
remoteok.py
remote_manual.py
silver_remoteok.py
transformacao_gold.py
chunks_descricao_vagas.py
extrai_requisitos.py
matching_perfil_vagas.py
tabelas_delta.py
funcoes_agente.py
copilot_textual.py
frontend_simulado.py.ipynb
validacao_final.py
```

Também mostrar:

```text
README.md
ROTEIRO_DEMO.md
```

---

## 4. Ambiente e setup

### Notebook

```text
first_environment.py
```

O que mostrar:

- versão do Spark;
- usuário atual;
- catálogo ativo;
- schemas disponíveis.

### Mensagem da demo

Este notebook valida que o ambiente Databricks está funcionando e que conseguimos acessar Spark e Unity Catalog.

---

## 5. Criação da plataforma

### Notebook

```text
setup_plataforma.py
```

O que mostrar:

- schema criado:

```text
workspace.jobflow_ai
```

- volumes criados:

```text
/Volumes/workspace/jobflow_ai/raw
/Volumes/workspace/jobflow_ai/checkpoints
/Volumes/workspace/jobflow_ai/documents
```

- tabela de auditoria:

```text
workspace.jobflow_ai.ingestion_runs
```

### Mensagem da demo

Aqui foi criada a base do projeto: schema, volumes e tabela de auditoria para registrar as execuções de ingestão.

---

## 6. Ingestão de dados externos

### Notebooks

```text
remoteok.py
remote_manual.py
```

### O que mostrar

A chamada direta à API RemoteOK apresentou falha de DNS no compute utilizado. Essa falha foi registrada na tabela de auditoria.

Depois, foi feito fallback manual com upload do JSON baixado da própria RemoteOK.

Resultado validado:

```text
bronze records: 100
```

### Mensagem da demo

Mesmo com a falha de DNS do ambiente, o projeto manteve rastreabilidade. A falha foi auditada e o fallback manual permitiu continuar o pipeline com os dados da API externa.

---

## 7. Pipeline Bronze, Silver e Gold

### Notebooks

```text
silver_remoteok.py
transformacao_gold.py
```

### Tabelas

```text
workspace.jobflow_ai.bronze_remoteok_jobs
workspace.jobflow_ai.silver_remoteok_jobs
workspace.jobflow_ai.gold_job_postings
```

### O que mostrar

- Bronze com dados brutos;
- Silver com dados normalizados;
- Gold com vagas prontas para análise.

Resultado esperado:

```text
bronze: 100 registros
silver: 100 registros
gold: 100 registros
```

### Mensagem da demo

O pipeline Spark transforma os dados da API em camadas organizadas, seguindo o padrão Bronze, Silver e Gold.

---

## 8. Processamento de dados não estruturados

### Notebook

```text
chunks_descricao_vagas.py
```

### Tabela

```text
workspace.jobflow_ai.gold_job_description_chunks
```

### O que mostrar

As descrições das vagas são textos livres. O projeto transforma essas descrições em chunks.

Resultado validado:

```text
total chunks: 210
jobs with chunks: 100
```

### Mensagem da demo

Essa etapa atende ao requisito de processamento de dados não estruturados. As descrições das vagas são preparadas para recuperação e análise textual.

---

## 9. Extração de requisitos e skills

### Notebook

```text
extrai_requisitos.py
```

### Tabelas

```text
workspace.jobflow_ai.skills_catalog
workspace.jobflow_ai.gold_job_requirement_sentences
workspace.jobflow_ai.gold_job_skill_matches
```

### O que mostrar

Resultado validado:

```text
skills catalog: 36 rows
sentences extracted: 1255
skill matches saved: 37
```

### Mensagem da demo

A partir das descrições das vagas, o projeto extrai sentenças de requisitos e identifica skills relevantes.

---

## 10. Matching perfil x vagas

### Notebook

```text
matching_perfil_vagas.py
```

### Tabelas

```text
workspace.jobflow_ai.demo_user_profiles
workspace.jobflow_ai.demo_user_profile_skills
workspace.jobflow_ai.gold_job_match_scores
```

### O que mostrar

Resultado validado:

```text
match scores: 100
top score: 86.5
```

### Mensagem da demo

Aqui o projeto compara o perfil demo com as vagas e calcula uma pontuação de compatibilidade para cada vaga.

---

## 11. Tabelas transacionais do app

### Notebook

```text
tabelas_delta.py
```

### Tabelas

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

### Mensagem da demo

Como Lakebase não estava visível no workspace, foram usadas Delta Tables como camada transacional alternativa.

Essas tabelas permitem gravar ações reais do usuário, como salvar vaga, criar aplicação e adicionar nota de entrevista.

---

## 12. Ferramentas do agente

### Notebook

```text
funcoes_agente.py
```

### Ferramentas de leitura

```text
buscar_vagas_recomendadas
obter_detalhes_vaga
listar_aplicacoes
encontrar_aplicacoes_paradas
```

### Ferramentas de escrita

```text
salvar_vaga
criar_aplicacao
atualizar_etapa_aplicacao
adicionar_nota_entrevista
```

### Mensagem da demo

O agente não apenas consulta dados. Ele também executa ações reais de escrita nas tabelas transacionais.

---

## 13. Mini copiloto textual

### Notebook

```text
copilot_textual.py
```

### Comandos demonstrados

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

### Mensagem da demo

O mini copiloto interpreta comandos simples em linguagem natural e chama a ferramenta correta.

Ele demonstra a lógica de um agente com ferramentas de leitura e escrita.

---

## 14. Frontend simulado

### Notebook

```text
frontend_simulado.py.ipynb
```

### O que mostrar

A tela mostra:

```text
100 vagas publicadas
100 vagas ranqueadas
vagas salvas
aplicações
notas de entrevista
```

Também mostra:

- perfil do usuário demo;
- top vagas recomendadas;
- vagas salvas;
- aplicações criadas;
- notas de entrevista.

### Mensagem da demo

Como Databricks Apps não estava visível no workspace, foi criado um frontend simulado em notebook para demonstrar a experiência do usuário final.

---

## 15. Validação final

### Notebook

```text
validacao_final.py
```

### Resultado validado

```text
RESULTADO: VALIDAÇÃO FINAL CONCLUÍDA
Requisitos validados: 6/6
```

### Componentes verificados

```text
- tabelas Bronze, Silver e Gold
- auditoria de ingestão
- chunks de descrições de vagas
- extração de requisitos e skills
- matching perfil x vagas
- tabelas transacionais do app
- ações gravadas pelo copiloto
- frontend simulado
```

### Mensagem da demo

A validação final confirma que os principais requisitos foram atendidos e que o projeto está funcional ponta a ponta.

---

## 16. Requisitos do capstone atendidos

```text
[x] Spark data pipeline
[x] API externa
[x] Processamento de dados não estruturados
[x] Frontend / interface
[x] Agente com ferramentas de leitura
[x] Agente com ferramentas de escrita real
```

---

## 17. Limitações conhecidas

```text
- A chamada direta à API RemoteOK falhou por DNS no compute utilizado.
- O fallback foi feito com upload manual do JSON da RemoteOK.
- Lakebase não estava visível no workspace.
- Databricks Apps não estava visível no workspace.
- O frontend foi simulado em notebook.
- Embeddings e Vector Search podem ser adicionados como evolução futura.
```

### Mensagem da demo

As limitações foram tratadas de forma explícita, com fallback, auditoria e alternativas funcionais em Delta Tables e notebooks.

---

## 18. Próximos passos

```text
- adicionar embeddings reais;
- usar Vector Search para busca semântica;
- transformar o notebook em Databricks App quando disponível;
- integrar mais APIs de vagas;
- adicionar múltiplos usuários;
- melhorar o agente com LLM;
- criar alertas para aplicações paradas;
- gerar cartas de apresentação automaticamente.
```

---

## 19. Fechamento

### Mensagem final

O JobFlow AI demonstra uma solução de dados completa no Databricks.

Ele cobre desde ingestão e transformação com Spark até processamento de texto, matching, persistência transacional, ferramentas de agente, frontend simulado e validação final.

O projeto está versionado no GitHub e validado com evidência de execução ponta a ponta.