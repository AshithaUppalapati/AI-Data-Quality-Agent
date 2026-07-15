# AI Data Quality & Observability Agent

> LLM-powered anomaly detection, root-cause analysis, and automated remediation for modern data platforms.

---

## Overview

This project integrates Large Language Models into a modern data engineering stack to automate data quality monitoring, schema drift detection, root-cause analysis, SQL remediation, and documentation generation.

It combines **Apache Spark**, **Delta Lake**, **LangChain**, **FAISS**, and **Azure OpenAI** to build an AI-native data observability system.

---

## Why This Project Exists

Traditional data quality systems rely on static rules and manual triage. This project demonstrates how AI can:

- Understand anomalies in context
- Explain issues in plain English
- Suggest SQL fixes automatically
- Search historical patterns for root-cause hints
- Generate documentation on demand
- Act as an always-on analytics assistant

---

## Architecture

```
┌──────────────────────────┐
│   Ingestion / Pipelines  │
└──────────────┬───────────┘
               ▼
    ┌──────────────────────┐
    │   Spark DQ Metrics   │
    └──────────────┬───────┘
                   ▼
    ┌──────────────────────┐
    │  Anomaly Detection   │
    └──────────────┬───────┘
                   ▼
┌──────────────────────────────────────┐
│         LLM Intelligence Layer       │
│  · Root Cause Analysis               │
│  · SQL Remediation Generation        │
│  · Documentation Generation          │
└──────────────┬───────────────────────┘
               ▼
    ┌──────────────────────┐
    │  Vector Search (FAISS│
    │  Historical Issue RAG│
    └──────────────┬───────┘
                   ▼
        ┌──────────────────┐
        │   API / Assistant│
        └──────────────────┘
```

---

## Features & Roadmap

### ✅ Phase 1 — Foundations
- [x] Synthetic dataset with schema drift
- [x] Spark job for DQ metrics
- [x] Delta Lake storage for metrics

### ⚡ Phase 2 — LLM Intelligence
- [x] LLM-powered anomaly explanation
- [x] SQL remediation generator
- [x] Slack / Jira alert generator

### 🔍 Phase 3 — Vector Search
- [X] FAISS index for historical issues
- [x] Similarity-based root-cause suggestions

### 🤖 Phase 4 — RAG Analytics Assistant
- [] Metadata embeddings (dbt, Spark, Delta)
- [X] RAG assistant for pipeline questions
- [] Documentation generator

### 🌐 Phase 5 — API Layer
- [X] Task 1: Verify FastAPI + uvicorn installed
- [X] Task 2: App skeleton + GET /health endpoint
- [X] Task 3: POST /run-agent endpoint
- [X] Task 4: POST /ask endpoint (RAG assistant)
- [x] Task 5: GET /reports and GET /reports/{id} endpoints
- [x] Task 6: API tests
- [x] Task 7: Docs, commit, push

---

## API Layer

A FastAPI service exposes the full pipeline over HTTP so it can be triggered, queried, and audited without touching Spark or the CLI directly.

Run locally:

​```bash
cd src/api
uvicorn main:app --reload
​```

Runs on `http://127.0.0.1:8000`; interactive Swagger docs at `/docs`.

| Method | Endpoint | Description |
|---|---|---|
| `GET`  | `/health` | Liveness check |
| `POST` | `/run-agent` | Runs the full DQ pipeline (metrics → anomaly detection → root cause → SQL remediation → alerts → RAG) and returns the report |
| `POST` | `/ask` | Ask the RAG assistant a question about pipeline health/history |
| `GET`  | `/reports` | List all past pipeline runs, newest first |
| `GET`  | `/reports/{id}` | Fetch the full report for a specific run |

### Example: run the pipeline

​```bash
curl.exe -X POST http://127.0.0.1:8000/run-agent -H "Content-Type: application/json" -d "{}"
​```

💰 Each call spins up a real Spark session and makes real LLM calls — roughly $0.001 and 30-60s per run.

### Example: ask a question

​```powershell
$body = @{ question = "Has schema drift happened before in this pipeline?" } | ConvertTo-Json
Invoke-RestMethod -Uri http://127.0.0.1:8000/ask -Method Post -ContentType "application/json" -Body $body
​```

Pass `"include_current_context": true` to ground the answer in live pipeline metrics (spins up Spark; slower).

### Example: browse past runs

​```powershell
Invoke-RestMethod -Uri http://127.0.0.1:8000/reports -Method Get
Invoke-RestMethod -Uri http://127.0.0.1:8000/reports/<id> -Method Get
​```

### Tests

​```bash
pytest tests/test_api.py -v
​```

`/run-agent` and `/ask` are tested against mocked pipeline functions — the suite never triggers real Spark sessions or OpenAI calls. `/health` and `/reports` are tested against real file I/O in a temp directory.

---

## Tech Stack

| Category | Tools |
|---|---|
| **Data & Compute** | Apache Spark / PySpark, Delta Lake, Databricks |
| **AI & Retrieval** | LangChain, Azure OpenAI / OpenAI API, FAISS |
| **Serving** | FastAPI, Python 3.10+, Docker |

---

## Project Structure

```
AI-Data-Quality-Agent/
├── src/
│   ├── dq_metrics/
│   ├── anomaly_detector/
│   ├── llm_agent/
│   ├── vector_search/
│   └── api/
├── notebooks/
├── docs/
└── README.md
```

---

## Current Status

🚧 **In Progress** — Setting up project structure and initial components.

---

## Contact

**Ashitha Uppalapati** · Senior Data Engineer  
[LinkedIn](https://linkedin.com/in/ashitha-u) · [GitHub](https://github.com/AshithaUppalapati)
