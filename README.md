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
- [ ] Task 1: Verify FastAPI + uvicorn installed
- [ ] Task 2: App skeleton + GET /health endpoint
- [ ] Task 3: POST /run-agent endpoint
- [ ] Task 4: POST /ask endpoint (RAG assistant)
- [ ] Task 5: GET /reports and GET /reports/{id} endpoints
- [ ] Task 6: API tests
- [ ] Task 7: Docs, commit, push

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
