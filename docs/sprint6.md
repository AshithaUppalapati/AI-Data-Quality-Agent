# Sprint 6 — CI, Health Score Consistency, API Hardening, Structured Logging, Metadata & Docs

## What Was Built

Sprint 6 closed out the project across six independent tasks, moving it from "working locally" to something closer to production-shaped:

1. **CI** — `.github/workflows/ci.yml` runs the full pytest suite on every push and pull request against `main`, with Java 17 and pip/Ivy caching so PySpark/Delta don't redownload on every run. No `OPENAI_API_KEY` secret is configured for the CI environment, deliberately — that's what proves the test suite never makes a real OpenAI call.
2. **Health score recalibration** — `calculate_health_score()` in `anomaly_detector.py` became the single place that turns anomaly counts into a health score and status label, with `SCORING_WEIGHTS` and `HEALTH_STATUS_BANDS` as tunable constants. A `recent_batches` parameter was threaded through `metrics_reader.py` → `agent_orchestrator.py` → `main.py`, so `/run-agent` can score "current state" (most recent N batches) separately from a full historical audit.
3. **API key auth** — a `verify_api_key` FastAPI dependency now guards `/run-agent`, `/ask`, `/reports`, and `/reports/{id}`. `/health` deliberately stays open, since liveness checks and monitoring tools shouldn't need a credential just to confirm the service is up.
4. **Dockerized API service** — a new `api` service in `docker-compose.yml`, with `EXPOSE 8000` added to the `Dockerfile`, so the FastAPI layer runs in the same container setup as the batch pipeline.
5. **Structured logging** — a shared `src/logging_config.py` (`get_logger(name)`, level via `LOG_LEVEL` env var, logger name baked into the format string) replaced ad hoc `print()`-based status/error output across essentially the whole `src/` tree: `spark_session.py`, `generate_orders.py`, `metrics_reader.py`, `anomaly_detector.py`, `agent_orchestrator.py`, `main.py`, `rag_assistant.py`, `faiss_indexer.py`, `sql_remediation.py`, `dq_metrics_job.py`, `delta_writer.py`, `root_cause_analyzer.py`, `alert_generator.py`, `statistical_detector.py`. Decorative CLI banners (`🤖 AI DATA QUALITY AGENT — STARTING`, `STEP N:` headers) deliberately stayed as `print()` — they're human-facing terminal UX, not operational log records.
6. **Metadata embeddings + documentation generator** — the two Phase 4 roadmap items that got skipped when Phase 5 (API layer) started. A dbt-style `data/schema/orders_schema.yml` documents the `orders` table's columns, gets embedded into a second, separate FAISS index (`src/vector_search/metadata_indexer.py`), and `rag_assistant.py` now searches both the incident-history index and the schema-metadata index depending on the question. A standalone `src/llm_agent/documentation_generator.py` script reads that same schema.yml plus live Delta Lake metrics history and uses GPT-4o-mini to generate `docs/data_dictionary.md` on demand.

## Key Design Decisions

### Why health_score has one implementation instead of two
`health_score` was originally computed independently in `anomaly_detector.py` and `agent_orchestrator.py`, and the two had quietly drifted apart — the orchestrator's version didn't even factor in `info_count`. Rather than patch both to agree once, `calculate_health_score()` became the single source of truth that both `detect_all_anomalies()` (rule-based only) and `combine_anomaly_reports()` (rule-based + statistical) call. This is the same lesson as Sprint 4's anomaly-schema bug, applied one level up: normalize at the one place two code paths need to agree, not at every place that reads the result.

### Why `/health` skips auth but every other endpoint requires it
`/health` exists so a load balancer, container orchestrator, or uptime check can confirm the service is alive without needing a secret. Every other endpoint either runs the real pipeline, spends OpenAI tokens, or reads potentially sensitive report data — those need to be behind `verify_api_key`. Putting auth on the wrong side of that line either breaks liveness checks or leaves real functionality open.

### Why decorative banners stay on `print()` instead of routing through the logger
Logging and human-facing terminal UX solve different problems. A logger call is a structured record meant to be filtered by severity, redirected, or shipped somewhere else — `LOG_LEVEL=WARNING` should not also decide whether the `🤖 AI DATA QUALITY AGENT — STARTING` banner shows up. Keeping the banners on `print()` and the actual status/error/outcome lines on `logger.*` preserves that distinction instead of collapsing it for consistency's sake.

### Why schema metadata got its own FAISS index instead of merging into the incident index
Incident history and schema metadata answer fundamentally different question types — "has this happened before" versus "what does this column mean" — and change on different cadences: incidents accumulate every pipeline run, schema metadata only changes when the table itself changes. Two separate indexes mean rebuilding one never touches the other, and `rag_assistant.py` decides per-question which to search (or both) via independent keyword gates, instead of forcing every query through one undifferentiated index.

## Real Bug: Silent Mojibake from an Unspecified File Encoding

`documentation_generator.py` writes `docs/data_dictionary.md` with a header the script itself authors:

```python
header = (
    f"# Data Dictionary — {schema['models'][0]['name']}\n\n"
    ...
    f"after a schema or pipeline change — do not hand-edit._\n\n"
)
with open(OUTPUT_PATH, "w") as f:
    f.write(result["documentation"])
```

The first generated file came back with the em dashes replaced by `�`:

```
# Data Dictionary � orders
...
Regenerate with `python src/llm_agent/documentation_generator.py` after a schema or pipeline change � do not hand-edit._
```

`open(path, "w")` with no `encoding=` argument doesn't default to UTF-8 — it uses `locale.getpreferredencoding()`, which on Windows is typically cp1252. The em dash (`—`, U+2014) exists in cp1252, so the write didn't raise an error; it just wrote different bytes than UTF-8 would have. Every markdown viewer, GitHub's file preview, and most editors assume UTF-8 by default, so those cp1252 bytes decoded back as `�`. On Linux or macOS, where the default locale encoding is almost always UTF-8 already, this bug would never have surfaced — it's specific to this project running on Windows.

The fix was two-fold: add `encoding="utf-8"` explicitly to both the read (`load_schema()`) and write (`write_documentation()`) file operations, so the behavior no longer depends on the host OS's locale, and swap the em dashes in the hardcoded header for plain hyphens as a second layer of defense against the same class of bug from any other non-ASCII character (a curly quote, an accented name) the LLM might someday write into the body text on its own.

## Interview Q&A

**Q: Why did `open(path, "w")` silently corrupt the file instead of raising an error?**
A: Because the em dash exists in both UTF-8 and cp1252 — just as different byte sequences. Python's default text-mode encoding isn't UTF-8, it's whatever `locale.getpreferredencoding()` returns for the host OS, so the write succeeded and produced a valid file — just not the bytes a UTF-8 reader expects. This is exactly why silent-but-wrong is worse than a crash: nothing failed, so nothing would have caught this without someone actually opening the generated file and reading it.

**Q: How do you guard against this class of bug going forward, versus just fixing this one file?**
A: Two layers. The immediate fix is to never let file encoding depend on implicit OS defaults — pass `encoding="utf-8"` explicitly on every `open()` that reads or writes text, so the behavior is identical on Windows, Linux, and macOS. The second layer is defensive: since this file's content partly comes from an LLM that could write any Unicode character into a future response, sticking to plain ASCII in the parts of the file I author directly (the header) reduces how often the underlying encoding mismatch even has a chance to matter.

**Q: This sprint also had a shared `calculate_health_score()` fix and an import-ordering bug in `anomaly_detector.py`. What's the common thread across all three?**
A: Each one only failed under a specific, easy-to-miss condition rather than every time — a Windows locale default, an orchestrator import that happened to fix `sys.path` before `anomaly_detector.py` needed it, two scoring functions that started identical and only diverged over time. None of these show up in a quick local test run under the exact conditions the developer already trusts. The pattern I keep coming back to this project: verify behavior under the actual conditions it'll run in (a different OS, a direct script invocation, two code paths computing the "same" thing independently), not just the one path you happened to test first.