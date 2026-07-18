# Sprint 5 — API Layer (FastAPI Service, RAG Endpoint, Reports, Tests)

## What Was Built

Sprint 5 exposed the Sprint 3/4 pipeline over HTTP so it can be triggered and
queried without touching Spark or the CLI directly:

1. **FastAPI app skeleton** — `src/api/main.py`, with a `GET /health` liveness
   check and `uvicorn` running it locally with hot reload.
2. **`POST /run-agent`** — runs the full pipeline (metrics → anomaly detection
   → root cause → SQL remediation → alerts → RAG) via HTTP and returns the
   same report `run_dq_agent()` produces from the CLI.
3. **`POST /ask`** — exposes the Sprint 4 RAG assistant as an endpoint, with an
   opt-in flag to ground answers in live pipeline metrics instead of history
   alone.
4. **`GET /reports` and `GET /reports/{id}`** — list all past pipeline runs
   (newest first) and fetch one in full, reading the JSON files
   `run_dq_agent()` already writes to `data/reports/`.
5. **API test suite** — `tests/test_api.py`, using FastAPI's `TestClient` with
   the expensive dependencies (Spark, OpenAI) mocked out at the endpoint
   boundary.

## Key Design Decisions

### Why `/ask` doesn't build live pipeline context by default
`ask_rag_assistant()`'s FAISS retrieval is driven entirely by embedding the
question text — `current_context` (live null rates, drift counts, etc.) is
only used afterward, to append a "here's what's happening right now" block
into the prompt for the LLM to compare against history. Building it means
spinning up a full Spark session to answer one question. Since most questions
("has this happened before?") only need the historical index, `/ask` defaults
`include_current_context` to `False` and lets the caller opt into the slower,
costlier path only when they actually want live grounding.

### Why `/reports` reuses `REPORTS_DIR` from the orchestrator instead of
recomputing the path
`agent_orchestrator.py` already defines `REPORTS_DIR`. Importing it into
`main.py` instead of recomputing `os.path.join(PROJECT_ROOT, "data", "reports")`
a second time means there's exactly one place that knows where reports live —
if that path ever changes, only one file needs updating instead of two
silently drifting apart.

### Why the API tests mock `run_dq_agent` and `ask_rag_assistant` instead of
calling them for real
Both hit real Spark sessions and real OpenAI calls — ~30-60s and real money
per call. The correctness of the pipeline itself is already covered by the
existing unit test suite (Sprint 1-4). What the API layer needs tested is its
own logic: does it route requests correctly, validate input, return the right
status codes, and propagate failures instead of swallowing them. Mocking at
the endpoint boundary tests exactly that, and nothing more, on every run.

## Real Bug: Swallowing Exceptions Instead of Re-Raising Them

While debugging `/run-agent` locally, I added a broad `except Exception`
block around the pipeline call to print the traceback for visibility:

```python
try:
    spark = create_spark_session(app_name="DQ-Agent-API")
    return run_dq_agent(spark, pipeline_name=request.pipeline_name)
except Exception as e:
    print(f">>> ERROR CAUGHT: {type(e).__name__}: {e}")
    traceback.print_exc()
finally:
    stop_spark_session(spark)
```

This looks like reasonable error handling but is actually a regression: FastAPI
sees the exception as handled, so the endpoint returns a `200 OK` with a
`null` body instead of an error. Any caller — a script, a monitoring check, my
own tests — has no way to distinguish success from failure by status code
alone. The traceback prints to the server's own terminal, which is invisible
to anything actually calling the API.

The fix is a single `raise` at the end of the `except` block: log for local
visibility, then let the exception continue propagating so FastAPI converts
it into a proper `500`. To make sure this doesn't regress silently again,
`tests/test_api.py::test_run_agent_propagates_failure` mocks `run_dq_agent` to
raise, then asserts the endpoint responds `500` — a test that fails specifically
if someone reintroduces a bare `except` without `raise`.

## Interview Q&A

**Q: What's the difference between catching an exception for logging and
catching it for error handling?**
A: If your `except` block doesn't end in `raise` (or return an explicit error
response), you haven't handled the error — you've hidden it. In an API, that
means a client-visible `200` for a request that actually failed internally.
I hit this directly: adding a `print`/`traceback.print_exc()` for debugging
silently turned every pipeline failure into a "successful" `null` response
until I added `raise` back.

**Q: How do you decide what an endpoint should always do versus what should
be opt-in?**
A: By what it costs versus what most callers need. `/ask`'s live-context
grounding requires a Spark session just to answer one question — expensive
for something most questions (anything about history) don't need. Making it
an opt-in flag rather than default behavior means the common case stays fast
and cheap, and the caller explicitly pays for the expensive path only when
they want it.

**Q: How do you test an API that wraps expensive, non-deterministic external
calls (LLMs, Spark) without burning time and money on every test run?**
A: Mock at the boundary the endpoint owns, not the internals of what it
calls. My tests replace `run_dq_agent` and `ask_rag_assistant` with fakes and
assert on the endpoint's own behavior — status codes, request validation,
error propagation, whether a Spark session is skipped when it should be. The
pipeline's actual correctness is a different test suite's job; the API layer
only needs to prove it's wired up right.