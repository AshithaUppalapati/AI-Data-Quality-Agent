# Sprint 4 — Statistical Detection, Docker, FAISS/RAG, Enhanced Orchestrator

## What Was Built

Sprint 4 added three capabilities on top of the Sprint 3 LLM agent:

1. **Statistical anomaly detection** — Z-score, IQR, and linear regression trend
   detection that catches unusual values and gradual degradation the rule-based
   thresholds miss.
2. **Docker containerization** — the full pipeline (generator, DQ metrics job,
   LLM agent) runs identically on Windows or Linux with zero manual environment
   setup, eliminating JAVA_HOME/HADOOP_HOME/winutils issues entirely.
3. **FAISS vector search + RAG assistant** — historical DQ reports are embedded
   and indexed, so the agent can answer "has this happened before?" grounded in
   real past incidents instead of generic LLM knowledge.
4. **Enhanced orchestrator** — combines rule-based and statistical detection into
   one unified health report, then runs LLM analysis, SQL remediation, alerts,
   and RAG search against the combined findings.

## Key Design Decisions

### Why combine rule-based and statistical detection instead of picking one
Rule-based catches known threshold violations (documented business rules).
Statistical catches unknown, gradual degradation that no one wrote a rule for.
Neither alone gives full coverage — `combine_anomaly_reports()` merges both
into a single health score and anomaly list, treated identically downstream.

### Why some steps use try/except and others don't
Steps wrapping network/API/file calls (LLM analysis, SQL generation, alerts,
RAG search) get `try`/`except` — external calls can fail unpredictably and one
failure shouldn't take down the whole pipeline. Pure in-memory computation
(the combiner itself) doesn't need it — there's nothing external to fail.

### Why statistical detection failures don't halt the pipeline
Steps 1–2 (metrics, rule detection) `raise` on failure — the pipeline can't
meaningfully continue without them. Step 3 (statistical detection) instead
substitutes an empty fallback result on failure, because Step 4 (the combiner)
needs *some* value to exist even in a degraded state.

### Local development, then Docker
Debugged and iterated locally first for fast feedback (~15s per run vs.
Docker's rebuild-and-run cycle), then verified portability by rebuilding the
image and running the same command in the container. Same output, zero
environment setup — proof the pipeline isn't dependent on this specific
machine's configuration.

## Real Bug: Schema Mismatch Between Detection Sources

The most instructive bug this sprint: `analyze_root_causes()`, `generate_sql_remediation()`,
and `generate_all_alerts()` were originally written in Sprint 3 against the
rule-based anomaly shape only — which included an `info` category and a `details`
key on every anomaly. When statistical anomalies (which use `stats` instead of
`details`, and have no `info` category) got merged in via the combiner, three
separate `KeyError`s surfaced across three different downstream functions
(`'info'`, `'details'`, `'info_count'`).

Root cause: two independently-written anomaly builder functions
(`_anomaly()` in `anomaly_detector.py` and `_stat_anomaly()` in
`statistical_detector.py`) never agreed on a shared schema, but the
orchestrator merges their output into one list and processes it generically.

Fix: normalize at the merge point. `combine_anomaly_reports()` now backfills
a `details` key on any statistical anomaly missing one (falling back to its
`stats` dict), and explicitly carries forward `info` / `info_count` from the
rule-based report so every downstream consumer sees the shape it expects,
regardless of which detector actually produced a given anomaly.

## Interview Q&A

**Q: How do you handle combining data from two different sources with different schemas?**
A: Normalize at the single seam where they merge, not at every downstream
consumer. I found this the hard way — three separate functions broke with
three different `KeyError`s because each expected the exact shape Sprint 3's
rule-based detector produced. Rather than patching every consumer, I fixed
`combine_anomaly_reports()` once, so anything downstream can keep treating
all anomalies identically.

**Q: When do you use try/except vs. let something fail loudly?**
A: Around anything that touches the outside world — API calls, file I/O,
network requests — where failure is expected and recoverable. Pure
computation on data already in memory doesn't need it; if that fails, it's a
real bug that should surface immediately, not get silently swallowed.

**Q: Why develop locally before Docker instead of just working in Docker from the start?**
A: Iteration speed. Debugging this sprint took several rounds of small fixes
(a missing dict key, a missing comma, a schema mismatch) — each one needed
about 15 seconds to test locally. My Docker image bakes `src/` in at build
time, so testing the same fix there would mean a 30–60 second rebuild per
attempt. I containerize once the code is proven stable, not while it's still
being debugged.