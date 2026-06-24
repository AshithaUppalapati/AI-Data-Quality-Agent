# Sprint 3 — Rough Notes (LLM Intelligence)

## Task 1+2 — OpenAI API Setup
- OpenAI API, gpt-4o-mini
- Key stored in .env, never committed
- python-dotenv for secure loading
- Tested connectivity: "DQ Agent API connection successful"

## Task 3 — Metrics Reader
- Reads all 5 metric types from Delta Lake
- Computes trend direction per metric
- Detects schema drift events (column diffs)
- Facade pattern: build_full_context() single entry point
- Output: structured dict ready for LLM

## Task 4 — Anomaly Detector
- 3 detection strategies: threshold, trend, delta
- Why combined: each strategy alone misses real issues
- Health score: 100 - (critical×20) - (warning×10) - (info×2)
- Severity: CRITICAL / WARNING / INFO
- Standardized anomaly format for consistent LLM prompting
- Result on our data: 0/100 health score, 9 anomalies
  - 2 critical: schema change + negative prices
  - 6 warnings: null rates, violations trending up
  - 1 info: new column added

## Task 5 — LLM Root Cause Analyzer
- System prompt + user prompt pattern
- System: establishes LLM role and output format
- User: provides anomaly data and metrics context
- temperature=0.3 for consistent, factual responses
- Structured output: Executive Summary, Root Cause,
  Business Impact, Recommended Actions, Verdict
- Cost per run: ~$0.0002
- Key insight: grounding LLM in real data prevents hallucination

## Task 6 — SQL Remediation Generator
- temperature=0.1 for deterministic SQL output
- Schema context provided so LLM uses correct column names
- 4-part fix per anomaly: Diagnostic, Remediation,
  Validation, Prevention
- Spark SQL / Delta Lake syntax throughout
- idempotent queries — safe to run multiple times
- Cost per run: ~$0.0004
- 8 anomalies → 8 complete SQL fix packages

## Task 7 — Alert Generator
- Slack: LLM generated (natural language adds value)
- Jira: template based (structure matters more than language)
- Design rule: use LLM where language adds value,
  template where structure matters
- Slack: emoji-rich, scannable in 10 seconds
- Jira: P1 Critical priority, structured fields
- Cost per run: ~$0.00006 (Slack only, Jira is free)

## Task 8 — Agent Orchestrator
- Facade + pipeline pattern combined
- 5 steps: metrics → anomalies → RCA → SQL → alerts
- Error handling: one step failing doesn't crash pipeline
- Report persisted to data/reports/ as JSON
- Single entry point: run_dq_agent()
- Full pipeline in ~60 seconds for <$0.001

## Task 9 — End-to-End Run
- Full pipeline confirmed: generator → DQ → LLM → report
- Duration: 57.7 seconds end to end
- Total cost: $0.000741 per full run
- Report saved to data/reports/dq_report_[timestamp].json

## Sprint 3 Total API Cost
- Task 5 RCA:       $0.000201
- Task 6 SQL:       $0.000413
- Task 7 Alerts:    $0.000061
- Task 8 Pipeline:  $0.000745
- Task 9 Full run:  $0.000741
- Total Sprint 3:   ~$0.002161

## Known Limitations
- Rule-based detection only catches known violations
- Unknown violations handled partially by LLM reasoning
- Sprint 4 improvement: Z-score statistical detection
- Azure OpenAI: ASU tenant had no quota — used OpenAI API
  Code is identical, one config line to swap back

## Interview Q&A
Q: How do you prevent LLM hallucination?
A: Ground the LLM in real data — pass actual column names,
   batch numbers, and percentages from Delta Lake metrics.
   The LLM can only reference what's in the prompt.

Q: Why temperature=0.1 for SQL but 0.3 for analysis?
A: SQL needs to be deterministic and syntactically correct.
   Lower temperature = less creative = more reliable code.
   Analysis benefits from slightly more natural language
   variation while still being factually grounded.

Q: Why separate SQL remediation from root cause analysis?
A: Different prompt strategies, different output formats,
   different temperature settings. Separation means each
   component is independently testable and replaceable.
   Open/Closed principle — add new output formats without
   touching analysis logic.

Q: What would you improve in production?
A: 1. Switch from OpenAI API to Azure OpenAI for enterprise
      data governance and compliance
   2. Add statistical baseline detection (Z-scores)
   3. Integrate with real Slack webhook and Jira API
   4. Add feedback loop — engineer marks false positives
      to improve detection over time
   5. Cache LLM responses for identical anomaly patterns
      to reduce cost at scale