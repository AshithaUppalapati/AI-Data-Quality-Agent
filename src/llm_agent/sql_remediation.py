"""
SQL Remediation Generator
=========================
Uses GPT-4o-mini to generate SQL fixes for detected anomalies.

PROMPT STRATEGY (Interview Talking Point):
  Different tasks need different prompt strategies.

  Root cause analysis → conversational, explanatory
    temperature=0.3, longer responses, narrative format

  SQL generation → precise, deterministic, code-focused
    temperature=0.1, structured output, code blocks
    Lower temperature = less creative = more reliable SQL

WHY SQL REMEDIATION MATTERS:
  Traditional DQ tools stop at alerting.
  Engineers still spend hours writing fix queries manually.
  Automated SQL remediation cuts incident response time
  from hours to minutes.

  In production this integrates with:
    - Databricks notebooks (run fix directly)
    - dbt tests (add as data test)
    - Airflow DAGs (trigger remediation job)

TABLE SCHEMA CONTEXT:
  We provide the schema to the LLM so it generates
  syntactically correct SQL for our specific tables.
  Without schema context, LLM guesses column names
  and produces unusable SQL.
"""

import os
import sys
import json

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()


# ─────────────────────────────────────────────────────────────────────────────
# TABLE SCHEMA CONTEXT
# ─────────────────────────────────────────────────────────────────────────────

# Providing schema prevents LLM from hallucinating column names
TABLE_SCHEMA = """
Table: orders
Columns:
  order_id        STRING      NOT NULL  (primary key)
  customer_id     STRING               (foreign key)
  order_date      TIMESTAMP            (when order was placed)
  product_id      STRING               (product reference)
  product_category STRING              (Electronics, Clothing, etc)
  quantity        INTEGER              (units ordered, must be > 0)
  unit_price      DOUBLE               (price per unit, must be > 0)
  total_amount    DOUBLE               (quantity × unit_price)
  order_status    STRING               (pending/confirmed/shipped/
                                        delivered/cancelled/refunded)
  payment_method  STRING               (credit_card/debit_card/
                                        paypal/apple_pay/crypto)
  batch_num       INTEGER   NOT NULL   (processing batch identifier)
  shipping_state  STRING               (US state code, batch 3+)
  billing_state   STRING               (US state code, batch 3+)
  discount_pct    DOUBLE               (0.0-0.30, batch 5+)

Storage: Delta Lake (supports MERGE, UPDATE, DELETE, time travel)
Platform: Apache Spark / Databricks
"""


# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────────────────────

SQL_SYSTEM_PROMPT = """You are a senior data engineer expert in 
Apache Spark SQL and Delta Lake.

Given a list of data quality anomalies and the table schema,
generate specific SQL remediation queries.

For each anomaly provide:
1. DIAGNOSTIC QUERY  — finds affected records
2. REMEDIATION QUERY — fixes the issue
3. VALIDATION QUERY  — confirms the fix worked
4. PREVENTION RULE   — dbt test or constraint to prevent recurrence

Rules for SQL generation:
- Use Spark SQL syntax (compatible with Databricks)
- Use Delta Lake features where appropriate (MERGE, UPDATE)
- Always include WHERE clauses to target only affected records
- Add comments explaining what each query does
- For schema changes, generate ALTER TABLE statements
- For null issues, generate UPDATE or filter queries
- Make queries idempotent (safe to run multiple times)

Format each fix as:
### FIX [N]: [ANOMALY NAME]
**Severity:** CRITICAL/WARNING
**Diagnostic:**
```sql
-- query here
```
**Remediation:**
```sql
-- query here  
```
**Validation:**
```sql
-- query here
```
**Prevention:**
```sql
-- dbt test or constraint here
```
"""


# ─────────────────────────────────────────────────────────────────────────────
# PROMPT BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_remediation_prompt(anomaly_report: dict) -> str:
    """
    Build remediation prompt from anomaly report.

    Only sends CRITICAL and WARNING anomalies —
    INFO anomalies don't need SQL fixes.
    """
    critical = anomaly_report["anomalies"]["critical"]
    warnings = anomaly_report["anomalies"]["warnings"]

    # Combine critical + warnings for remediation
    actionable = critical + warnings

    anomaly_list = "\n".join([
        f"{i+1}. [{a['severity']}] {a['category'].upper()}: "
        f"{a['description']}\n"
        f"   Details: {json.dumps(a['details'], indent=6)}"
        for i, a in enumerate(actionable)
    ])

    prompt = f"""
TABLE SCHEMA:
{TABLE_SCHEMA}

ANOMALIES REQUIRING SQL REMEDIATION:
Total: {len(actionable)} ({len(critical)} critical, 
{len(warnings)} warnings)

{anomaly_list}

Generate specific SQL remediation queries for each anomaly above.
Prioritize CRITICAL issues first.
Use Delta Lake / Spark SQL syntax throughout.
Make all queries idempotent and production-safe.
"""
    return prompt


# ─────────────────────────────────────────────────────────────────────────────
# MAIN GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

def generate_sql_remediation(
    anomaly_report: dict,
    model: str = None
) -> dict:
    """
    Generate SQL remediation queries for detected anomalies.

    Args:
        anomaly_report: Output from detect_all_anomalies()
        model:          Model override.

    Returns:
        dict with:
          - sql_fixes:    full SQL remediation text
          - anomaly_count: how many anomalies addressed
          - model:        which model was used
          - tokens_used:  for cost tracking
    """
    # 💰 COST: ~$0.002-0.003 per call
    client = get_openai_client()
    model  = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    # Count actionable anomalies
    critical     = anomaly_report["anomalies"]["critical"]
    warnings     = anomaly_report["anomalies"]["warnings"]
    total_fixes  = len(critical) + len(warnings)

    if total_fixes == 0:
        return {
            "sql_fixes":     "No actionable anomalies detected. Pipeline healthy.",
            "anomaly_count": 0,
            "model":         model,
            "tokens_used":   0,
            "cost_usd":      0
        }

    prompt = build_remediation_prompt(anomaly_report)

    print(f"\n[SQLRemediation] Generating fixes for "
          f"{total_fixes} anomalies...")
    print(f"[SQLRemediation] Model: {model}")

    response = client.chat.completions.create(
        model       = model,
        messages    = [
            {"role": "system", "content": SQL_SYSTEM_PROMPT},
            {"role": "user",   "content": prompt}
        ],
        temperature = 0.1,    # very low = deterministic SQL
        max_tokens  = 2000,   # SQL can be verbose
    )

    sql_fixes    = response.choices[0].message.content
    tokens_used  = response.usage.total_tokens
    cost_estimate = round(tokens_used * 0.00000015, 6)

    print(f"[SQLRemediation] Generation complete")
    print(f"[SQLRemediation] Tokens: {tokens_used} (~${cost_estimate})")

    return {
        "sql_fixes":     sql_fixes,
        "anomaly_count": total_fixes,
        "model":         model,
        "tokens_used":   tokens_used,
        "cost_usd":      cost_estimate
    }


def get_openai_client() -> OpenAI:
    """Initialize OpenAI client."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError(
            "OPENAI_API_KEY not found. Check your .env file."
        )
    return OpenAI(api_key=api_key)


# ─────────────────────────────────────────────────────────────────────────────
# CLI — test the generator
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from dq_metrics.spark_session import (
        create_spark_session,
        stop_spark_session
    )
    from llm_agent.metrics_reader import build_full_context
    from llm_agent.anomaly_detector import detect_all_anomalies

    spark = create_spark_session(app_name="SQL-Remediation-Test")

    try:
        # Step 1 — Build context
        print("Step 1: Reading metrics from Delta Lake...")
        context = build_full_context(spark)

        # Step 2 — Detect anomalies
        print("\nStep 2: Detecting anomalies...")
        anomaly_report = detect_all_anomalies(context)

        # Step 3 — Generate SQL fixes
        print("\nStep 3: Generating SQL remediation...")
        result = generate_sql_remediation(anomaly_report)

        # Step 4 — Print result
        print("\n" + "="*60)
        print("SQL REMEDIATION QUERIES")
        print("="*60)
        print(f"Model:          {result['model']}")
        print(f"Tokens used:    {result['tokens_used']}")
        print(f"Cost:           ${result['cost_usd']}")
        print(f"Anomalies fixed: {result['anomaly_count']}")
        print("\n" + "-"*60)
        print(result["sql_fixes"])
        print("-"*60)

    finally:
        stop_spark_session(spark)