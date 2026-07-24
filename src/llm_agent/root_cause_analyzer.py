"""
Root Cause Analyzer
===================
Uses GPT-4o-mini to analyze anomaly reports and generate
plain-English root cause explanations.

PROMPT ENGINEERING STRATEGY (Interview Talking Point):
  We use a two-part prompt pattern:

  System prompt: establishes the LLM's role and constraints
  User prompt: provides the actual anomaly context

TEMPERATURE SETTING:
  temperature=0.3 → more deterministic, less creative
  For data analysis, consistency > creativity.
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
from logging_config import get_logger

logger = get_logger(__name__)

load_dotenv()


def get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError(
            "OPENAI_API_KEY not found in environment. "
            "Check your .env file."
        )
    return OpenAI(api_key=api_key)


SYSTEM_PROMPT = """You are a senior data engineering expert specializing 
in data quality monitoring and pipeline reliability.

You will be given a data quality anomaly report from an e-commerce 
order pipeline. Your job is to:

1. Analyze the anomalies and identify root causes
2. Explain issues in plain English that both engineers and 
   business stakeholders can understand
3. Prioritize by business impact
4. Suggest specific, actionable remediation steps

Always structure your response with these exact sections:
## EXECUTIVE SUMMARY
(2-3 sentences for business stakeholders)

## ROOT CAUSE ANALYSIS
(Technical explanation of each critical and warning anomaly)

## BUSINESS IMPACT
(What breaks if these issues are not fixed)

## RECOMMENDED ACTIONS
(Specific steps to fix each issue, ordered by priority)

## PIPELINE HEALTH VERDICT
(One line: CRITICAL / WARNING / HEALTHY with justification)

Be specific. Reference actual column names, batch numbers, 
and percentages from the data provided.
Do not hallucinate — only reference what is in the anomaly report."""


def build_user_prompt(
    anomaly_report: dict,
    context: dict
) -> str:
    null_trend    = context.get("null_rates", {}).get("trend_direction", "unknown")
    drift_events  = context.get("schema_drift", {}).get("drift_events", [])
    total_violations = context.get("rule_violations", {}).get("total_violations", 0)
    avg_dup_rate  = context.get("duplicate_rates", {}).get("avg_dup_rate_pct", 0)
    avg_rows      = context.get("volume_stats", {}).get("avg_rows", 0)

    critical_list = "\n".join([
        f"  - [{a['category'].upper()}] {a['description']}"
        for a in anomaly_report["anomalies"]["critical"]
    ])
    warning_list = "\n".join([
        f"  - [{a['category'].upper()}] {a['description']}"
        for a in anomaly_report["anomalies"]["warnings"]
    ])
    info_list = "\n".join([
        f"  - [{a['category'].upper()}] {a['description']}"
        for a in anomaly_report["anomalies"]["info"]
    ])

    prompt = f"""
PIPELINE DATA QUALITY REPORT
=============================

PIPELINE OVERVIEW:
- Domain: E-commerce order processing
- Batches analyzed: 6 monthly batches
- Average records per batch: {avg_rows}
- Overall health score: {anomaly_report['health_score']}/100

KEY METRICS SUMMARY:
- Null rate trend: {null_trend} (+{context.get('null_rates', {}).get('trend_delta_pct', 0)}% from Batch 1 to Batch 6)
- Schema drift events: {len(drift_events)} detected
- Total business rule violations: {total_violations}
- Average duplicate rate: {avg_dup_rate}%

DETECTED ANOMALIES:

CRITICAL ({anomaly_report['critical_count']}):
{critical_list if critical_list else "  None"}

WARNINGS ({anomaly_report['warning_count']}):
{warning_list if warning_list else "  None"}

INFO ({anomaly_report['info_count']}):
{info_list if info_list else "  None"}

SCHEMA DRIFT DETAILS:
{json.dumps(drift_events, indent=2)}

Please analyze this report and provide root cause analysis,
business impact assessment, and recommended actions.
"""
    return prompt


def analyze_root_causes(
    anomaly_report: dict,
    context: dict,
    model: str = None
) -> dict:
    client = get_openai_client()
    model  = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    user_prompt = build_user_prompt(anomaly_report, context)

    logger.info("Sending to %s...", model)
    logger.info(
        "Anomalies: %d critical, %d warnings",
        anomaly_report['critical_count'], anomaly_report['warning_count']
    )

    response = client.chat.completions.create(
        model       = model,
        messages    = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_prompt}
        ],
        temperature = 0.3,
        max_tokens  = 1500,
    )

    analysis     = response.choices[0].message.content
    tokens_used  = response.usage.total_tokens
    cost_estimate = round(tokens_used * 0.00000015, 6)

    logger.info("Analysis complete")
    logger.info("Tokens used: %d (~$%s)", tokens_used, cost_estimate)

    return {
        "analysis":     analysis,
        "model":        model,
        "tokens_used":  tokens_used,
        "cost_usd":     cost_estimate,
        "health_score": anomaly_report["health_score"],
        "health_status": anomaly_report["health_status"],
        "anomaly_counts": {
            "critical": anomaly_report["critical_count"],
            "warnings": anomaly_report["warning_count"],
            "info":     anomaly_report["info_count"]
        }
    }


if __name__ == "__main__":
    from dq_metrics.spark_session import (
        create_spark_session,
        stop_spark_session
    )
    from llm_agent.metrics_reader import build_full_context
    from llm_agent.anomaly_detector import detect_all_anomalies

    spark = create_spark_session(app_name="Root-Cause-Analyzer-Test")

    try:
        print("Step 1: Reading metrics from Delta Lake...")
        context = build_full_context(spark)

        print("\nStep 2: Detecting anomalies...")
        anomaly_report = detect_all_anomalies(context)

        print("\nStep 3: Running LLM root cause analysis...")
        result = analyze_root_causes(anomaly_report, context)

        print("\n" + "="*60)
        print("LLM ROOT CAUSE ANALYSIS")
        print("="*60)
        print(f"Model:        {result['model']}")
        print(f"Tokens used:  {result['tokens_used']}")
        print(f"Cost:         ${result['cost_usd']}")
        print(f"Health:       {result['health_score']}/100 "
              f"({result['health_status']})")
        print("\n" + "-"*60)
        print(result["analysis"])
        print("-"*60)

    finally:
        stop_spark_session(spark)