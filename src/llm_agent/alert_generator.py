"""
Alert Generator
===============
Generates structured alert messages for Slack and Jira
from anomaly reports.

WHY A SEPARATE MODULE (Interview Talking Point):
  Different consumers need different formats.
  Slack: short, emoji-rich, scannable in 10 seconds
  Jira:  detailed, structured, ticket-ready

LLM VS TEMPLATE (Design Decision):
  We use LLM for Slack message — needs natural language
  We use template for Jira — needs structured fields
  Rule: use LLM where natural language adds value,
        use templates where structure matters more.
"""

import os
import sys
from datetime import datetime

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
        raise ValueError("OPENAI_API_KEY not found.")
    return OpenAI(api_key=api_key)


SLACK_SYSTEM_PROMPT = """You are a data engineering alert system.
Generate a concise Slack alert message for a data quality incident.

Rules:
- Maximum 20 lines
- Use emojis for visual scanning (🚨 ⚠️ ✅ 📊)
- Lead with severity and health score
- List critical issues first, then warnings
- End with a clear action item
- Professional but urgent tone
- No markdown headers — use emojis as visual anchors
- Include timestamp
"""


def generate_slack_alert(
    anomaly_report: dict,
    pipeline_name: str = "E-commerce Orders Pipeline"
) -> dict:
    client = get_openai_client()
    model  = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    health_score  = anomaly_report["health_score"]
    health_status = anomaly_report["health_status"]
    critical      = anomaly_report["anomalies"]["critical"]
    warnings      = anomaly_report["anomalies"]["warnings"]

    critical_list = "\n".join([
        f"- {a['description']}" for a in critical
    ])
    warning_list = "\n".join([
        f"- {a['description']}" for a in warnings[:3]
    ])

    prompt = f"""
Generate a Slack alert for this data quality incident:

Pipeline: {pipeline_name}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}
Health Score: {health_score}/100
Status: {health_status}

Critical Issues ({len(critical)}):
{critical_list if critical_list else "None"}

Top Warnings ({len(warnings)}):
{warning_list if warning_list else "None"}

Action needed: Engineering team must investigate immediately.
"""

    response = client.chat.completions.create(
        model       = model,
        messages    = [
            {"role": "system", "content": SLACK_SYSTEM_PROMPT},
            {"role": "user",   "content": prompt}
        ],
        temperature = 0.4,
        max_tokens  = 400,
    )

    message     = response.choices[0].message.content
    tokens_used = response.usage.total_tokens
    cost        = round(tokens_used * 0.00000015, 6)

    logger.info("Slack alert generated (%d tokens, ~$%s)", tokens_used, cost)

    return {
        "channel":     "slack",
        "message":     message,
        "tokens_used": tokens_used,
        "cost_usd":    cost
    }


def generate_jira_ticket(
    anomaly_report: dict,
    pipeline_name:  str = "E-commerce Orders Pipeline"
) -> dict:
    health_score  = anomaly_report["health_score"]
    health_status = anomaly_report["health_status"]
    critical      = anomaly_report["anomalies"]["critical"]
    warnings      = anomaly_report["anomalies"]["warnings"]
    info          = anomaly_report["anomalies"]["info"]

    priority_map = {
        "CRITICAL": "P1 - Critical",
        "WARNING":  "P2 - High",
        "HEALTHY":  "P3 - Medium"
    }

    critical_section = "\n".join([
        f"* [CRITICAL] {a['description']}"
        for a in critical
    ])
    warning_section = "\n".join([
        f"* [WARNING] {a['description']}"
        for a in warnings
    ])
    info_section = "\n".join([
        f"* [INFO] {a['description']}"
        for a in info
    ])

    description = f"""
h2. Data Quality Incident Report

*Pipeline:* {pipeline_name}
*Detected:* {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}
*Health Score:* {health_score}/100
*Status:* {health_status}

h2. Summary
Total anomalies detected: {anomaly_report['total_anomalies']}
* Critical: {anomaly_report['critical_count']}
* Warnings: {anomaly_report['warning_count']}
* Info: {anomaly_report['info_count']}

h2. Critical Issues
{critical_section if critical_section else "None"}

h2. Warnings
{warning_section if warning_section else "None"}

h2. Info
{info_section if info_section else "None"}

h2. Next Steps
# Review critical issues immediately
# Run SQL diagnostic queries from remediation report
# Investigate upstream data sources
# Apply fixes and validate
# Update this ticket with findings

h2. Automated Analysis
This ticket was generated automatically by the
AI Data Quality Agent. Full analysis available
in the pipeline monitoring dashboard.
"""

    ticket = {
        "channel":     "jira",
        "project":     "DATA",
        "issue_type":  "Bug",
        "priority":    priority_map.get(health_status, "P2 - High"),
        "summary":     f"[DQ ALERT] {pipeline_name} — "
                       f"Health Score {health_score}/100 — "
                       f"{health_status}",
        "description": description,
        "labels":      ["data-quality", "automated", "pipeline"],
        "components":  ["Data Engineering"],
        "assignee":    "data-engineering-team",
        "tokens_used": 0,
        "cost_usd":    0.0
    }

    logger.info("Jira ticket generated (template, $0.00)")
    return ticket


def generate_all_alerts(
    anomaly_report: dict,
    pipeline_name:  str = "E-commerce Orders Pipeline"
) -> dict:
    logger.info("Generating alerts...")

    slack = generate_slack_alert(anomaly_report, pipeline_name)
    jira  = generate_jira_ticket(anomaly_report, pipeline_name)

    total_cost = slack["cost_usd"] + jira["cost_usd"]

    logger.info("All alerts generated | Total cost: $%s", total_cost)

    return {
        "slack":      slack,
        "jira":       jira,
        "total_cost": total_cost
    }


if __name__ == "__main__":
    from dq_metrics.spark_session import (
        create_spark_session,
        stop_spark_session
    )
    from llm_agent.metrics_reader import build_full_context
    from llm_agent.anomaly_detector import detect_all_anomalies

    spark = create_spark_session(app_name="Alert-Generator-Test")

    try:
        print("Step 1: Reading metrics from Delta Lake...")
        context = build_full_context(spark)

        print("\nStep 2: Detecting anomalies...")
        anomaly_report = detect_all_anomalies(context)

        print("\nStep 3: Generating alerts...")
        alerts = generate_all_alerts(anomaly_report)

        print("\n" + "="*60)
        print("SLACK ALERT")
        print("="*60)
        print(alerts["slack"]["message"])

        print("\n" + "="*60)
        print("JIRA TICKET")
        print("="*60)
        print(f"Summary:  {alerts['jira']['summary']}")
        print(f"Priority: {alerts['jira']['priority']}")
        print(f"Labels:   {alerts['jira']['labels']}")
        print(f"\nDescription:")
        print(alerts["jira"]["description"])

        print(f"\nTotal alert cost: ${alerts['total_cost']}")

    finally:
        stop_spark_session(spark)