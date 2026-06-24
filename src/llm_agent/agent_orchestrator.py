"""
DQ Agent Orchestrator
=====================
Ties all LLM agent components into a single pipeline.

ORCHESTRATOR PATTERN (Interview Talking Point):
  Each component (metrics reader, anomaly detector,
  root cause analyzer, SQL remediation, alert generator)
  works independently and is separately testable.

  The orchestrator:
    1. Calls each component in sequence
    2. Passes output of each step as input to next
    3. Handles errors gracefully (one step failing
       doesn't kill the entire pipeline)
    4. Persists full report for audit trail
    5. Returns structured result to caller

  This is the facade pattern + pipeline pattern combined.

ERROR HANDLING STRATEGY:
  Each step wrapped in try/except.
  If LLM step fails (API outage, rate limit):
    → log the error
    → continue with remaining steps
    → mark failed step in report
  Pipeline never crashes completely.

REPORT PERSISTENCE:
  Full report saved to data/reports/ as JSON.
  Why: audit trail, debugging, future RAG indexing.
  Phase 4 will index these reports in FAISS for
  similarity search — "find similar past incidents."
"""

import os
import sys
import json
from datetime import datetime

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from pyspark.sql import SparkSession
from dq_metrics.spark_session import create_spark_session, stop_spark_session
from llm_agent.metrics_reader import build_full_context
from llm_agent.anomaly_detector import detect_all_anomalies
from llm_agent.root_cause_analyzer import analyze_root_causes
from llm_agent.sql_remediation import generate_sql_remediation
from llm_agent.alert_generator import generate_all_alerts

# Reports output directory
REPORTS_DIR = os.path.join(PROJECT_ROOT, "data", "reports")


# ─────────────────────────────────────────────────────────────────────────────
# REPORT PERSISTENCE
# ─────────────────────────────────────────────────────────────────────────────

def save_report(report: dict) -> str:
    """
    Save full agent report to disk as JSON.

    WHY PERSIST REPORTS:
      1. Audit trail — who was notified, when, what was found
      2. Debugging — replay any past analysis
      3. Future RAG — Phase 4 indexes these for similarity search
         "find incidents similar to today's schema drift"

    Returns path where report was saved.
    """
    os.makedirs(REPORTS_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename  = f"dq_report_{timestamp}.json"
    path      = os.path.join(REPORTS_DIR, filename)

    # Make report JSON serializable
    serializable = {
        k: v for k, v in report.items()
        if k != "spark"
    }

    with open(path, "w") as f:
        json.dump(serializable, f, indent=2, default=str)

    print(f"[Orchestrator] Report saved → {path}")
    return path


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def run_dq_agent(
    spark:         SparkSession = None,
    pipeline_name: str          = "E-commerce Orders Pipeline",
    save_to_disk:  bool         = True
) -> dict:
    """
    Run the complete DQ agent pipeline.

    Args:
        spark:         SparkSession. If None, creates one internally.
        pipeline_name: Name for alerts and reports.
        save_to_disk:  Whether to persist report as JSON.

    Returns:
        Complete report dict with all step outputs.

    💰 COST: ~$0.001-0.005 per full run
    """
    run_start    = datetime.now()
    owns_spark   = spark is None
    total_cost   = 0.0

    print("\n" + "🤖 " + "="*56)
    print("  AI DATA QUALITY AGENT — STARTING")
    print("="*58)
    print(f"  Pipeline: {pipeline_name}")
    print(f"  Started:  {run_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*58 + "\n")

    # Initialize report structure
    report = {
        "pipeline_name": pipeline_name,
        "run_timestamp": run_start.isoformat(),
        "status":        "running",
        "steps":         {},
        "total_cost_usd": 0.0,
        "report_path":   None
    }

    try:
        # ── Create Spark session if not provided ──────────────────────────
        if owns_spark:
            print("⚙️  Initializing Spark session...")
            spark = create_spark_session(app_name="DQ-Agent")

        # ── STEP 1: Read metrics from Delta Lake ──────────────────────────
        print("\n📊 STEP 1: Reading metrics from Delta Lake...")
        try:
            context = build_full_context(spark)
            report["steps"]["metrics_reader"] = {
                "status":           "success",
                "null_trend":       context["null_rates"]["trend_direction"],
                "drift_events":     context["schema_drift"]["drift_count"],
                "total_violations": context["rule_violations"]["total_violations"],
                "avg_dup_rate":     context["duplicate_rates"]["avg_dup_rate_pct"]
            }
            print("   ✅ Metrics loaded successfully")
        except Exception as e:
            report["steps"]["metrics_reader"] = {
                "status": "failed",
                "error":  str(e)
            }
            print(f"   ❌ Metrics reader failed: {e}")
            raise  # Can't continue without metrics

        # ── STEP 2: Detect anomalies ──────────────────────────────────────
        print("\n🔍 STEP 2: Detecting anomalies...")
        try:
            anomaly_report = detect_all_anomalies(context)
            report["steps"]["anomaly_detector"] = {
                "status":        "success",
                "health_score":  anomaly_report["health_score"],
                "health_status": anomaly_report["health_status"],
                "critical":      anomaly_report["critical_count"],
                "warnings":      anomaly_report["warning_count"],
                "info":          anomaly_report["info_count"],
                "total":         anomaly_report["total_anomalies"]
            }
            print(f"   ✅ {anomaly_report['total_anomalies']} anomalies detected "
                  f"| Health: {anomaly_report['health_score']}/100 "
                  f"({anomaly_report['health_status']})")
        except Exception as e:
            report["steps"]["anomaly_detector"] = {
                "status": "failed",
                "error":  str(e)
            }
            print(f"   ❌ Anomaly detector failed: {e}")
            raise

        # ── STEP 3: LLM root cause analysis ──────────────────────────────
        print("\n🧠 STEP 3: Running LLM root cause analysis...")
        try:
            rca_result = analyze_root_causes(anomaly_report, context)
            total_cost += rca_result["cost_usd"]
            report["steps"]["root_cause_analyzer"] = {
                "status":      "success",
                "model":       rca_result["model"],
                "tokens_used": rca_result["tokens_used"],
                "cost_usd":    rca_result["cost_usd"],
                "analysis":    rca_result["analysis"]
            }
            print(f"   ✅ Analysis complete "
                  f"({rca_result['tokens_used']} tokens, "
                  f"${rca_result['cost_usd']})")
        except Exception as e:
            report["steps"]["root_cause_analyzer"] = {
                "status": "failed",
                "error":  str(e)
            }
            print(f"   ⚠️  Root cause analysis failed: {e}")
            print(f"   Continuing with remaining steps...")

        # ── STEP 4: SQL remediation ───────────────────────────────────────
        print("\n🔧 STEP 4: Generating SQL remediation...")
        try:
            sql_result = generate_sql_remediation(anomaly_report)
            total_cost += sql_result["cost_usd"]
            report["steps"]["sql_remediation"] = {
                "status":        "success",
                "anomaly_count": sql_result["anomaly_count"],
                "tokens_used":   sql_result["tokens_used"],
                "cost_usd":      sql_result["cost_usd"],
                "sql_fixes":     sql_result["sql_fixes"]
            }
            print(f"   ✅ SQL fixes generated "
                  f"({sql_result['anomaly_count']} fixes, "
                  f"${sql_result['cost_usd']})")
        except Exception as e:
            report["steps"]["sql_remediation"] = {
                "status": "failed",
                "error":  str(e)
            }
            print(f"   ⚠️  SQL remediation failed: {e}")
            print(f"   Continuing with remaining steps...")

        # ── STEP 5: Generate alerts ───────────────────────────────────────
        print("\n🔔 STEP 5: Generating alerts...")
        try:
            alerts = generate_all_alerts(anomaly_report, pipeline_name)
            total_cost += alerts["total_cost"]
            report["steps"]["alert_generator"] = {
                "status":       "success",
                "slack_message": alerts["slack"]["message"],
                "jira_summary":  alerts["jira"]["summary"],
                "jira_priority": alerts["jira"]["priority"],
                "cost_usd":      alerts["total_cost"]
            }
            print(f"   ✅ Slack + Jira alerts generated "
                  f"(${alerts['total_cost']})")
        except Exception as e:
            report["steps"]["alert_generator"] = {
                "status": "failed",
                "error":  str(e)
            }
            print(f"   ⚠️  Alert generation failed: {e}")

        # ── Finalize report ───────────────────────────────────────────────
        run_end  = datetime.now()
        duration = (run_end - run_start).total_seconds()

        report["status"]         = "completed"
        report["total_cost_usd"] = round(total_cost, 6)
        report["duration_secs"]  = round(duration, 1)
        report["health_score"]   = anomaly_report["health_score"]
        report["health_status"]  = anomaly_report["health_status"]

        # ── Save report ───────────────────────────────────────────────────
        if save_to_disk:
            report_path        = save_report(report)
            report["report_path"] = report_path

    except Exception as e:
        report["status"] = "failed"
        report["error"]  = str(e)
        print(f"\n❌ Agent pipeline failed: {e}")

    finally:
        if owns_spark and spark:
            stop_spark_session(spark)

    # ── Print summary ─────────────────────────────────────────────────────
    print("\n" + "="*58)
    print("  AI DATA QUALITY AGENT — COMPLETE")
    print("="*58)
    print(f"  Status:       {report['status'].upper()}")
    print(f"  Health:       {report.get('health_score', 'N/A')}/100 "
          f"({report.get('health_status', 'N/A')})")
    print(f"  Duration:     {report.get('duration_secs', 'N/A')}s")
    print(f"  Total cost:   ${report.get('total_cost_usd', 0)}")
    print(f"  Report saved: {report.get('report_path', 'N/A')}")
    print("="*58 + "\n")

    return report


# ─────────────────────────────────────────────────────────────────────────────
# CLI — run the full agent
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    report = run_dq_agent(
        pipeline_name = "E-commerce Orders Pipeline",
        save_to_disk  = True
    )

    # Print final Slack alert
    slack = report.get("steps", {}) \
                  .get("alert_generator", {}) \
                  .get("slack_message", "")
    if slack:
        print("\n📱 SLACK ALERT PREVIEW:")
        print("-"*40)
        print(slack)
        print("-"*40)