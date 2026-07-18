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
from llm_agent.anomaly_detector import detect_all_anomalies, calculate_health_score
from llm_agent.root_cause_analyzer import analyze_root_causes
from llm_agent.sql_remediation import generate_sql_remediation
from llm_agent.alert_generator import generate_all_alerts
from llm_agent.statistical_detector import detect_statistical_anomalies
from vector_search.rag_assistant import ask_rag_assistant

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

def combine_anomaly_reports(rule_report: dict, stat_report: dict) -> dict:
    rule_critical = rule_report["anomalies"]["critical"]
    rule_warnings = rule_report["anomalies"]["warnings"]
    rule_info = rule_report["anomalies"]["info"]
    stat_critical = stat_report["anomalies"]["critical"]
    stat_warnings = stat_report["anomalies"]["warnings"]
    for a in stat_critical + stat_warnings:
        if "details" not in a:
            a["details"] = a.get("stats", {})
    all_critical = rule_critical + stat_critical
    all_warnings = rule_warnings + stat_warnings
    # Shared formula — see calculate_health_score() in anomaly_detector.py.
    # Previously reimplemented here independently, and had drifted from
    # detect_all_anomalies() by silently ignoring info_count.
    scoring = calculate_health_score(
        critical_count = len(all_critical),
        warning_count  = len(all_warnings),
        info_count     = len(rule_info)
    )

    return {
        "total_anomalies": len(all_critical) + len(all_warnings),
        "critical_count": len(all_critical),
        "warning_count": len(all_warnings),
        "health_score": scoring["health_score"],
        "anomalies": {
            "critical": all_critical,
            "warnings": all_warnings,
            "info": rule_info
        },
        "health_status": scoring["health_status"],
        "info_count": len(rule_info)
    }
# ─────────────────────────────────────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def run_dq_agent(
    spark:         SparkSession = None,
    pipeline_name: str          = "E-commerce Orders Pipeline",
    save_to_disk:  bool         = True,
    recent_batches: int         = None
) -> dict:
    """
    Runs the complete DQ agent pipeline.

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
            context = build_full_context(spark, recent_batches=recent_batches)
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

        # ── STEP 3: New Statistical detection, non-fatal on failure ──────────────────────────────
        print("\n🧠 STEP 3: Statistical anomaly detection...")
        try:
            stat_report = detect_statistical_anomalies(context)
            report["steps"]["stat_detector"] = {
                "status": "success", "total": stat_report["total_anomalies"]
            }
            print(f"   ✅ {stat_report['total_anomalies']} statistical anomalies")
        except Exception as e:
            report["steps"]["stat_detector"] = {"status": "failed", "error": str(e)}
            print(f"   ⚠️  Statistical detection failed: {e}")
            stat_report = {"total_anomalies": 0, "critical_count": 0, "warning_count": 0,
                            "anomalies": {"critical": [], "warnings": []}}


        # STEP 4 — NEW: combine, no try/except (pure dict logic, nothing external to fail)
        print("\n🔗 STEP 4: Combining detection results...")
        combined_report = combine_anomaly_reports(anomaly_report, stat_report)
        report["steps"]["combined_detection"] = {
            "status": "success", "health_score": combined_report["health_score"]
        }
        print(f"   ✅ Combined health: {combined_report['health_score']}/100 "
              f"({combined_report['health_status']})")

        # STEP 5 — was STEP 3, now uses combined_report
        print("\n🧠 STEP 5: Running LLM root cause analysis...")
        try:
            rca_result = analyze_root_causes(combined_report, context)
            total_cost += rca_result["cost_usd"]
            report["steps"]["root_cause_analyzer"] = {
                "status": "success", "analysis": rca_result["analysis"],
                "cost_usd": rca_result["cost_usd"]
            }
            print(f"   ✅ Analysis complete (${rca_result['cost_usd']})")
        except Exception as e:
            report["steps"]["root_cause_analyzer"] = {"status": "failed", "error": str(e)}
            print(f"   ⚠️  Root cause analysis failed: {e}")

        # STEP 6 — was STEP 4, now uses combined_report
        print("\n🔧 STEP 6: Generating SQL remediation...")
        try:
            sql_result = generate_sql_remediation(combined_report)
            total_cost += sql_result["cost_usd"]
            report["steps"]["sql_remediation"] = {
                "status": "success", "sql_fixes": sql_result["sql_fixes"],
                "cost_usd": sql_result["cost_usd"]
            }
            print(f"   ✅ SQL fixes generated (${sql_result['cost_usd']})")
        except Exception as e:
            report["steps"]["sql_remediation"] = {"status": "failed", "error": str(e)}
            print(f"   ⚠️  SQL remediation failed: {e}")

        # STEP 7 — was STEP 5, now uses combined_report
        print("\n🔔 STEP 7: Generating alerts...")
        try:
            alerts = generate_all_alerts(combined_report, pipeline_name)
            total_cost += alerts["total_cost"]
            report["steps"]["alert_generator"] = {
                "status": "success", "slack_message": alerts["slack"]["message"],
                "cost_usd": alerts["total_cost"]
            }
            print(f"   ✅ Alerts generated (${alerts['total_cost']})")
        except Exception as e:
            report["steps"]["alert_generator"] = {"status": "failed", "error": str(e)}
            print(f"   ⚠️  Alert generation failed: {e}")

        # STEP 8 — NEW: RAG similarity search
        print("\n🔍 STEP 8: Searching for similar past incidents...")
        try:
            rag_result = ask_rag_assistant(
                question=f"Pipeline health {combined_report['health_score']}/100 "
                         f"with {combined_report['critical_count']} critical anomalies",
                current_context=context,
                force_retrieval=True
            )
            total_cost += rag_result["cost_usd"]
            report["steps"]["rag_assistant"] = {
                "status": "success", "answer": rag_result["answer"],
                "cost_usd": rag_result["cost_usd"]
            }
            print(f"   ✅ RAG search complete (${rag_result['cost_usd']})")
        except Exception as e:
            report["steps"]["rag_assistant"] = {"status": "failed", "error": str(e)}
            print(f"   ⚠️  RAG search failed: {e}")

        # Finalize — now reads from combined_report
        run_end  = datetime.now()
        report["status"]         = "completed"
        report["total_cost_usd"] = round(total_cost, 6)
        report["duration_secs"]  = round((run_end - run_start).total_seconds(), 1)
        report["health_score"]   = combined_report["health_score"]
        report["health_status"]  = combined_report["health_status"]

        if save_to_disk:
            report["report_path"] = save_report(report)

    except Exception as e:
        report["status"] = "failed"
        report["error"]  = str(e)
        print(f"\n❌ Agent pipeline failed: {e}")

    finally:
        if owns_spark and spark:
            stop_spark_session(spark)

    print(f"\n✅ Status: {report['status'].upper()} | "
          f"Health: {report.get('health_score','N/A')}/100 | "
          f"Cost: ${report.get('total_cost_usd', 0)}\n")

    return report


if __name__ == "__main__":
    report = run_dq_agent(
        pipeline_name="E-commerce Orders Pipeline",
        save_to_disk=True
    )
    slack = report.get("steps", {}).get("alert_generator", {}).get("slack_message", "")
    if slack:
        print("\n📱 SLACK ALERT PREVIEW:")
        print("-"*40)
        print(slack)
        print("-"*40)