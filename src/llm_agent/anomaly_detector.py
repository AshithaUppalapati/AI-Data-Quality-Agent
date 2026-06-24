"""
Anomaly Detector
================
Analyzes DQ metrics context and flags anomalies
using threshold, trend, and delta-based detection.

WHY THREE DETECTION STRATEGIES (Interview Talking Point):
  Single-strategy detection misses real issues.

  Threshold alone: misses gradual degradation
    (5% null rate every batch looks fine individually
     but trending from 3% → 5% over 6 months is a problem)

  Trend alone: too many false positives
    (any increase triggers alert even if tiny)

  Delta alone: misses sustained high values
    (10% null rate that never changes = no alert)

  Combined: catches what each strategy misses.
  This is how production monitoring systems work.

SEVERITY LEVELS:
  CRITICAL  → immediate action required
  WARNING   → investigate soon
  INFO      → awareness only
"""

import os
import sys

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))


# ─────────────────────────────────────────────────────────────────────────────
# THRESHOLDS — tune these for your domain
# ─────────────────────────────────────────────────────────────────────────────

THRESHOLDS = {
    "null_rate_warning_pct":    5.0,   # warn if any column > 5% nulls
    "null_rate_critical_pct":   10.0,  # critical if any column > 10% nulls
    "null_trend_delta_pct":     1.5,   # flag if null rate grew > 1.5% overall
    "dup_rate_warning_pct":     2.0,   # warn if duplicate rate > 2%
    "dup_rate_critical_pct":    5.0,   # critical if duplicate rate > 5%
    "violation_warning_pct":    10.0,  # warn if violations > 10% of rows
    "violation_increase_pct":   20.0,  # flag if violations grew > 20%
    "volume_drop_pct":          20.0,  # flag if row count drops > 20%
    "volume_spike_pct":         50.0,  # flag if row count spikes > 50%
}


# ─────────────────────────────────────────────────────────────────────────────
# ANOMALY BUILDER HELPER
# ─────────────────────────────────────────────────────────────────────────────

def _anomaly(severity: str, category: str,
             description: str, details: dict) -> dict:
    """
    Build a standardized anomaly record.

    Standardized format means the LLM always receives
    anomalies in the same structure — easier to prompt
    and more consistent responses.
    """
    return {
        "severity":    severity,    # CRITICAL / WARNING / INFO
        "category":    category,    # null_rates / schema_drift / etc
        "description": description, # human-readable summary
        "details":     details      # supporting data for LLM context
    }


# ─────────────────────────────────────────────────────────────────────────────
# 1. NULL RATE ANOMALIES
# ─────────────────────────────────────────────────────────────────────────────

def detect_null_rate_anomalies(null_rates: dict) -> list:
    """
    Detect anomalies in null rate metrics.

    Checks:
      1. Any column exceeds warning/critical threshold
      2. Overall null rate trend is degrading significantly
      3. Specific high-risk columns (customer_id, order_status)
    """
    anomalies = []

    # Check worst columns against thresholds
    for col in null_rates.get("worst_columns", []):
        rate = col["avg_null_rate_pct"]
        name = col["column"]

        if rate >= THRESHOLDS["null_rate_critical_pct"]:
            anomalies.append(_anomaly(
                severity    = "CRITICAL",
                category    = "null_rates",
                description = f"Column '{name}' has critically high "
                              f"null rate: {rate}%",
                details     = {
                    "column":        name,
                    "avg_null_rate": rate,
                    "threshold":     THRESHOLDS["null_rate_critical_pct"]
                }
            ))
        elif rate >= THRESHOLDS["null_rate_warning_pct"]:
            anomalies.append(_anomaly(
                severity    = "WARNING",
                category    = "null_rates",
                description = f"Column '{name}' null rate {rate}% "
                              f"exceeds warning threshold",
                details     = {
                    "column":        name,
                    "avg_null_rate": rate,
                    "threshold":     THRESHOLDS["null_rate_warning_pct"]
                }
            ))

    # Check trend degradation
    trend_delta = null_rates.get("trend_delta_pct", 0)
    if null_rates.get("trend_direction") == "degrading" and \
       trend_delta >= THRESHOLDS["null_trend_delta_pct"]:
        anomalies.append(_anomaly(
            severity    = "WARNING",
            category    = "null_rates",
            description = f"Null rates degrading across batches: "
                          f"+{trend_delta}% increase from Batch 1 to last batch",
            details     = {
                "trend_delta_pct": trend_delta,
                "batch_trend":     null_rates.get("batch_trend", []),
                "threshold":       THRESHOLDS["null_trend_delta_pct"]
            }
        ))

    # Check high-risk columns specifically
    high_risk_cols = {"customer_id", "order_id", "order_status"}
    for col in null_rates.get("worst_columns", []):
        if col["column"] in high_risk_cols and \
           col["avg_null_rate_pct"] > 0:
            anomalies.append(_anomaly(
                severity    = "WARNING",
                category    = "null_rates",
                description = f"High-risk column '{col['column']}' "
                              f"has non-zero null rate: "
                              f"{col['avg_null_rate_pct']}%",
                details     = {
                    "column":   col["column"],
                    "null_rate": col["avg_null_rate_pct"],
                    "risk":     "Primary key or critical business field"
                }
            ))

    return anomalies


# ─────────────────────────────────────────────────────────────────────────────
# 2. SCHEMA DRIFT ANOMALIES
# ─────────────────────────────────────────────────────────────────────────────

def detect_schema_drift_anomalies(schema_drift: dict) -> list:
    """
    Detect schema drift events.

    Any schema change is at minimum INFO level.
    Column removals are always WARNING or higher —
    removing a column breaks downstream consumers.
    """
    anomalies = []

    if not schema_drift.get("drift_detected"):
        return anomalies

    for event in schema_drift.get("drift_events", []):
        cols_removed = event.get("cols_removed", [])
        cols_added   = event.get("cols_added", [])

        # Column removal is more dangerous than addition
        if cols_removed:
            anomalies.append(_anomaly(
                severity    = "CRITICAL",
                category    = "schema_drift",
                description = f"Schema breaking change: columns removed "
                              f"between Batch {event['from_batch']} "
                              f"and Batch {event['to_batch']}: "
                              f"{cols_removed}",
                details     = {
                    "from_batch":   event["from_batch"],
                    "to_batch":     event["to_batch"],
                    "cols_removed": cols_removed,
                    "cols_added":   cols_added,
                    "impact":       "Downstream JOINs and aggregations "
                                   "referencing removed columns will fail"
                }
            ))
        elif cols_added:
            anomalies.append(_anomaly(
                severity    = "INFO",
                category    = "schema_drift",
                description = f"New columns added between Batch "
                              f"{event['from_batch']} and "
                              f"Batch {event['to_batch']}: {cols_added}",
                details     = {
                    "from_batch": event["from_batch"],
                    "to_batch":   event["to_batch"],
                    "cols_added": cols_added,
                    "impact":     "Downstream consumers should be updated "
                                 "to handle new columns"
                }
            ))

    return anomalies


# ─────────────────────────────────────────────────────────────────────────────
# 3. DUPLICATE RATE ANOMALIES
# ─────────────────────────────────────────────────────────────────────────────

def detect_duplicate_anomalies(duplicate_rates: dict) -> list:
    """
    Detect duplicate rate anomalies.

    Checks avg rate and worst batch against thresholds.
    """
    anomalies = []
    avg_rate  = duplicate_rates.get("avg_dup_rate_pct", 0)
    worst     = duplicate_rates.get("worst_batch", {})

    if avg_rate >= THRESHOLDS["dup_rate_critical_pct"]:
        anomalies.append(_anomaly(
            severity    = "CRITICAL",
            category    = "duplicate_rates",
            description = f"Critical duplicate rate: {avg_rate}% average "
                          f"across all batches",
            details     = {
                "avg_rate":  avg_rate,
                "threshold": THRESHOLDS["dup_rate_critical_pct"],
                "impact":    "Severe data integrity issue — "
                             "revenue and reporting will be doubled"
            }
        ))
    elif avg_rate >= THRESHOLDS["dup_rate_warning_pct"]:
        anomalies.append(_anomaly(
            severity    = "WARNING",
            category    = "duplicate_rates",
            description = f"Elevated duplicate rate: {avg_rate}% average. "
                          f"Worst batch: {worst.get('batch_num')} "
                          f"at {worst.get('dup_rate_pct')}%",
            details     = {
                "avg_rate":    avg_rate,
                "worst_batch": worst,
                "threshold":   THRESHOLDS["dup_rate_warning_pct"],
                "likely_cause": "Upstream ETL re-runs or retry storms"
            }
        ))

    return anomalies


# ─────────────────────────────────────────────────────────────────────────────
# 4. RULE VIOLATION ANOMALIES
# ─────────────────────────────────────────────────────────────────────────────

def detect_violation_anomalies(rule_violations: dict) -> list:
    """
    Detect business rule violation anomalies.

    Checks:
      1. Total violation count vs row count
      2. Trend direction
      3. Specific violation types that are most dangerous
    """
    anomalies = []
    batch_data = rule_violations.get("batch_data", [])

    if not batch_data:
        return anomalies

    # Check violation trend
    if rule_violations.get("trend_direction") == "degrading":
        first = batch_data[0]["total_violations"]
        last  = batch_data[-1]["total_violations"]
        pct_increase = round(((last - first) / first) * 100, 1) \
                       if first > 0 else 0

        if pct_increase >= THRESHOLDS["violation_increase_pct"]:
            anomalies.append(_anomaly(
                severity    = "WARNING",
                category    = "rule_violations",
                description = f"Business rule violations increasing: "
                              f"+{pct_increase}% from Batch 1 to last batch "
                              f"({first} → {last} violations)",
                details     = {
                    "first_batch_violations": first,
                    "last_batch_violations":  last,
                    "pct_increase":           pct_increase,
                    "breakdown": rule_violations.get("violation_breakdown", {})
                }
            ))

    # Flag negative prices specifically — financial impact
    breakdown = rule_violations.get("violation_breakdown", {})
    neg_prices = breakdown.get("negative_price", 0)
    if neg_prices > 0:
        anomalies.append(_anomaly(
            severity    = "CRITICAL",
            category    = "rule_violations",
            description = f"{neg_prices} records with negative unit prices "
                          f"detected across all batches",
            details     = {
                "negative_price_count": neg_prices,
                "financial_impact":     "Revenue calculations and financial "
                                       "reports will be incorrect",
                "recommended_action":   "Add upstream validation: "
                                       "unit_price > 0 constraint"
            }
        ))

    # Flag future dates — data integrity issue
    future_dates = breakdown.get("future_dates", 0)
    if future_dates > 0:
        anomalies.append(_anomaly(
            severity    = "WARNING",
            category    = "rule_violations",
            description = f"{future_dates} records with future order dates",
            details     = {
                "future_date_count": future_dates,
                "impact":            "Time-series analytics and "
                                    "cohort analysis will be skewed",
                "recommended_action": "Add constraint: "
                                     "order_date <= current_timestamp()"
            }
        ))

    return anomalies


# ─────────────────────────────────────────────────────────────────────────────
# 5. VOLUME ANOMALIES
# ─────────────────────────────────────────────────────────────────────────────

def detect_volume_anomalies(volume_stats: dict) -> list:
    """
    Detect volume anomalies — sudden drops or spikes.

    Compares each batch to the average row count.
    """
    anomalies = []
    batch_data = volume_stats.get("batch_data", [])
    avg_rows   = volume_stats.get("avg_rows", 0)

    if not batch_data or avg_rows == 0:
        return anomalies

    for batch in batch_data:
        rows      = batch["total_rows"]
        pct_diff  = ((rows - avg_rows) / avg_rows) * 100

        if pct_diff <= -THRESHOLDS["volume_drop_pct"]:
            anomalies.append(_anomaly(
                severity    = "CRITICAL",
                category    = "volume_stats",
                description = f"Batch {batch['batch_num']} row count "
                              f"dropped {abs(round(pct_diff, 1))}% "
                              f"below average ({rows} vs avg {avg_rows})",
                details     = {
                    "batch_num":  batch["batch_num"],
                    "row_count":  rows,
                    "avg_rows":   avg_rows,
                    "pct_drop":   abs(round(pct_diff, 1)),
                    "likely_cause": "Missing upstream data or "
                                   "failed ingestion job"
                }
            ))
        elif pct_diff >= THRESHOLDS["volume_spike_pct"]:
            anomalies.append(_anomaly(
                severity    = "WARNING",
                category    = "volume_stats",
                description = f"Batch {batch['batch_num']} row count "
                              f"spiked {round(pct_diff, 1)}% "
                              f"above average ({rows} vs avg {avg_rows})",
                details     = {
                    "batch_num":  batch["batch_num"],
                    "row_count":  rows,
                    "avg_rows":   avg_rows,
                    "pct_spike":  round(pct_diff, 1),
                    "likely_cause": "Duplicate ingestion or "
                                   "backfill operation"
                }
            ))

    return anomalies


# ─────────────────────────────────────────────────────────────────────────────
# 6. MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def detect_all_anomalies(context: dict) -> dict:
    """
    Run all anomaly detectors against the full metrics context.

    Returns structured anomaly report with:
      - all anomalies organized by severity
      - summary counts
      - overall pipeline health score

    This is the single entry point the LLM agent calls.
    """
    print("\n[AnomalyDetector] Running anomaly detection...")

    all_anomalies = []
    all_anomalies += detect_null_rate_anomalies(
        context.get("null_rates", {}))
    all_anomalies += detect_schema_drift_anomalies(
        context.get("schema_drift", {}))
    all_anomalies += detect_duplicate_anomalies(
        context.get("duplicate_rates", {}))
    all_anomalies += detect_violation_anomalies(
        context.get("rule_violations", {}))
    all_anomalies += detect_volume_anomalies(
        context.get("volume_stats", {}))

    # Organize by severity
    critical = [a for a in all_anomalies if a["severity"] == "CRITICAL"]
    warnings = [a for a in all_anomalies if a["severity"] == "WARNING"]
    info     = [a for a in all_anomalies if a["severity"] == "INFO"]

    # Pipeline health score (0-100)
    # Starts at 100, deduct points per anomaly
    health_score = 100
    health_score -= len(critical) * 20
    health_score -= len(warnings) * 10
    health_score -= len(info) * 2
    health_score  = max(0, health_score)

    report = {
        "total_anomalies": len(all_anomalies),
        "critical_count":  len(critical),
        "warning_count":   len(warnings),
        "info_count":      len(info),
        "health_score":    health_score,
        "health_status":   "CRITICAL" if health_score < 40
                           else "WARNING" if health_score < 70
                           else "HEALTHY",
        "anomalies":       {
            "critical": critical,
            "warnings": warnings,
            "info":     info
        }
    }

    print(f"[AnomalyDetector] Detection complete:")
    print(f"  Health score:  {health_score}/100 ({report['health_status']})")
    print(f"  Critical:      {len(critical)}")
    print(f"  Warnings:      {len(warnings)}")
    print(f"  Info:          {len(info)}")

    return report


# ─────────────────────────────────────────────────────────────────────────────
# CLI — test the detector
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json
    from dq_metrics.spark_session import create_spark_session, stop_spark_session
    from llm_agent.metrics_reader import build_full_context

    spark = create_spark_session(app_name="Anomaly-Detector-Test")

    try:
        # Step 1 — Build context from Delta Lake
        context = build_full_context(spark)

        # Step 2 — Run anomaly detection
        report = detect_all_anomalies(context)

        # Step 3 — Print report
        print("\n" + "="*60)
        print("ANOMALY DETECTION REPORT")
        print("="*60)
        print(json.dumps(report, indent=2))

    finally:
        stop_spark_session(spark)