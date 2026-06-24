"""
Metrics Reader
==============
Reads DQ metrics from Delta Lake and formats them
into structured context for the LLM agent.

WHY THIS MODULE EXISTS (Interview Talking Point):
  Delta Lake stores metrics as distributed Spark DataFrames.
  LLMs need plain text or structured dicts to reason about data.
  This module is the bridge — it reads, aggregates, and formats
  metrics into a context package the LLM can understand.

  Think of it as the "eyes" of the LLM agent.

DESIGN PATTERN — Context Builder:
  Each method reads one metric type and returns a clean dict.
  The build_full_context() method combines all metrics into
  one structured package passed to the LLM.
"""

import os
import sys

# Path setup
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dq_metrics.delta_writer import read_metric_from_delta


# ─────────────────────────────────────────────────────────────────────────────
# 1. NULL RATES READER
# ─────────────────────────────────────────────────────────────────────────────

def read_null_rates(spark: SparkSession) -> dict:
    """
    Read null rates from Delta Lake and return structured summary.

    Returns dict with:
      - per_batch: null rates per column per batch
      - worst_columns: top 5 columns with highest avg null rate
      - trend: null rate trend across batches (improving/degrading)
    """
    df = read_metric_from_delta(spark, "null_rates")

    # Average null rate per column across all batches
    avg_nulls = df.groupBy("column_name") \
                  .agg(F.avg("null_rate_pct").alias("avg_null_rate")) \
                  .orderBy("avg_null_rate", ascending=False)

    # Per batch summary — max null rate per batch
    batch_summary = df.groupBy("batch_num") \
                      .agg(
                          F.avg("null_rate_pct").alias("avg_null_rate"),
                          F.max("null_rate_pct").alias("max_null_rate"),
                          F.sum("null_count").alias("total_nulls")
                      ).orderBy("batch_num")

    # Worst columns
    worst_cols = [
        {
            "column": r["column_name"],
            "avg_null_rate_pct": round(r["avg_null_rate"], 4)
        }
        for r in avg_nulls.limit(5).collect()
        if r["avg_null_rate"] > 0
    ]

    # Batch trend
    batch_data = [
        {
            "batch_num": r["batch_num"],
            "avg_null_rate_pct": round(r["avg_null_rate"], 4),
            "max_null_rate_pct": round(r["max_null_rate"], 4),
            "total_nulls": r["total_nulls"]
        }
        for r in batch_summary.collect()
    ]

    # Detect trend direction
    if len(batch_data) >= 2:
        first_rate = batch_data[0]["avg_null_rate_pct"]
        last_rate  = batch_data[-1]["avg_null_rate_pct"]
        trend = "degrading" if last_rate > first_rate else "improving"
        trend_delta = round(last_rate - first_rate, 4)
    else:
        trend = "insufficient data"
        trend_delta = 0

    return {
        "metric_type": "null_rates",
        "worst_columns": worst_cols,
        "batch_trend": batch_data,
        "trend_direction": trend,
        "trend_delta_pct": trend_delta
    }


# ─────────────────────────────────────────────────────────────────────────────
# 2. SCHEMA DRIFT READER
# ─────────────────────────────────────────────────────────────────────────────

def read_schema_drift(spark: SparkSession) -> dict:
    """
    Read schema fingerprints from Delta Lake.
    Detects which batches have different schemas.

    Returns dict with:
      - drift_detected: True/False
      - drift_batches: which batches have different fingerprints
      - schema_changes: what changed between batches
    """
    df = read_metric_from_delta(spark, "schema_drift")
    rows = df.orderBy("batch_num").collect()

    fingerprints = [
        {
            "batch_num":   r["batch_num"],
            "col_count":   r["col_count"],
            "fingerprint": r["fingerprint"]
        }
        for r in rows
    ]

    # Detect drift — compare consecutive fingerprints
    drift_events = []
    for i in range(1, len(fingerprints)):
        prev = fingerprints[i - 1]
        curr = fingerprints[i]
        if prev["fingerprint"] != curr["fingerprint"]:
            prev_cols = set(prev["fingerprint"].split("|"))
            curr_cols = set(curr["fingerprint"].split("|"))
            added     = list(curr_cols - prev_cols)
            removed   = list(prev_cols - curr_cols)
            drift_events.append({
                "from_batch": prev["batch_num"],
                "to_batch":   curr["batch_num"],
                "cols_added":   added,
                "cols_removed": removed,
                "col_count_change": curr["col_count"] - prev["col_count"]
            })

    return {
        "metric_type":    "schema_drift",
        "drift_detected": len(drift_events) > 0,
        "drift_count":    len(drift_events),
        "drift_events":   drift_events,
        "fingerprints":   fingerprints
    }


# ─────────────────────────────────────────────────────────────────────────────
# 3. DUPLICATE RATES READER
# ─────────────────────────────────────────────────────────────────────────────

def read_duplicate_rates(spark: SparkSession) -> dict:
    """
    Read duplicate rates from Delta Lake.

    Returns dict with:
      - avg_dup_rate: average across all batches
      - worst_batch: batch with highest duplicate rate
      - trend: improving or degrading
    """
    df   = read_metric_from_delta(spark, "dup_rates")
    rows = df.orderBy("batch_num").collect()

    batch_data = [
        {
            "batch_num":    r["batch_num"],
            "dup_count":    r["dup_count"],
            "dup_rate_pct": round(r["dup_rate_pct"], 4)
        }
        for r in rows
    ]

    avg_rate  = round(
        sum(b["dup_rate_pct"] for b in batch_data) / len(batch_data), 4
    ) if batch_data else 0

    worst = max(batch_data, key=lambda x: x["dup_rate_pct"]) \
            if batch_data else None

    return {
        "metric_type":    "duplicate_rates",
        "avg_dup_rate_pct": avg_rate,
        "worst_batch":    worst,
        "batch_data":     batch_data
    }


# ─────────────────────────────────────────────────────────────────────────────
# 4. RULE VIOLATIONS READER
# ─────────────────────────────────────────────────────────────────────────────

def read_rule_violations(spark: SparkSession) -> dict:
    """
    Read business rule violations from Delta Lake.

    Returns dict with:
      - total_violations: across all batches
      - violation_breakdown: by type
      - worst_batch: highest violation count
      - trend: improving or degrading
    """
    df   = read_metric_from_delta(spark, "violations")
    rows = df.orderBy("batch_num").collect()

    batch_data = [
        {
            "batch_num":         r["batch_num"],
            "total_violations":  r["total_violations"],
            "negative_price":    r["negative_price"],
            "negative_quantity": r["negative_quantity"],
            "future_dates":      r["future_dates"],
            "invalid_status":    r["invalid_status"]
        }
        for r in rows
    ]

    total = sum(b["total_violations"] for b in batch_data)
    worst = max(batch_data, key=lambda x: x["total_violations"]) \
            if batch_data else None

    # Violation breakdown across all batches
    breakdown = {
        "negative_price":    sum(b["negative_price"]    for b in batch_data),
        "negative_quantity": sum(b["negative_quantity"] for b in batch_data),
        "future_dates":      sum(b["future_dates"]      for b in batch_data),
        "invalid_status":    sum(b["invalid_status"]    for b in batch_data)
    }

    # Trend
    if len(batch_data) >= 2:
        trend = "degrading" \
                if batch_data[-1]["total_violations"] > \
                   batch_data[0]["total_violations"] \
                else "improving"
    else:
        trend = "insufficient data"

    return {
        "metric_type":       "rule_violations",
        "total_violations":  total,
        "violation_breakdown": breakdown,
        "worst_batch":       worst,
        "batch_data":        batch_data,
        "trend_direction":   trend
    }


# ─────────────────────────────────────────────────────────────────────────────
# 5. VOLUME STATS READER
# ─────────────────────────────────────────────────────────────────────────────

def read_volume_stats(spark: SparkSession) -> dict:
    """
    Read volume statistics from Delta Lake.

    Returns dict with:
      - avg_rows: average row count across batches
      - min/max rows: range
      - col_count_trend: schema growth over time
    """
    df   = read_metric_from_delta(spark, "volume_stats")
    rows = df.orderBy("batch_num").collect()

    batch_data = [
        {
            "batch_num":  r["batch_num"],
            "total_rows": r["total_rows"],
            "col_count":  r["col_count"]
        }
        for r in rows
    ]

    row_counts = [b["total_rows"] for b in batch_data]
    avg_rows   = round(sum(row_counts) / len(row_counts), 0) \
                 if row_counts else 0

    return {
        "metric_type": "volume_stats",
        "avg_rows":    avg_rows,
        "min_rows":    min(row_counts) if row_counts else 0,
        "max_rows":    max(row_counts) if row_counts else 0,
        "batch_data":  batch_data
    }


# ─────────────────────────────────────────────────────────────────────────────
# 6. FULL CONTEXT BUILDER — Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def build_full_context(spark: SparkSession) -> dict:
    """
    Read all metrics and build a complete context package
    for the LLM agent.

    This is the single entry point the LLM agent calls.
    Returns a structured dict containing all metric summaries.

    WHY ONE FUNCTION (Interview Talking Point):
      The LLM agent shouldn't know about Delta Lake internals.
      It just calls build_full_context() and gets everything
      it needs. This is the facade pattern — hiding complexity
      behind a simple interface.
    """
    print("[MetricsReader] Building full context from Delta Lake...")

    context = {
        "null_rates":      read_null_rates(spark),
        "schema_drift":    read_schema_drift(spark),
        "duplicate_rates": read_duplicate_rates(spark),
        "rule_violations": read_rule_violations(spark),
        "volume_stats":    read_volume_stats(spark)
    }

    print("[MetricsReader] Context built successfully")
    print(f"  Null rate trend:     {context['null_rates']['trend_direction']}")
    print(f"  Schema drift:        {context['schema_drift']['drift_count']} events detected")
    print(f"  Total violations:    {context['rule_violations']['total_violations']}")
    print(f"  Avg duplicate rate:  {context['duplicate_rates']['avg_dup_rate_pct']}%")

    return context


# ─────────────────────────────────────────────────────────────────────────────
# CLI — test the reader
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json
    from dq_metrics.spark_session import create_spark_session, stop_spark_session

    spark = create_spark_session(app_name="Metrics-Reader-Test")

    try:
        context = build_full_context(spark)

        print("\n" + "="*60)
        print("FULL CONTEXT PACKAGE")
        print("="*60)
        print(json.dumps(context, indent=2))

    finally:
        stop_spark_session(spark)   