"""
Statistical Baseline Detector
==============================
Detects anomalies using statistical methods rather than
hardcoded thresholds.

WHY STATISTICAL DETECTION:
  Rule-based detection requires you to know what's wrong
  before it happens. Statistical detection catches anything
  that's statistically unusual — even violation types you
  never anticipated.

TWO METHODS IMPLEMENTED:
  1. Z-SCORE DETECTION — assumes normal distribution
  2. IQR DETECTION — does NOT assume normal distribution,
     more robust for small datasets
"""

import os
import sys
import math

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from logging_config import get_logger

logger = get_logger(__name__)


def _mean(values: list) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _std_dev(values: list) -> float:
    if len(values) < 2:
        return 0.0
    mean = _mean(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return math.sqrt(variance)


def _z_score(value: float, mean: float, std: float) -> float:
    if std == 0:
        return 0.0
    return (value - mean) / std


def _iqr_bounds(values: list) -> tuple:
    if len(values) < 4:
        return (float('-inf'), float('inf'))

    sorted_vals = sorted(values)
    n           = len(sorted_vals)

    q1 = sorted_vals[n // 4]
    q3 = sorted_vals[(3 * n) // 4]
    iqr = q3 - q1

    lower_fence = q1 - 1.5 * iqr
    upper_fence = q3 + 1.5 * iqr

    return (lower_fence, upper_fence)


def _stat_anomaly(
    severity:    str,
    metric:      str,
    batch_num:   int,
    value:       float,
    description: str,
    stats:       dict
) -> dict:
    return {
        "severity":        severity,
        "category":        "statistical",
        "metric":          metric,
        "batch_num":       batch_num,
        "value":           value,
        "description":     description,
        "detection_method": stats.get("method", "unknown"),
        "stats":           stats
    }


def detect_null_rate_stats(null_rates: dict) -> list:
    anomalies  = []
    batch_data = null_rates.get("batch_trend", [])

    if len(batch_data) < 3:
        return anomalies

    values    = [b["avg_null_rate_pct"] for b in batch_data]
    mean      = _mean(values)
    std       = _std_dev(values)
    lower_iqr, upper_iqr = _iqr_bounds(values)

    for batch in batch_data:
        val     = batch["avg_null_rate_pct"]
        z       = _z_score(val, mean, std)
        in_iqr  = lower_iqr <= val <= upper_iqr

        if abs(z) > 2.0 or not in_iqr:
            severity = "CRITICAL" if abs(z) > 3.0 else "WARNING"
            anomalies.append(_stat_anomaly(
                severity    = severity,
                metric      = "null_rate",
                batch_num   = batch["batch_num"],
                value       = val,
                description = f"Batch {batch['batch_num']} null rate "
                              f"{val}% is statistically unusual "
                              f"(z-score: {round(z, 2)}, "
                              f"mean: {round(mean, 2)}%, "
                              f"std: {round(std, 2)}%)",
                stats       = {
                    "method":   "z-score + IQR",
                    "z_score":  round(z, 4),
                    "mean":     round(mean, 4),
                    "std":      round(std, 4),
                    "iqr_lower": round(lower_iqr, 4),
                    "iqr_upper": round(upper_iqr, 4)
                }
            ))

    return anomalies


def detect_violation_stats(rule_violations: dict) -> list:
    anomalies  = []
    batch_data = rule_violations.get("batch_data", [])

    if len(batch_data) < 3:
        return anomalies

    values = [b["total_violations"] for b in batch_data]
    mean   = _mean(values)
    std    = _std_dev(values)
    lower_iqr, upper_iqr = _iqr_bounds(values)

    for batch in batch_data:
        val = batch["total_violations"]
        z   = _z_score(val, mean, std)

        if val > upper_iqr or abs(z) > 2.0:
            severity = "CRITICAL" if abs(z) > 3.0 else "WARNING"
            anomalies.append(_stat_anomaly(
                severity    = severity,
                metric      = "rule_violations",
                batch_num   = batch["batch_num"],
                value       = val,
                description = f"Batch {batch['batch_num']} has "
                              f"{val} violations — statistically "
                              f"high (z-score: {round(z, 2)}, "
                              f"mean: {round(mean, 1)}, "
                              f"IQR upper: {round(upper_iqr, 1)})",
                stats       = {
                    "method":    "z-score + IQR",
                    "z_score":   round(z, 4),
                    "mean":      round(mean, 4),
                    "std":       round(std, 4),
                    "iqr_upper": round(upper_iqr, 4)
                }
            ))

    trend_anomaly = _detect_trend(
        values    = values,
        metric    = "rule_violations",
        batch_data = batch_data
    )
    if trend_anomaly:
        anomalies.append(trend_anomaly)

    return anomalies


def detect_duplicate_stats(duplicate_rates: dict) -> list:
    anomalies  = []
    batch_data = duplicate_rates.get("batch_data", [])

    if len(batch_data) < 3:
        return anomalies

    values = [b["dup_rate_pct"] for b in batch_data]
    mean   = _mean(values)
    std    = _std_dev(values)
    lower_iqr, upper_iqr = _iqr_bounds(values)

    for batch in batch_data:
        val = batch["dup_rate_pct"]
        z   = _z_score(val, mean, std)

        if abs(z) > 2.0 or not (lower_iqr <= val <= upper_iqr):
            severity = "CRITICAL" if abs(z) > 3.0 else "WARNING"
            anomalies.append(_stat_anomaly(
                severity    = severity,
                metric      = "duplicate_rate",
                batch_num   = batch["batch_num"],
                value       = val,
                description = f"Batch {batch['batch_num']} duplicate "
                              f"rate {val}% is statistically unusual "
                              f"(z-score: {round(z, 2)})",
                stats       = {
                    "method":  "z-score + IQR",
                    "z_score": round(z, 4),
                    "mean":    round(mean, 4),
                    "std":     round(std, 4)
                }
            ))

    return anomalies


def detect_volume_stats(volume_stats: dict) -> list:
    anomalies  = []
    batch_data = volume_stats.get("batch_data", [])

    if len(batch_data) < 3:
        return anomalies

    values = [b["total_rows"] for b in batch_data]
    mean   = _mean(values)
    std    = _std_dev(values)
    lower_iqr, upper_iqr = _iqr_bounds(values)

    for batch in batch_data:
        val = batch["total_rows"]
        z   = _z_score(val, mean, std)

        if abs(z) > 2.0 or not (lower_iqr <= val <= upper_iqr):
            severity  = "CRITICAL" if abs(z) > 3.0 else "WARNING"
            direction = "low" if val < mean else "high"
            anomalies.append(_stat_anomaly(
                severity    = severity,
                metric      = "volume",
                batch_num   = batch["batch_num"],
                value       = val,
                description = f"Batch {batch['batch_num']} row count "
                              f"{val} is statistically {direction} "
                              f"(z-score: {round(z, 2)}, "
                              f"mean: {round(mean, 0)})",
                stats       = {
                    "method":  "z-score + IQR",
                    "z_score": round(z, 4),
                    "mean":    round(mean, 4),
                    "std":     round(std, 4)
                }
            ))

    return anomalies


def _detect_trend(
    values:     list,
    metric:     str,
    batch_data: list
) -> dict:
    if len(values) < 4:
        return None

    n   = len(values)
    x   = list(range(n))
    x_mean = _mean(x)
    y_mean = _mean(values)

    numerator   = sum((x[i] - x_mean) * (values[i] - y_mean)
                      for i in range(n))
    denominator = sum((x[i] - x_mean) ** 2 for i in range(n))

    if denominator == 0:
        return None

    slope = numerator / denominator

    pct_change_per_batch = (slope / y_mean * 100) \
                           if y_mean > 0 else 0

    if pct_change_per_batch > 5.0:
        return _stat_anomaly(
            severity    = "WARNING",
            metric      = metric,
            batch_num   = batch_data[-1]["batch_num"],
            value       = values[-1],
            description = f"{metric} shows consistent upward trend: "
                          f"+{round(pct_change_per_batch, 1)}% "
                          f"increase per batch on average "
                          f"(slope: {round(slope, 4)})",
            stats       = {
                "method":               "linear_regression",
                "slope":                round(slope, 4),
                "pct_change_per_batch": round(pct_change_per_batch, 2),
                "first_value":          values[0],
                "last_value":           values[-1]
            }
        )

    return None


def detect_statistical_anomalies(context: dict) -> dict:
    logger.info("Running statistical detection...")

    all_anomalies = []
    all_anomalies += detect_null_rate_stats(
        context.get("null_rates", {}))
    all_anomalies += detect_violation_stats(
        context.get("rule_violations", {}))
    all_anomalies += detect_duplicate_stats(
        context.get("duplicate_rates", {}))
    all_anomalies += detect_volume_stats(
        context.get("volume_stats", {}))

    critical = [a for a in all_anomalies if a["severity"] == "CRITICAL"]
    warnings = [a for a in all_anomalies if a["severity"] == "WARNING"]

    logger.info(
        "Statistical detection complete: %d anomalies | critical=%d | warnings=%d",
        len(all_anomalies), len(critical), len(warnings)
    )

    return {
        "detection_method":  "statistical",
        "total_anomalies":   len(all_anomalies),
        "critical_count":    len(critical),
        "warning_count":     len(warnings),
        "anomalies": {
            "critical": critical,
            "warnings": warnings
        }
    }


if __name__ == "__main__":
    import json
    from dq_metrics.spark_session import (
        create_spark_session,
        stop_spark_session
    )
    from llm_agent.metrics_reader import build_full_context

    spark = create_spark_session(
        app_name="Statistical-Detector-Test"
    )

    try:
        print("Reading metrics from Delta Lake...")
        context = build_full_context(spark)

        print("\nRunning statistical detection...")
        report = detect_statistical_anomalies(context)

        print("\n" + "="*60)
        print("STATISTICAL ANOMALY REPORT")
        print("="*60)
        print(json.dumps(report, indent=2))

    finally:
        stop_spark_session(spark)