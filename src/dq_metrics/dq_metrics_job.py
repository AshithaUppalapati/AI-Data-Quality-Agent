"""
DQ Metrics Job
==============
Reads all synthetic e-commerce order batches and computes
5 categories of data quality metrics using PySpark.

Output is written to Delta Lake under data/metrics/.

WHY SPARK OVER PANDAS (Interview Talking Point):
  Pandas loads all data into memory on one machine.
  Spark distributes processing across cores/nodes.

MEDALLION ARCHITECTURE:
  data/raw/          → Bronze layer (raw CSVs as-is)
  data/metrics/      → Silver layer (computed DQ metrics)
  Phase 2 outputs    → Gold layer (LLM insights, reports)
"""

import os
import pandas as pd
from datetime import datetime
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import sys
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
))
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))
from dq_metrics.spark_session import create_spark_session, stop_spark_session
from dq_metrics.delta_writer import write_all_metrics, read_metric_from_delta
from logging_config import get_logger

logger = get_logger(__name__)

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
RAW_DATA_PATH     = os.path.join(PROJECT_ROOT, "data", "raw")
METRICS_BASE_PATH = os.path.join(PROJECT_ROOT, "data", "metrics")


def load_batch(spark: SparkSession, batch_num: int) -> DataFrame:
    path = os.path.join(RAW_DATA_PATH, f"orders_batch_{batch_num:02d}.csv")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path)

    logger.info("Batch %d: %d rows, %d cols", batch_num, df.count(), len(df.columns))
    return df


def load_all_batches(spark: SparkSession, n_batches: int = 6) -> dict:
    return {i: load_batch(spark, i) for i in range(1, n_batches + 1)}


def compute_null_rates(df: DataFrame, batch_num: int) -> DataFrame:
    spark      = df.sparkSession
    total_rows = df.count()

    agg_exprs = [
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]

    null_counts_row = df.agg(*agg_exprs).collect()[0]

    records = []
    for col_name in df.columns:
        null_count = null_counts_row[col_name] or 0
        null_rate  = round((null_count / total_rows) * 100, 4) \
                     if total_rows > 0 else 0.0
        records.append({
            "batch_num":     batch_num,
            "column_name":   col_name,
            "null_count":    int(null_count),
            "total_rows":    int(total_rows),
            "null_rate_pct": float(null_rate),
            "computed_at":   datetime.now().isoformat()
        })

    return spark.createDataFrame(pd.DataFrame(records))


def compute_schema_fingerprint(df: DataFrame, batch_num: int) -> DataFrame:
    spark       = df.sparkSession
    columns     = sorted(df.columns)
    fingerprint = "|".join(columns)
    col_count   = len(columns)

    return spark.createDataFrame(pd.DataFrame([{
        "batch_num":   batch_num,
        "col_count":   col_count,
        "fingerprint": fingerprint,
        "columns":     str(columns),
        "computed_at": datetime.now().isoformat()
    }]))


def compute_duplicate_rate(df: DataFrame, batch_num: int) -> DataFrame:
    spark      = df.sparkSession
    total_rows = df.count()

    dup_count = df.groupBy("order_id") \
                  .count() \
                  .filter(F.col("count") > 1) \
                  .count()

    dup_rate = round((dup_count / total_rows) * 100, 4) \
               if total_rows > 0 else 0.0

    return spark.createDataFrame(pd.DataFrame([{
        "batch_num":    batch_num,
        "total_rows":   int(total_rows),
        "dup_count":    int(dup_count),
        "dup_rate_pct": float(dup_rate),
        "computed_at":  datetime.now().isoformat()
    }]))


def compute_rule_violations(df: DataFrame, batch_num: int) -> DataFrame:
    spark      = df.sparkSession
    total_rows = df.count()

    valid_statuses = [
        "pending", "confirmed", "shipped",
        "delivered", "cancelled", "refunded"
    ]

    neg_price = df.filter(
        F.col("unit_price").isNotNull() &
        (F.col("unit_price") < 0)
    ).count()

    neg_qty = df.filter(
        F.col("quantity").isNotNull() &
        (F.col("quantity") < 0)
    ).count()

    future_dates = df.filter(
        F.col("order_date").isNotNull() &
        (F.col("order_date") > F.current_timestamp())
    ).count()

    invalid_status = df.filter(
        F.col("order_status").isNotNull() &
        (~F.col("order_status").isin(valid_statuses))
    ).count()

    total_violations = neg_price + neg_qty + \
                       future_dates + invalid_status

    return spark.createDataFrame(pd.DataFrame([{
        "batch_num":         batch_num,
        "total_rows":        int(total_rows),
        "negative_price":    int(neg_price),
        "negative_quantity": int(neg_qty),
        "future_dates":      int(future_dates),
        "invalid_status":    int(invalid_status),
        "total_violations":  int(total_violations),
        "computed_at":       datetime.now().isoformat()
    }]))


def compute_volume_stats(df: DataFrame, batch_num: int) -> DataFrame:
    spark      = df.sparkSession
    total_rows = df.count()
    col_count  = len(df.columns)

    return spark.createDataFrame(pd.DataFrame([{
        "batch_num":   batch_num,
        "total_rows":  int(total_rows),
        "col_count":   col_count,
        "computed_at": datetime.now().isoformat()
    }]))


def run_dq_metrics_job(spark: SparkSession, n_batches: int = 6) -> dict:
    print("\n" + "="*60)
    print("DQ METRICS JOB STARTING")
    print("="*60)

    all_null_rates   = []
    all_fingerprints = []
    all_dup_rates    = []
    all_violations   = []
    all_volume_stats = []

    for batch_num in range(1, n_batches + 1):
        print(f"\n--- Processing Batch {batch_num} ---")

        df = load_batch(spark, batch_num)

        all_null_rates.append(compute_null_rates(df, batch_num))
        all_fingerprints.append(compute_schema_fingerprint(df, batch_num))
        all_dup_rates.append(compute_duplicate_rate(df, batch_num))
        all_violations.append(compute_rule_violations(df, batch_num))
        all_volume_stats.append(compute_volume_stats(df, batch_num))

        print(f"--- Batch {batch_num} complete ---")

    logger.info("Combining all batch metrics...")

    def union_all(dfs):
        return reduce(DataFrame.union, dfs)

    results = {
        "null_rates":   union_all(all_null_rates),
        "schema_drift": union_all(all_fingerprints),
        "dup_rates":    union_all(all_dup_rates),
        "violations":   union_all(all_violations),
        "volume_stats": union_all(all_volume_stats),
    }

    print("\n" + "="*60)
    print("DQ METRICS JOB COMPLETE")
    print("="*60)

    return results


if __name__ == "__main__":
    spark = create_spark_session(app_name="DQ-Metrics-Job")

    try:
        results = run_dq_metrics_job(spark)

        print("\n=== NULL RATES (sample) ===")
        results["null_rates"].orderBy(
            "batch_num", "null_rate_pct"
        ).show(20, truncate=False)

        print("\n=== SCHEMA DRIFT ===")
        results["schema_drift"].select(
            "batch_num", "col_count", "fingerprint"
        ).show(truncate=False)

        print("\n=== DUPLICATE RATES ===")
        results["dup_rates"].show(truncate=False)

        print("\n=== RULE VIOLATIONS ===")
        results["violations"].show(truncate=False)

        print("\n=== VOLUME STATS ===")
        results["volume_stats"].show(truncate=False)

        logger.debug("About to write to Delta Lake")

        paths = write_all_metrics(results)

        logger.debug("Delta write complete")

        print("\n=== VERIFICATION: Reading back from Delta Lake ===")

        null_rates_delta = read_metric_from_delta(
            spark, "null_rates", batch_num=5
        )
        print("\nNull rates for Batch 5 (from Delta Lake):")
        null_rates_delta.orderBy(
            "null_rate_pct", ascending=False
        ).show(truncate=False)

        violations_delta = read_metric_from_delta(
            spark, "violations"
        )
        print("\nAll violations (from Delta Lake):")
        violations_delta.orderBy("batch_num").show(truncate=False)

    except Exception as e:
        logger.exception("DQ metrics job failed: %s", e)

    finally:
        stop_spark_session(spark)