"""
Delta Lake Writer
=================
Persists DQ metrics DataFrames to Delta Lake tables.

MEDALLION ARCHITECTURE (Interview Talking Point):
  Bronze → raw CSVs (data/raw/)
  Silver → computed metrics (data/metrics/) ← THIS FILE
  Gold   → LLM insights, aggregated reports (Phase 2)

WHY DELTA LAKE OVER PLAIN PARQUET:
  1. ACID transactions — no partial writes on failure
  2. Time travel — query metrics from any past run
  3. Schema enforcement — rejects wrong-shaped data
  4. Efficient upserts via MERGE (future improvement)

WRITE MODE — OVERWRITE:
  We overwrite each metric table on every run.
  This keeps the table clean and avoids duplicates.
"""

import os
from pyspark.sql import DataFrame
from logging_config import get_logger

logger = get_logger(__name__)

PROJECT_ROOT      = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
METRICS_BASE_PATH = os.path.join(PROJECT_ROOT, "data", "metrics")


def write_metric_to_delta(
    df:           DataFrame,
    metric_name:  str,
    partition_by: str = "batch_num",
    mode:         str = "overwrite",
) -> str:
    """
    Write a metrics DataFrame to a Delta Lake table.
    Returns the path where the Delta table was written.
    """
    output_path = os.path.join(METRICS_BASE_PATH, metric_name)

    logger.info("Writing %s -> %s", metric_name, output_path)
    logger.info(
        "Rows: %d | Mode: %s | Partition: %s",
        df.count(), mode, partition_by
    )

    df.write \
      .format("delta") \
      .mode(mode) \
      .partitionBy(partition_by) \
      .save(output_path)

    logger.info("%s written successfully", metric_name)
    return output_path


def write_all_metrics(results: dict) -> dict:
    """
    Write all DQ metric DataFrames to Delta Lake.
    Returns Dict of {metric_name: delta_path}.
    """
    print("\n" + "="*60)
    print("WRITING METRICS TO DELTA LAKE")
    print("="*60)

    partition_map = {
        "null_rates":   "batch_num",
        "schema_drift": "batch_num",
        "dup_rates":    "batch_num",
        "violations":   "batch_num",
        "volume_stats": "batch_num",
    }

    paths = {}
    for metric_name, df in results.items():
        partition_col = partition_map.get(metric_name, "batch_num")
        paths[metric_name] = write_metric_to_delta(
            df           = df,
            metric_name  = metric_name,
            partition_by = partition_col,
            mode         = "overwrite",
        )

    print("\n" + "="*60)
    print("ALL METRICS WRITTEN TO DELTA LAKE")
    print("="*60)
    logger.info("Delta table locations:")
    for name, path in paths.items():
        logger.info("  %-15s -> %s", name, path)

    return paths


def read_metric_from_delta(
    spark,
    metric_name: str,
    batch_num:   int = None,
) -> DataFrame:
    """
    Read a metric back from Delta Lake.
    Optionally filter by batch_num for partition pruning.
    """
    path = os.path.join(METRICS_BASE_PATH, metric_name)

    df = spark.read.format("delta").load(path)

    if batch_num is not None:
        df = df.filter(df.batch_num == batch_num)
        logger.info(
            "%s | batch_num=%s | Partition pruning active",
            metric_name, batch_num
        )

    return df