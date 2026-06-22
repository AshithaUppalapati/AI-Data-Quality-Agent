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
  Production improvement: switch to MERGE on batch_num
  to preserve full history across runs.
"""

import os
from pyspark.sql import DataFrame


# ── Project root ──────────────────────────────────────────────────────────────
PROJECT_ROOT      = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
METRICS_BASE_PATH = os.path.join(PROJECT_ROOT, "data", "metrics")


# ─────────────────────────────────────────────────────────────────────────────
# CORE WRITER
# ─────────────────────────────────────────────────────────────────────────────

def write_metric_to_delta(
    df:           DataFrame,
    metric_name:  str,
    partition_by: str = "batch_num",
    mode:         str = "overwrite",
) -> str:
    """
    Write a metrics DataFrame to a Delta Lake table.

    Args:
        df:           Spark DataFrame containing metrics.
        metric_name:  Name of the metric (used as folder name).
        partition_by: Column to partition by. Default: batch_num.
        mode:         Write mode. Default: overwrite.
                      Options: overwrite, append, merge (future).

    Returns:
        Path where the Delta table was written.

    WHY WE RETURN THE PATH:
      Caller can log it, pass it to the LLM agent,
      or use it to verify the write succeeded.
    """
    output_path = os.path.join(METRICS_BASE_PATH, metric_name)

    print(f"[DeltaWriter] Writing {metric_name} → {output_path}")
    print(f"[DeltaWriter] Rows: {df.count()} | "
          f"Mode: {mode} | "
          f"Partition: {partition_by}")

    df.write \
      .format("delta") \
      .mode(mode) \
      .partitionBy(partition_by) \
      .save(output_path)

    print(f"[DeltaWriter] ✅ {metric_name} written successfully")
    return output_path


# ─────────────────────────────────────────────────────────────────────────────
# WRITE ALL METRICS
# ─────────────────────────────────────────────────────────────────────────────

def write_all_metrics(results: dict) -> dict:
    """
    Write all DQ metric DataFrames to Delta Lake.

    Args:
        results: Dict of {metric_name: DataFrame}
                 from run_dq_metrics_job()

    Returns:
        Dict of {metric_name: delta_path}
        so caller knows where everything landed.
    """
    print("\n" + "="*60)
    print("WRITING METRICS TO DELTA LAKE")
    print("="*60)

    # Map metric names to their partition columns
    # Most partition by batch_num for time-series queries
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
    print("\nDelta table locations:")
    for name, path in paths.items():
        print(f"  {name:15} → {path}")

    return paths


# ─────────────────────────────────────────────────────────────────────────────
# READER — verify writes worked
# ─────────────────────────────────────────────────────────────────────────────

def read_metric_from_delta(
    spark,
    metric_name: str,
    batch_num:   int = None,
) -> DataFrame:
    """
    Read a metric back from Delta Lake.
    Optionally filter by batch_num for partition pruning.

    WHY THIS MATTERS:
      Reading back after write is how you verify
      the Delta table is correct and queryable.
      In production this would be a data contract test.

    PARTITION PRUNING DEMO:
      Passing batch_num adds a filter that tells Spark
      to only read that partition folder — skipping all others.
      At scale this is the difference between reading
      1GB vs 1TB of data.
    """
    path = os.path.join(METRICS_BASE_PATH, metric_name)

    df = spark.read.format("delta").load(path)

    if batch_num is not None:
        df = df.filter(df.batch_num == batch_num)
        print(f"[DeltaReader] {metric_name} | "
              f"batch_num={batch_num} | "
              f"Partition pruning active ✅")

    return df