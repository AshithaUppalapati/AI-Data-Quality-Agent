"""
Spark Session Factory
=====================
Central factory for creating SparkSession instances.

WHY A FACTORY PATTERN:
  Every Spark job needs a configured SparkSession. Instead of
  duplicating configuration across files, we centralize it here.
  This means:
    - One place to change Spark config
    - Consistent Delta Lake setup across all jobs
    - Easy to extend for different environments
      (local → Databricks → cloud cluster)
    - Testable and mockable

DESIGN DECISION — Environment Awareness:
  The factory reads SPARK_ENV environment variable:
    local      → optimized for laptop (2 cores, small memory)
    databricks → minimal config (Databricks manages the rest)
    test       → minimal Spark for fast unit tests
"""

import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def create_spark_session(
    app_name: str = "AI-DQ-Agent",
    env: str = None,
) -> SparkSession:
    """
    Create and return a configured SparkSession.

    Args:
        app_name: Name shown in Spark UI and logs.
        env: Environment override. If None, reads SPARK_ENV
             environment variable. Defaults to 'local'.

    Returns:
        Configured SparkSession with Delta Lake support.
    """
    env = env or os.getenv("SPARK_ENV", "local")

    print(f"[SparkSessionFactory] Creating session | app={app_name} | env={env}")

    if env == "databricks":
        # On Databricks, SparkSession is pre-configured
        # We just get the existing session
        spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()

    elif env == "test":
        # Minimal config for fast unit tests
        # Single thread, no UI, tiny memory footprint
        spark = configure_spark_with_delta_pip(
            SparkSession.builder
            .appName(app_name)
            .master("local[1]")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
        ).getOrCreate()

    else:
        # Local development — use all available cores
        # local[*] means "use all CPU cores on this machine"
        spark = configure_spark_with_delta_pip(
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.memory", "2g")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.driver.extraJavaOptions",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/java.lang=ALL-UNNAMED "
                "--add-opens=java.base/java.util=ALL-UNNAMED "
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
                "--add-opens=java.base/java.io=ALL-UNNAMED "
                "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED")
        .config("spark.executor.extraJavaOptions",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/java.lang=ALL-UNNAMED "
                "--add-opens=java.base/java.util=ALL-UNNAMED "
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
                "--add-opens=java.base/java.io=ALL-UNNAMED "
                "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED")
        ).getOrCreate()

    # Suppress verbose INFO logs — only show warnings and errors
    spark.sparkContext.setLogLevel("ERROR")

    print(f"[SparkSessionFactory] Session ready | "
          f"Spark {spark.version} | "
          f"Master: {spark.sparkContext.master}")

    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """
    Cleanly stop a SparkSession.
    Always call this at the end of a job to release resources.

    WHY THIS MATTERS:
      Not stopping Spark leaves JVM processes running in background.
      In production pipelines this causes memory leaks and port conflicts.
    """
    app_name = spark.sparkContext.appName
    spark.stop()
    print(f"[SparkSessionFactory] Session stopped | app={app_name}")