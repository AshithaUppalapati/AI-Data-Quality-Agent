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
from logging_config import get_logger

logger = get_logger(__name__)


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

    logger.info("Creating session | app=%s | env=%s", app_name, env)

    if env == "databricks":
        spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()

    elif env == "test":
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
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        ).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Session ready | Spark %s | Master: %s",
                spark.version, spark.sparkContext.master)

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
    logger.info("Session stopped | app=%s", app_name)