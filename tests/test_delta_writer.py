"""
Unit Tests — Delta Writer
=========================
Tests for Delta Lake read/write operations.
"""

import pytest
import sys
import os
import tempfile

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "src")
))

from dq_metrics.spark_session import create_spark_session, stop_spark_session
from dq_metrics.delta_writer import write_metric_to_delta, read_metric_from_delta
from dq_metrics.dq_metrics_job import compute_null_rates, compute_volume_stats
import pandas as pd


@pytest.fixture(scope="session")
def spark():
    spark = create_spark_session(app_name="DeltaWriter-Tests", env="test")
    yield spark
    stop_spark_session(spark)


@pytest.fixture(scope="session")
def tmp_metrics_path(tmp_path_factory):
    """Temporary directory for Delta Lake writes during tests."""
    return str(tmp_path_factory.mktemp("metrics"))


@pytest.fixture
def sample_df(spark):
    """Simple sample DataFrame for write tests."""
    return spark.createDataFrame(pd.DataFrame([
        {"batch_num": 1, "total_rows": 1000,
         "col_count": 12, "computed_at": "2024-01-01"},
        {"batch_num": 2, "total_rows": 1020,
         "col_count": 13, "computed_at": "2024-02-01"},
    ]))


class TestDeltaWriter:

    def test_write_creates_delta_table(self, sample_df, tmp_metrics_path):
        path = os.path.join(tmp_metrics_path, "test_volume")
        write_metric_to_delta(
            sample_df, "test_volume",
            partition_by="batch_num", mode="overwrite"
        )
        # Delta table folder should exist
        expected = os.path.join(
            os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "data", "metrics")
            ),
            "test_volume"
        )
        assert os.path.exists(expected)

    def test_write_returns_path(self, sample_df):
        path = write_metric_to_delta(
            sample_df, "test_volume_path",
            partition_by="batch_num", mode="overwrite"
        )
        assert path is not None
        assert "test_volume_path" in path

    def test_overwrite_mode_replaces_data(self, spark, sample_df):
        # Write once
        write_metric_to_delta(
            sample_df, "test_overwrite",
            partition_by="batch_num", mode="overwrite"
        )
        # Write again with overwrite
        write_metric_to_delta(
            sample_df, "test_overwrite",
            partition_by="batch_num", mode="overwrite"
        )
        # Read back — should have same row count, not doubled
        result = read_metric_from_delta(spark, "test_overwrite")
        assert result.count() == sample_df.count()


class TestDeltaReader:

    def test_read_returns_correct_row_count(self, spark, sample_df):
        write_metric_to_delta(
            sample_df, "test_read",
            partition_by="batch_num", mode="overwrite"
        )
        result = read_metric_from_delta(spark, "test_read")
        assert result.count() == 2

    def test_partition_filter_returns_subset(self, spark, sample_df):
        write_metric_to_delta(
            sample_df, "test_partition",
            partition_by="batch_num", mode="overwrite"
        )
        result = read_metric_from_delta(
            spark, "test_partition", batch_num=1
        )
        assert result.count() == 1
        assert result.collect()[0]["batch_num"] == 1