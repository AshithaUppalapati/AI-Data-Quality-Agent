"""
Unit Tests — DQ Metrics Job
============================
Tests for all 5 metric computation functions.

TESTING PHILOSOPHY (Interview Talking Point):
  We test behavior, not implementation.
  Each test answers: "given this input, do I get this output?"
  We don't test Spark internals — we test our business logic.

TEST DATA STRATEGY:
  We create minimal synthetic DataFrames per test.
  No file I/O, no dependencies on data/raw/ CSVs.
  Tests are fast, isolated, and deterministic.
"""

import pytest
import sys
import os

# Add src/ to path
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "src")
))

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
from dq_metrics.spark_session import create_spark_session, stop_spark_session
from dq_metrics.dq_metrics_job import (
    compute_null_rates,
    compute_schema_fingerprint,
    compute_duplicate_rate,
    compute_rule_violations,
    compute_volume_stats,
)


# ─────────────────────────────────────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """
    Create one shared Spark session for all tests.
    scope="session" means it's created once and reused —
    Spark startup is expensive, so we minimize it.
    Stopped automatically after all tests complete.
    """
    spark = create_spark_session(app_name="DQ-Tests", env="test")
    yield spark
    stop_spark_session(spark)


@pytest.fixture
def clean_df(spark):
    """
    A clean DataFrame with no nulls, no violations.
    Baseline for testing — everything should pass.
    """
    data = [
        ("ORD_001", "CUST_001", "2024-01-15", "PROD_100",
         2, 50.0, 100.0, "delivered", "credit_card", 1),
        ("ORD_002", "CUST_002", "2024-01-16", "PROD_101",
         1, 25.0, 25.0,  "shipped",   "paypal",      1),
        ("ORD_003", "CUST_003", "2024-01-17", "PROD_102",
         3, 75.0, 225.0, "pending",   "debit_card",  1),
        ("ORD_004", "CUST_004", "2024-01-18", "PROD_103",
         1, 10.0, 10.0,  "confirmed", "credit_card", 1),
    ]
    schema = StructType([
        StructField("order_id",       StringType(),  False),
        StructField("customer_id",    StringType(),  True),
        StructField("order_date",     StringType(),  True),
        StructField("product_id",     StringType(),  True),
        StructField("quantity",       IntegerType(), True),
        StructField("unit_price",     DoubleType(),  True),
        StructField("total_amount",   DoubleType(),  True),
        StructField("order_status",   StringType(),  True),
        StructField("payment_method", StringType(),  True),
        StructField("batch_num",      IntegerType(), False),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def dirty_df(spark):
    """
    A DataFrame with known DQ issues baked in.
    Used to verify our detectors catch what they should.

    Issues injected:
      - 1 null customer_id
      - 1 negative unit_price
      - 1 negative quantity
      - 1 invalid order_status
      - 1 duplicate order_id
    """
    data = [
        ("ORD_001", "CUST_001", "2024-01-15", "PROD_100",
         2,   50.0,  100.0, "delivered", "credit_card", 1),
        ("ORD_002", None,       "2024-01-16", "PROD_101",
         1,   25.0,  25.0,  "shipped",   "paypal",      1),  # null customer_id
        ("ORD_003", "CUST_003", "2024-01-17", "PROD_102",
         -1,  75.0,  225.0, "pending",   "debit_card",  1),  # negative quantity
        ("ORD_004", "CUST_004", "2024-01-18", "PROD_103",
         1,  -10.0,  10.0,  "confirmed", "credit_card", 1),  # negative price
        ("ORD_005", "CUST_005", "2024-01-19", "PROD_104",
         1,   30.0,  30.0,  "UNKNOWN",   "credit_card", 1),  # invalid status
        ("ORD_001", "CUST_001", "2024-01-15", "PROD_100",
         2,   50.0,  100.0, "delivered", "credit_card", 1),  # duplicate ORD_001
    ]
    schema = StructType([
        StructField("order_id",       StringType(),  False),
        StructField("customer_id",    StringType(),  True),
        StructField("order_date",     StringType(),  True),
        StructField("product_id",     StringType(),  True),
        StructField("quantity",       IntegerType(), True),
        StructField("unit_price",     DoubleType(),  True),
        StructField("total_amount",   DoubleType(),  True),
        StructField("order_status",   StringType(),  True),
        StructField("payment_method", StringType(),  True),
        StructField("batch_num",      IntegerType(), False),
    ])
    return spark.createDataFrame(data, schema)


# ─────────────────────────────────────────────────────────────────────────────
# NULL RATE TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestNullRates:

    def test_no_nulls_on_clean_data(self, clean_df):
        result = compute_null_rates(clean_df, batch_num=1)
        rows   = {r["column_name"]: r["null_rate_pct"]
                  for r in result.collect()}
        # All columns should have 0% null rate
        for col, rate in rows.items():
            assert rate == 0.0, \
                f"Expected 0% null rate for {col}, got {rate}"

    def test_null_detected_on_dirty_data(self, dirty_df):
        result = compute_null_rates(dirty_df, batch_num=1)
        rows   = {r["column_name"]: r["null_count"]
                  for r in result.collect()}
        # customer_id has 1 null
        assert rows["customer_id"] == 1

    def test_order_id_never_null(self, dirty_df):
        result = compute_null_rates(dirty_df, batch_num=1)
        rows   = {r["column_name"]: r["null_count"]
                  for r in result.collect()}
        assert rows["order_id"] == 0

    def test_batch_num_in_output(self, clean_df):
        result = compute_null_rates(clean_df, batch_num=3)
        batch_nums = [r["batch_num"] for r in result.collect()]
        assert all(b == 3 for b in batch_nums)

    def test_null_rate_calculation(self, dirty_df):
        # 6 total rows, 1 null customer_id = 16.6667%
        result = compute_null_rates(dirty_df, batch_num=1)
        rows   = {r["column_name"]: r["null_rate_pct"]
                  for r in result.collect()}
        expected = round((1 / 6) * 100, 4)
        assert rows["customer_id"] == expected


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA FINGERPRINT TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestSchemaFingerprint:

    def test_fingerprint_is_sorted(self, clean_df):
        result      = compute_schema_fingerprint(clean_df, batch_num=1)
        fingerprint = result.collect()[0]["fingerprint"]
        cols        = fingerprint.split("|")
        assert cols == sorted(cols), \
            "Fingerprint columns should be sorted alphabetically"

    def test_col_count_correct(self, clean_df):
        result    = compute_schema_fingerprint(clean_df, batch_num=1)
        col_count = result.collect()[0]["col_count"]
        assert col_count == len(clean_df.columns)

    def test_different_schemas_different_fingerprints(self, spark, clean_df):
        # Add a new column to simulate schema drift
        from pyspark.sql import functions as F
        drifted_df  = clean_df.withColumn("discount_pct", F.lit(0.1))
        fp_original = compute_schema_fingerprint(
            clean_df, batch_num=1).collect()[0]["fingerprint"]
        fp_drifted  = compute_schema_fingerprint(
            drifted_df, batch_num=2).collect()[0]["fingerprint"]
        assert fp_original != fp_drifted, \
            "Different schemas should produce different fingerprints"

    def test_same_schema_same_fingerprint(self, spark, clean_df):
        # Same schema, different batch — fingerprint should match
        fp1 = compute_schema_fingerprint(
            clean_df, batch_num=1).collect()[0]["fingerprint"]
        fp2 = compute_schema_fingerprint(
            clean_df, batch_num=2).collect()[0]["fingerprint"]
        assert fp1 == fp2


# ─────────────────────────────────────────────────────────────────────────────
# DUPLICATE RATE TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestDuplicateRate:

    def test_no_duplicates_on_clean_data(self, clean_df):
        result   = compute_duplicate_rate(clean_df, batch_num=1)
        dup_rate = result.collect()[0]["dup_rate_pct"]
        assert dup_rate == 0.0

    def test_duplicate_detected(self, dirty_df):
        result    = compute_duplicate_rate(dirty_df, batch_num=1)
        dup_count = result.collect()[0]["dup_count"]
        assert dup_count == 1, \
            "Expected 1 duplicate order_id (ORD_001)"

    def test_duplicate_rate_calculation(self, dirty_df):
        # 6 rows, 1 duplicate order_id = 16.6667%
        result   = compute_duplicate_rate(dirty_df, batch_num=1)
        dup_rate = result.collect()[0]["dup_rate_pct"]
        expected = round((1 / 6) * 100, 4)
        assert dup_rate == expected


# ─────────────────────────────────────────────────────────────────────────────
# BUSINESS RULE VIOLATION TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestRuleViolations:

    def test_no_violations_on_clean_data(self, clean_df):
        result     = compute_rule_violations(clean_df, batch_num=1)
        violations = result.collect()[0]["total_violations"]
        assert violations == 0

    def test_negative_price_detected(self, dirty_df):
        result    = compute_rule_violations(dirty_df, batch_num=1)
        neg_price = result.collect()[0]["negative_price"]
        assert neg_price == 1

    def test_negative_quantity_detected(self, dirty_df):
        result  = compute_rule_violations(dirty_df, batch_num=1)
        neg_qty = result.collect()[0]["negative_quantity"]
        assert neg_qty == 1

    def test_invalid_status_detected(self, dirty_df):
        result         = compute_rule_violations(dirty_df, batch_num=1)
        invalid_status = result.collect()[0]["invalid_status"]
        assert invalid_status == 1

    def test_total_violations_sum_correct(self, dirty_df):
        result     = compute_rule_violations(dirty_df, batch_num=1)
        row        = result.collect()[0]
        expected   = (row["negative_price"] +
                      row["negative_quantity"] +
                      row["future_dates"] +
                      row["invalid_status"])
        assert row["total_violations"] == expected


# ─────────────────────────────────────────────────────────────────────────────
# VOLUME STATS TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestVolumeStats:

    def test_row_count_correct(self, clean_df):
        result     = compute_volume_stats(clean_df, batch_num=1)
        total_rows = result.collect()[0]["total_rows"]
        assert total_rows == clean_df.count()

    def test_col_count_correct(self, clean_df):
        result    = compute_volume_stats(clean_df, batch_num=1)
        col_count = result.collect()[0]["col_count"]
        assert col_count == len(clean_df.columns)

    def test_batch_num_correct(self, clean_df):
        result    = compute_volume_stats(clean_df, batch_num=4)
        batch_num = result.collect()[0]["batch_num"]
        assert batch_num == 4