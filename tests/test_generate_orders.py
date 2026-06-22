"""
Unit Tests — Synthetic Data Generator
======================================
Tests for generate_orders.py

WHY WE TEST THE GENERATOR (Interview Talking Point):
  Untested data generators are a silent risk.
  If your synthetic data doesn't actually contain
  the DQ issues you think it does, your entire
  agent is validating against a lie.
  These tests prove the generator behaves exactly
  as designed.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "src")
))

from data_generator.generate_orders import (
    generate_batch,
    generate_all_batches
)


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA DRIFT TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestSchemaDrift:

    def test_batch_1_has_state_column(self):
        df = generate_batch(batch_num=1, n_records=200)
        assert "state" in df.columns
        assert "shipping_state" not in df.columns

    def test_batch_3_has_split_state_columns(self):
        df = generate_batch(batch_num=3, n_records=200)
        assert "shipping_state" in df.columns
        assert "billing_state" in df.columns
        assert "state" not in df.columns

    def test_batch_5_has_discount_pct(self):
        df = generate_batch(batch_num=5, n_records=200)
        assert "discount_pct" in df.columns

    def test_batch_2_has_no_discount_pct(self):
        df = generate_batch(batch_num=2, n_records=200)
        assert "discount_pct" not in df.columns

    def test_column_count_grows_across_batches(self):
        df1 = generate_batch(batch_num=1, n_records=100)
        df3 = generate_batch(batch_num=3, n_records=100)
        df5 = generate_batch(batch_num=5, n_records=100)
        assert len(df1.columns) < len(df3.columns)
        assert len(df3.columns) < len(df5.columns)


# ─────────────────────────────────────────────────────────────────────────────
# DQ INJECTION TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestDQIssues:

    def test_nulls_are_injected(self):
        df          = generate_batch(batch_num=1, n_records=500, null_rate=0.5)
        total_nulls = df.isnull().sum().sum()
        assert total_nulls > 0, "Expected nulls to be injected"

    def test_order_id_never_null(self):
        df = generate_batch(batch_num=1, n_records=500, null_rate=0.9)
        assert df["order_id"].isnull().sum() == 0

    def test_batch_num_never_null(self):
        df = generate_batch(batch_num=1, n_records=500, null_rate=0.9)
        assert df["batch_num"].isnull().sum() == 0

    def test_duplicates_are_injected(self):
        df        = generate_batch(batch_num=1, n_records=500, dup_rate=0.1)
        dup_count = df.duplicated(subset=["order_id"]).sum()
        assert dup_count > 0, "Expected duplicate order_ids"

    def test_invalid_prices_exist(self):
        df         = generate_batch(batch_num=1, n_records=500, invalid_rate=0.5)
        neg_prices = (df["unit_price"] < 0).sum()
        assert neg_prices > 0, "Expected negative unit prices"

    def test_invalid_quantities_exist(self):
        df      = generate_batch(batch_num=1, n_records=500, invalid_rate=0.5)
        neg_qty = (df["quantity"] < 0).sum()
        assert neg_qty > 0, "Expected negative quantities"


# ─────────────────────────────────────────────────────────────────────────────
# BATCH GENERATION TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestBatchGeneration:

    def test_all_batches_generated(self):
        batches = generate_all_batches(n_batches=6, n_records=100)
        assert len(batches) == 6

    def test_batch_keys_are_sequential(self):
        batches = generate_all_batches(n_batches=3, n_records=100)
        assert list(batches.keys()) == [1, 2, 3]

    def test_each_batch_is_dataframe(self):
        import pandas as pd
        batches = generate_all_batches(n_batches=2, n_records=100)
        for _, df in batches.items():
            assert isinstance(df, pd.DataFrame)

    def test_null_rate_increases_across_batches(self):
        # Later batches should have higher null rates by design
        batches   = generate_all_batches(n_batches=6, n_records=500)
        null_rate_1 = batches[1].isnull().mean().mean()
        null_rate_6 = batches[6].isnull().mean().mean()
        assert null_rate_6 > null_rate_1, \
            "Null rate should increase across batches"

    def test_record_count_approximately_correct(self):
        df = generate_batch(batch_num=1, n_records=1000, dup_rate=0.02)
        # With 2% dup rate, expect between 1000 and 1100 records
        assert 1000 <= len(df) <= 1100