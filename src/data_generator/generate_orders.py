"""
Synthetic E-Commerce Order Data Generator
==========================================
Generates realistic order data with intentional quality issues:
  - Schema drift (columns evolve across time batches)
  - Null injection in critical fields
  - Duplicate records
  - Invalid values (negative prices, future dates, bad enums)
  - Realistic distributions using weighted random sampling
"""

import random
import uuid
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
import numpy as np

RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

PRODUCT_CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys"]
PAYMENT_METHODS_V1 = ["credit_card", "debit_card", "paypal"]
PAYMENT_METHODS_V2 = ["credit_card", "debit_card", "paypal", "apple_pay", "crypto"]
ORDER_STATUSES     = ["pending", "confirmed", "shipped", "delivered", "cancelled", "refunded"]
US_STATES          = ["TX", "CA", "NY", "FL", "WA", "IL", "AZ", "CO"]


def _base_order(batch_num: int, reference_date: datetime) -> dict:
    order_date = reference_date - timedelta(days=random.randint(0, 30))
    quantity   = random.randint(1, 10)
    unit_price = round(random.uniform(5.0, 500.0), 2)

    record = {
        "order_id":           str(uuid.uuid4()),
        "customer_id":        f"CUST_{random.randint(1000, 9999)}",
        "order_date":         order_date.strftime("%Y-%m-%d %H:%M:%S"),
        "product_id":         f"PROD_{random.randint(100, 999)}",
        "product_category":   random.choice(PRODUCT_CATEGORIES),
        "quantity":           quantity,
        "unit_price":         unit_price,
        "total_amount":       round(quantity * unit_price, 2),
        "order_status":       random.choice(ORDER_STATUSES),
        "payment_method":     random.choice(
            PAYMENT_METHODS_V2 if batch_num >= 4 else PAYMENT_METHODS_V1
        ),
        "batch_num":          batch_num,
    }

    # Schema drift: Batch 1-2 uses 'state', Batch 3+ splits into two columns
    if batch_num <= 2:
        record["state"] = random.choice(US_STATES)
    else:
        record["shipping_state"] = random.choice(US_STATES)
        record["billing_state"]  = random.choice(US_STATES)

    # Schema drift: discount_pct added in Batch 5
    if batch_num >= 5:
        record["discount_pct"] = round(random.uniform(0, 0.30), 2)

    return record


def _inject_nulls(record: dict, null_rate: float = 0.05) -> dict:
    never_null = {"order_id", "batch_num"}
    for key in record:
        if key not in never_null and random.random() < null_rate:
            record[key] = None
    return record


def _inject_invalid_values(record: dict, rate: float = 0.03) -> dict:
    if random.random() < rate:
        record["unit_price"] = round(random.uniform(-50, -1), 2)
    if random.random() < rate:
        record["quantity"] = random.randint(-5, -1)
    if random.random() < rate:
        future = datetime.now() + timedelta(days=random.randint(1, 365))
        record["order_date"] = future.strftime("%Y-%m-%d %H:%M:%S")
    if random.random() < rate:
        record["order_status"] = random.choice(["UNKNOWN", "error", "N/A", ""])
    return record


def _inject_duplicates(records: list, dup_rate: float = 0.02) -> list:
    dupes = [
        record.copy()
        for record in records
        if random.random() < dup_rate
    ]
    combined = records + dupes
    random.shuffle(combined)
    return combined


def generate_batch(
    batch_num:      int,
    n_records:      int               = 1000,
    null_rate:      float             = 0.05,
    invalid_rate:   float             = 0.03,
    dup_rate:       float             = 0.02,
    reference_date: Optional[datetime] = None,
) -> pd.DataFrame:
    if reference_date is None:
        reference_date = datetime.now()

    records = [_base_order(batch_num, reference_date) for _ in range(n_records)]
    records = [_inject_nulls(r, null_rate) for r in records]
    records = [_inject_invalid_values(r, invalid_rate) for r in records]
    records = _inject_duplicates(records, dup_rate)

    df = pd.DataFrame(records)

    print(
        f"[Batch {batch_num}] Generated {len(df)} records "
        f"| Columns: {list(df.columns)} "
        f"| Nulls @ {null_rate:.0%} "
        f"| Dupes @ {dup_rate:.0%}"
    )
    return df


def generate_all_batches(
    n_batches: int = 6,
    n_records: int = 1000,
) -> dict:
    base_date = datetime(2024, 1, 1)
    batches   = {}

    for i in range(1, n_batches + 1):
        ref_date     = base_date + timedelta(days=30 * (i - 1))
        null_rate    = 0.03 + (i * 0.005)
        invalid_rate = 0.02 + (i * 0.003)
        batches[i]   = generate_batch(
            batch_num      = i,
            n_records      = n_records,
            null_rate      = null_rate,
            invalid_rate   = invalid_rate,
            reference_date = ref_date,
        )

    return batches


if __name__ == "__main__":
    import os

    # Always resolve path relative to project root, not where the script lives
    # This is the correct pattern for any production pipeline script
    PROJECT_ROOT = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )
    output_dir = os.path.join(PROJECT_ROOT, "data", "raw")
    os.makedirs(output_dir, exist_ok=True)

    batches = generate_all_batches(n_batches=6, n_records=1000)

    for batch_num, df in batches.items():
        path = f"{output_dir}/orders_batch_{batch_num:02d}.csv"
        df.to_csv(path, index=False)
        print(f"  → Saved: {path}  ({len(df)} rows, {df.shape[1]} cols)")

    print("\n✅ Done. Data ready for Spark ingestion.")