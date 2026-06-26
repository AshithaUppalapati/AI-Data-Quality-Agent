# My Notes: Synthetic Order Data Generator

## What this is

I built this to generate fake e-commerce order data with schema changes
and data quality problems baked in, so I could test my AI-Native Data
Quality Agent against something realistic instead of clean toy data.

It tests: schema drift detection, DQ metrics, trend analysis, LLM-based
anomaly explanations, and ETL robustness.

## How it works (so I don't forget)

Four parts:

### 1. `_base_order()` — the clean record

Builds one order: metadata, product, pricing, customer, status, payment
method, batch number.

It also changes shape depending on batch number — this is my schema drift
simulation:

| Batch | What changes |
|-------|----------------|
| 1–2 | Just a `state` column |
| 3–4 | `state` splits into `shipping_state` + `billing_state` |
| 5+ | `discount_pct` column shows up |

I made this batch-driven, not random, so I get reproducible test runs —
I can write a test that says "batch 3 should have shipping_state" and it
won't flake.

### 2. The three corruption functions

- **`_inject_nulls()`** — blanks out non-critical fields. I protect
  `order_id` and `batch_num` from this — if I null my own keys I lose the
  ability to track what I corrupted, which defeats the point.
- **`_inject_invalid_values()`** — this is the important one. It makes
  values that pass schema validation but are still wrong: negative prices,
  negative quantities, future timestamps, bad statuses. The whole point is
  proving that schema validation isn't enough — I need business rule
  checks on top of it.
- **`_inject_duplicates()`** — duplicates a % of records and shuffles them
  in, to simulate Kafka at-least-once delivery / ETL retries.

### 3. `generate_batch()` — one batch

Order: make records → null → invalid values → duplicates → DataFrame →
print summary. This order matters — it's mimicking real ETL stages.

### 4. `generate_all_batches()` — the full time series

Loops `generate_batch()` across months, schema drifts as described above,
payment methods evolve, issues get worse over batches. Each batch → its
own CSV.

## How I run it

```python
from data_generator.generate_orders import generate_all_batches

generate_all_batches(
    n_batches=6,
    n_records_per_batch=1000,
    output_dir="data/orders"
)
```

Gives me:
```
data/orders/
    orders_batch_1.csv
    ...
    orders_batch_6.csv
```

## Things to remember about my own design (limitations)

- It's a closed loop — I wrote both the generator and the detector. This
  doesn't prove my DQ agent generalizes to corruption it wasn't built for.
  It only proves it catches what I designed it to catch. Don't oversell
  this to myself or in an interview.
- Corruption is injected independently/randomly per field. Real corruption
  is usually correlated — one broken upstream service causes a null AND
  an invalid value in the same batch, same root cause. I'm not modeling
  that, so my root-cause-analysis testing is weaker than it looks.
- This only works at small scale (1,000s of rows) because it builds
  everything in memory as Python objects before converting to a DataFrame.
  I haven't built it to scale — that's fine for this use case, but I
  shouldn't pretend it's production-grade if asked.

## Questions I should be able to answer without looking this up

**Why protect `order_id`/`batch_num` from nulling?**
They're my ground-truth keys. Corrupt them and I can't measure what I
corrupted.

**Why batch-driven schema drift instead of random?**
Reproducibility, and it's more realistic — real upstream schema changes
are usually a clean cutover at a point in time, not gradual randomness.

**Why do invalid values need to pass schema validation?**
Because that's the actual gap I'm testing for. Type-correct doesn't mean
business-correct. If I can't explain this clearly, I haven't understood
the point of my own generator.

**Why Kafka at-least-once for duplicates?**
At-least-once = no data loss, but duplicate processing is possible after a
retry. I should know this tradeoff cold, not just drop the term "Kafka."

**How would I scale this to 10M records?**
- Stream/write in chunks instead of holding the full batch in memory.
- Vectorize the injectors (NumPy/pandas masks) instead of looping rows.
- Switch to Parquet instead of CSV — smaller, faster, keeps types (CSV
  turns everything into strings, which actively hurts type-check testing).
- Batches are independent → trivially parallelizable across processes.
- Be straight about this: at 1,000 rows none of this was needed. The
  current code trades performance for simplicity because the original use
  case never required scale. Don't pretend it already scales.

**How would I test the DQ agent itself, not just feed it data?**
- I already know ground truth (I generated the corruption), so log it
  separately from the corrupted data.
- Test the agent's findings against that ground truth — precision/recall,
  not just "it produced output."
- Run it on a *clean* batch too and check it reports zero issues — false
  positives matter as much as catching real problems.
- For the LLM explanation part specifically: check factual correctness
  against ground truth, not just whether it sounds plausible. A fluent
  explanation pointing at the wrong root cause is still wrong.

## Self-check before an interview

If I can't explain *why* a design choice was made (not just *what* it
does), that's a gap. "What's wrong with your own design" is a fair
question and I should answer it before being asked.
