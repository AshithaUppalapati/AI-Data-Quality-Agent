# Sprint 2 — Rough Notes (will be cleaned up at end of sprint)

## Task 1 — Environment Setup

### Issues Hit & Fixes

**Issue 1: JAVA_HOME not picked up by venv terminal**
- Java was installed but existing terminal didn't inherit new env vars
- Fix: set manually with $env:JAVA_HOME = "..." in current session
- Permanent fix: SetEnvironmentVariable at User scope

**Issue 2: winutils.exe missing (HADOOP_HOME unset)**
- Spark on Windows requires winutils.exe to simulate Hadoop filesystem ops
- Error: "HADOOP_HOME and hadoop.home.dir are unset"
- Fix: downloaded winutils.exe + hadoop.dll from cdarlint/winutils repo
- Placed in C:\hadoop\bin\
- Set HADOOP_HOME = C:\hadoop

**Issue 3: hadoop.dll blocked by Windows Defender**
- Fix: Unblock-File -Path "C:\hadoop\bin\hadoop.dll"

### What's Working
- Java 17.0.19 (Eclipse Temurin)
- PySpark 3.5.1
- Delta Lake 3.2.0
- Spark session starts and stops cleanly

## Task 2 — Spark Session Factory

### What we built
src/dq_metrics/spark_session.py
- create_spark_session(app_name, env) 
- stop_spark_session(spark)

### Design: Factory Pattern
- Single source of truth for Spark config
- Environment-aware: local / test / databricks
- local[*] = use all CPU cores on machine
- shuffle.partitions=8 (right-sized for laptop)
- ui.enabled=false (no browser UI needed locally)

### Known Issue: Windows temp dir cleanup error
- Harmless — JVM deletes temp folder twice on shutdown
- Disappears in Docker/Linux

## Task 3 — DQ Metrics Job

### What we built
src/dq_metrics/dq_metrics_job.py
- 5 metric functions: null_rates, schema_fingerprint,
  duplicate_rate, rule_violations, volume_stats
- run_dq_metrics_job() orchestrator
- All metrics computed per batch, unioned across batches

### Key fix: Python 3.14 incompatibility
- PySpark 3.5.1 officially supports Python 3.8-3.11
- Rebuilt venv with Python 3.11.9
- Added --add-opens JVM flags for Java 17 module access
- Added pyarrow for Arrow-based DataFrame conversion

### Results confirmed
- Null rates ramping 3% → 6% across batches ✅
- Schema drift: 12 → 13 → 14 cols across batches ✅
- ~2% duplicate rate consistent with injection ✅
- Rule violations ramping 99 → 141 across batches ✅

## Task 4 — Delta Lake Storage

### What we built
src/dq_metrics/delta_writer.py
- write_metric_to_delta() — single metric writer
- write_all_metrics()     — orchestrates all 5 tables
- read_metric_from_delta() — reads back with pruning

### Storage structure
data/metrics/
  null_rates/    batch_num=1/ ... batch_num=6/
  schema_drift/  batch_num=1/ ... batch_num=6/
  dup_rates/     batch_num=1/ ... batch_num=6/
  violations/    batch_num=1/ ... batch_num=6/
  volume_stats/  batch_num=1/ ... batch_num=6/

### Key concepts confirmed working
- Partitioned by batch_num → partition pruning active
- Overwrite mode → clean reruns
- Delta format → ACID + time travel enabled
- Read-back verification → data contract test pattern

### Fix: sys.path needed both project root AND src/
- project root → for data/ paths
- src/         → for dq_metrics package imports

# Sprint 2 — PySpark DQ Metrics Pipeline

## What We Built
A production-grade data quality metrics pipeline using PySpark
and Delta Lake that ingests 6 monthly e-commerce order batches
and computes 5 categories of DQ metrics persisted to Delta Lake.

---

## Architecture
data/raw/orders_batch_01-06.csv   ← Bronze layer

↓

Spark Session Factory          ← environment-aware config

↓

DQ Metrics Job

- null_rates per column        ← trend detection

- schema_fingerprint           ← drift detection

- duplicate_rate               ← upstream retry detection

- rule_violations              ← business logic checks

- volume_stats                 ← pipeline health signal

↓

Delta Lake (data/metrics/)     ← Silver layer

partitioned by batch_num       ← partition pruning

↓

41 Unit Tests                  ← production confidence

---

## Key Design Decisions

### 1. Factory Pattern for SparkSession
Single source of truth for Spark configuration.
Environment-aware: local / test / databricks.
One flag change — completely different config.
Interview answer: "Centralizing Spark config prevents
drift across jobs and makes environment promotion trivial."

### 2. Why Spark Over Pandas
Pandas loads all data into memory on one machine.
Spark distributes processing across cores and nodes.
For DQ monitoring at scale (billions of rows), Spark
is the only viable option. We use it here on small data
to demonstrate production patterns.

### 3. Medallion Architecture
Bronze → raw CSVs (data/raw/)
Silver → computed metrics (data/metrics/)
Gold   → LLM insights and reports (Phase 2)
Each layer adds value and trust to the data.

### 4. Partitioning by batch_num
Physically splits data into folders per batch.
Enables partition pruning — Spark skips irrelevant data.
At scale: difference between reading 1GB vs 1TB.

### 5. Overwrite Mode (vs Append vs Merge)
Overwrite → clean reruns, no duplicates, loses history
Append    → preserves history, risks duplicates on rerun
Merge     → upsert, best of both worlds, Delta Lake only
We use Overwrite now. Production upgrade: MERGE on batch_num.

### 6. PyArrow for DataFrame Conversion
Python 3.11 + PySpark 3.5.1 + Java 17 combination
requires Arrow-based serialization to avoid pickle
recursion errors. Arrow uses zero-copy columnar format
that both Python and JVM can read directly.

---

## Environment Setup (Windows-Specific)

### Java 17 — Eclipse Temurin
Required for PySpark. Set JAVA_HOME permanently:
[System.Environment]::SetEnvironmentVariable("JAVA_HOME", "path", "User")

### winutils.exe — Hadoop Windows Utilities
Spark on Windows needs winutils.exe to simulate
Hadoop filesystem operations (file permissions, temp dirs).
Downloaded from cdarlint/winutils repo.
Set HADOOP_HOME = C:\hadoop

### Python 3.11.9
PySpark 3.5.1 officially supports Python 3.8-3.11.
Python 3.12+ causes pickle serialization incompatibilities.
Multiple Python versions coexist safely on Windows via
the py launcher (py -3.11).

### Java 17 Module Access (--add-opens)
Java 17 restricts reflective access by default.
PySpark uses reflection for serialization and memory mgmt.
Fix: --add-opens JVM flags in SparkSession config.

---

## Metrics Explained

### Null Rates
Formula: null_count / total_rows * 100
Why: Sudden spike = broken upstream JOIN or schema mismatch
Pattern: order_id always 0% (primary key protection)
Trend: Batch 1 avg ~3.5% → Batch 6 avg ~6% (degrading)

### Schema Fingerprint
Formula: sorted(column_names).join("|")
Why sorted: column order changes aren't real drift
Drift detected: Batch 1-2 = 12 cols, Batch 3-4 = 13 cols,
               Batch 5-6 = 14 cols

### Duplicate Rate
Formula: duplicate_order_ids / total_rows * 100
Why: Upstream retry storms, ETL re-runs
Always clarify: duplicate on order_id or composite key?

### Business Rule Violations
Checks: negative prices, negative quantities,
        future order dates, invalid status enums
Why senior-level: schema validation misses these entirely
Trend: Batch 1 = 99 violations → Batch 6 = 140 violations

### Volume Stats
Row counts + column counts per batch
Why: 50% row drop = missing data, 200% spike = duplication
Fastest signal of overall pipeline health

---

## Test Strategy

41 tests across 3 files.
Philosophy: test behavior not implementation.
Test data: minimal synthetic DataFrames per test,
           no file I/O, no dependencies on data/raw/.
Spark fixture: scope="session" — one session reused
               across all tests (startup is expensive).

---

## Interview Q&A

Q: Why PySpark over pandas for DQ monitoring?
A: Pandas loads everything into memory on one machine.
   PySpark distributes across cores and nodes and handles
   datasets that don't fit in memory. For enterprise DQ
   monitoring at scale, Spark is the only viable option.
   We demonstrate production patterns even on small data.

Q: What is partition pruning and why does it matter?
A: When a Delta table is partitioned by batch_num, data
   is physically stored in separate folders per batch.
   When you query batch_num=5, Spark reads only that folder
   and skips all others. At scale this is the difference
   between reading gigabytes vs terabytes.

Q: Why Delta Lake over plain Parquet?
A: Delta adds ACID transactions (no partial writes on
   failure), time travel (query historical versions),
   schema enforcement (rejects wrong-shaped data), and
   efficient MERGE operations for upserts. For a metrics
   store, time travel is critical for trend analysis.

Q: How do you handle schema drift in your pipeline?
A: We compute a schema fingerprint per batch — sorted
   column names joined as a pipe-delimited string.
   Comparing fingerprints across batches flags additions,
   removals, and renames. Sorting ensures column reordering
   doesn't trigger false positives.

Q: What would you improve in production?
A: Switch write mode from overwrite to MERGE on batch_num
   for full history preservation. Add streaming ingestion
   via Kafka for real-time DQ monitoring. Add alerting
   thresholds that trigger Slack/Jira notifications when
   null rates exceed baseline by more than 2 standard
   deviations.