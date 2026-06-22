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

