# ─────────────────────────────────────────────────────────
# AI Data Quality Agent — Docker Image
# Base: Python 3.11-slim (matches our venv exactly)
# ─────────────────────────────────────────────────────────

FROM python:3.11-slim

LABEL maintainer="Ashitha Uppalapati"
LABEL project="AI-Data-Quality-Agent"
LABEL version="4.0"

# ── System dependencies ───────────────────────────────────
# Java 17 required for PySpark
# procps for process management
# wget for downloading files
RUN apt-get update && apt-get install -y \
    default-jdk \
    procps \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── Java environment ──────────────────────────────────────
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── Spark environment ─────────────────────────────────────
# SPARK_LOCAL_IP fixes hostname resolution on all platforms
# No winutils needed on Linux ✅
ENV SPARK_LOCAL_IP=127.0.0.1
ENV PYTHONPATH=/app/src

# ── Working directory ─────────────────────────────────────
WORKDIR /app

# ── Install Python dependencies ───────────────────────────
# Copy requirements first for Docker layer caching
# If requirements don't change, this layer is cached
# and pip install doesn't re-run on every build
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Copy source code ──────────────────────────────────────
COPY src/ ./src/
COPY data/ ./data/

# ── Default command ───────────────────────────────────────
# Runs the full DQ agent pipeline
CMD ["python", "src/llm_agent/agent_orchestrator.py"]

# ── Expose API port ────────────────────────────────────────
EXPOSE 8000