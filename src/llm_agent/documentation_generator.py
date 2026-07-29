"""
Documentation Generator
=========================
Generates a markdown data dictionary (docs/data_dictionary.md) from
schema.yml metadata plus live null-rate / schema-drift / rule-violation
history read straight from Delta Lake, using GPT-4o-mini to turn
structured facts into readable prose.

Standalone CLI script, same shape as sql_remediation.py: reads inputs,
calls the LLM once, writes output, prints a cost summary. Not wired
into the API or orchestrator — regenerate on demand when the schema or
pipeline changes, same way you'd run `dbt docs generate` by hand.

WHY THIS READS DELTA DIRECTLY INSTEAD OF THROUGH
metrics_reader.build_full_context() (Interview Talking Point):
  build_full_context() aggregates metrics into the shape the anomaly
  detector needs — a single current trend direction, a single current
  drift count. This generator wants the raw per-column, per-batch
  history across all 6 batches, to describe how each column actually
  behaved over time. That's closer to what dq_metrics_job.py's own
  verification block reads directly via read_metric_from_delta() than
  to what the anomaly detector consumes, so this script reads the same
  way: straight from Delta, one query per metric table, no aggregation
  in between.
"""

import os
import sys
from datetime import datetime

import yaml

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from dotenv import load_dotenv
from openai import OpenAI
from dq_metrics.spark_session import create_spark_session, stop_spark_session
from dq_metrics.delta_writer import read_metric_from_delta
from logging_config import get_logger

logger = get_logger(__name__)
load_dotenv()

SCHEMA_PATH = os.path.join(PROJECT_ROOT, "data", "schema", "orders_schema.yml")
DOCS_DIR    = os.path.join(PROJECT_ROOT, "docs")
OUTPUT_PATH = os.path.join(DOCS_DIR, "data_dictionary.md")


def get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found. Check your .env file.")
    return OpenAI(api_key=api_key)


def load_schema() -> dict:
    if not os.path.exists(SCHEMA_PATH):
        raise FileNotFoundError(
            f"schema.yml not found at {SCHEMA_PATH}. This is the "
            f"metadata source the documentation is built from."
        )
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def collect_column_history(spark) -> dict:
    """Per-column null-rate history across all batches, plus schema
    drift and rule violation summaries, read straight from Delta —
    the same three metric tables dq_metrics_job.py itself writes and
    reads back for verification."""
    null_rates_df   = read_metric_from_delta(spark, "null_rates")
    schema_drift_df = read_metric_from_delta(spark, "schema_drift")
    violations_df   = read_metric_from_delta(spark, "violations")

    null_rates_rows   = [row.asDict() for row in null_rates_df.collect()]
    schema_drift_rows = [row.asDict() for row in schema_drift_df.collect()]
    violations_rows   = [row.asDict() for row in violations_df.collect()]

    by_column = {}
    for row in null_rates_rows:
        by_column.setdefault(row["column_name"], []).append({
            "batch_num":     row["batch_num"],
            "null_rate_pct": row["null_rate_pct"],
        })
    for col in by_column:
        by_column[col].sort(key=lambda r: r["batch_num"])

    return {
        "null_rates_by_column": by_column,
        "schema_drift": sorted(schema_drift_rows, key=lambda r: r["batch_num"]),
        "violations":   sorted(violations_rows, key=lambda r: r["batch_num"]),
    }


def build_documentation_prompt(schema: dict, history: dict) -> str:
    model = schema["models"][0]
    lines = [f"TABLE: {model['name']}", " ".join(model.get("description", "").split()), ""]

    lines.append("COLUMNS (from schema.yml):")
    for col in model.get("columns", []):
        meta       = col.get("meta", {}) or {}
        introduced = meta.get("introduced_in_batch")
        desc       = " ".join(col.get("description", "").split())
        line = f"- {col['name']} ({col.get('data_type', 'unknown')}): {desc}"
        if introduced:
            line += f" [introduced in batch {introduced}]"
        lines.append(line)

    lines.append("\nOBSERVED NULL RATES BY COLUMN, ACROSS ALL 6 BATCHES:")
    for col_name, rates in history["null_rates_by_column"].items():
        rate_str = ", ".join(
            f"batch {r['batch_num']}: {r['null_rate_pct']}%" for r in rates
        )
        lines.append(f"- {col_name}: {rate_str}")

    lines.append("\nSCHEMA DRIFT HISTORY (column count per batch):")
    for row in history["schema_drift"]:
        lines.append(f"- batch {row['batch_num']}: {row['col_count']} columns")

    lines.append("\nRULE VIOLATIONS BY BATCH:")
    for row in history["violations"]:
        lines.append(
            f"- batch {row['batch_num']}: {row['total_violations']} total "
            f"(negative_price={row['negative_price']}, "
            f"negative_quantity={row['negative_quantity']}, "
            f"future_dates={row['future_dates']}, "
            f"invalid_status={row['invalid_status']})"
        )

    return "\n".join(lines)


DOC_SYSTEM_PROMPT = """You are a data engineer writing a data dictionary
for a colleague who has never seen this table before.

Given the column definitions and observed data quality history below,
write a markdown data dictionary. For each column, state its purpose,
data type, and any notable data quality behavior — in particular, null
rate patterns. If a column's null rate is 100% for early batches and
0% afterward, and the input says that column was introduced partway
through the batch history, explain it as expected schema evolution —
do not call it an anomaly or a defect. Then add one short closing
section summarizing the table-level schema drift and rule violation
history across all 6 batches.

Use ## headers per column. Write in plain, precise prose. Do not
invent facts not present in the input.
"""


def generate_documentation(model: str = None) -> dict:
    client = get_openai_client()
    model  = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    schema = load_schema()

    spark = create_spark_session(app_name="Documentation-Generator")
    try:
        history = collect_column_history(spark)
    finally:
        stop_spark_session(spark)

    prompt = build_documentation_prompt(schema, history)

    logger.info("Generating documentation with %s...", model)
    response = client.chat.completions.create(
        model       = model,
        messages    = [
            {"role": "system", "content": DOC_SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        temperature = 0.2,
        max_tokens  = 1800,
    )

    doc_body    = response.choices[0].message.content
    tokens_used = response.usage.total_tokens
    cost        = round(tokens_used * 0.00000015, 6)

    header = (
        f"# Data Dictionary - {schema['models'][0]['name']}\n\n"
        f"_Auto-generated {datetime.now().isoformat(timespec='seconds')} "
        f"from `data/schema/orders_schema.yml` and live Delta Lake metrics. "
        f"Regenerate with `python src/llm_agent/documentation_generator.py` "
        f"after a schema or pipeline change - do not hand-edit._\n\n"
    )
    full_doc = header + doc_body

    logger.info("Documentation generated (%d tokens, ~$%s)", tokens_used, cost)

    return {
        "documentation": full_doc,
        "tokens_used":   tokens_used,
        "cost_usd":      cost,
        "model":         model,
    }


def write_documentation(result: dict) -> str:
    os.makedirs(DOCS_DIR, exist_ok=True)
    # encoding="utf-8" is required here, not optional: without it this
    # falls back to the OS default (cp1252 on Windows), which silently
    # mis-encodes any non-ASCII character (an em dash, a curly quote the
    # LLM writes on its own) into bytes that render as "?" wherever the
    # file is opened as UTF-8 afterward - a GitHub preview, most editors,
    # anything else that assumes UTF-8 by default.
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(result["documentation"])
    return OUTPUT_PATH


if __name__ == "__main__":
    print("=" * 60)
    print("DOCUMENTATION GENERATOR")
    print("=" * 60)

    print("\nStep 1: Loading schema.yml + Delta Lake history...")
    result = generate_documentation()

    print("\nStep 2: Writing docs/data_dictionary.md...")
    path = write_documentation(result)

    print(f"\n✅ Documentation written to {path}")
    print(f"   Model:  {result['model']}")
    print(f"   Tokens: {result['tokens_used']} (~${result['cost_usd']})")
    print("\n" + "=" * 60)