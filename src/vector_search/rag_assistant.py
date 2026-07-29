"""
RAG Analytics Assistant
========================
Combines FAISS vector search with GPT-4o-mini to answer
questions about pipeline health using historical context.

RAG PATTERN (Interview Talking Point):
  Retrieval Augmented Generation solves the fundamental
  limitation of LLMs — they don't know YOUR data.

TWO RETRIEVAL SOURCES (Interview Talking Point):
  This assistant searches two separate FAISS indexes, gated by
  separate keyword heuristics, and merges whatever comes back into
  one prompt:
    - dq_incidents  (faiss_indexer.py)     — "has this happened before"
    - dq_metadata   (metadata_indexer.py)  — "what does this column mean"
  They're kept as separate indexes on purpose — see metadata_indexer.py's
  module docstring for why. A question can trigger either, both, or
  neither; force_retrieval forces both.
"""

import os
import sys
import json

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from dotenv import load_dotenv
from openai import OpenAI
from vector_search.faiss_indexer import (
    build_faiss_index,
    search_similar_incidents,
    embed_text
)
from vector_search.metadata_indexer import (
    build_metadata_index,
    search_similar_columns
)
from logging_config import get_logger

logger = get_logger(__name__)

load_dotenv()


def get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found.")
    return OpenAI(api_key=api_key)


RAG_SYSTEM_PROMPT = """You are an expert data engineering analytics
assistant with deep knowledge of data quality monitoring.

You have access to two kinds of grounding context: historical DQ
incident reports from the user's pipeline, and schema metadata
describing the orders table's columns (a dbt-style data dictionary).
Use whichever is relevant — or both — to give specific, grounded
answers.

Guidelines:
- Reference specific dates and metrics from incident context
- Reference specific column names, types, and notes from schema context
- Connect current issues to historical patterns
- Give actionable recommendations based on past fixes
- If a null rate or missing column is explained by schema evolution
  (a column introduced partway through the batch history), say so
  rather than treating it as an anomaly
- If no relevant history or schema context exists, say so clearly
- Be concise but thorough
- Use technical language appropriate for senior engineers
"""


def needs_retrieval(question: str) -> bool:
    retrieval_keywords = [
        "before", "previous", "last time", "history",
        "past", "similar", "happened", "incident",
        "when did", "has this", "before this",
        "pattern", "recurring", "trend", "again"
    ]
    question_lower = question.lower()
    return any(kw in question_lower for kw in retrieval_keywords)


def needs_metadata_retrieval(question: str) -> bool:
    metadata_keywords = [
        "column", "columns", "schema", "field", "fields", "table",
        "data type", "datatype", "means", "mean", "represents",
        "definition", "dictionary", "nullable", "null rate for",
        "what is", "what does", "which columns", "primary key",
        "foreign key", "constraint", "introduced", "added"
    ]
    question_lower = question.lower()
    return any(kw in question_lower for kw in metadata_keywords)


def build_context_from_incidents(incidents: list) -> str:
    if not incidents:
        return "No similar historical incidents found."

    context_parts = ["HISTORICAL INCIDENT CONTEXT:"]
    context_parts.append("="*50)

    for i, incident in enumerate(incidents, 1):
        context_parts.append(
            f"\nIncident {i} "
            f"[{incident.get('similarity_label', 'similar').upper()}]:"
        )
        context_parts.append(
            f"  Date:         {incident.get('run_timestamp','unknown')[:19]}"
        )
        context_parts.append(
            f"  Pipeline:     {incident.get('pipeline_name','unknown')}"
        )
        context_parts.append(
            f"  Health Score: {incident.get('health_score',0)}/100"
        )
        context_parts.append(
            f"  Critical:     {incident.get('critical_count',0)}"
        )
        context_parts.append(
            f"  Warnings:     {incident.get('warning_count',0)}"
        )

        text = incident.get("text", "")
        if "Analysis:" in text:
            analysis = text.split("Analysis:")[-1].strip()[:600]
            context_parts.append(f"  Analysis:     {analysis}")

        context_parts.append(
            f"  Similarity:   {incident.get('similarity_score',0)}"
        )

    context_parts.append("\n" + "="*50)
    return "\n".join(context_parts)


def build_context_from_columns(columns: list) -> str:
    if not columns:
        return "No relevant schema metadata found."

    context_parts = ["SCHEMA METADATA CONTEXT:"]
    context_parts.append("="*50)

    for i, col in enumerate(columns, 1):
        context_parts.append(f"\nColumn {i}: {col.get('column_name','unknown')}")
        context_parts.append(f"  Table:        {col.get('model_name','unknown')}")
        context_parts.append(f"  Data type:    {col.get('data_type','unknown')}")
        context_parts.append(f"  Description:  {col.get('description','')}")

        meta = col.get("meta", {}) or {}
        if "introduced_in_batch" in meta:
            context_parts.append(
                f"  Introduced:   batch {meta['introduced_in_batch']} "
                f"(null in earlier batches by design)"
            )
        if meta.get("note"):
            context_parts.append(f"  Note:         {meta['note']}")

        tests = col.get("tests", []) or []
        if tests:
            test_names = [
                t if isinstance(t, str) else list(t.keys())[0]
                for t in tests
            ]
            context_parts.append(f"  Constraints:  {', '.join(test_names)}")

        context_parts.append(
            f"  Similarity:   {col.get('similarity_score',0)}"
        )

    context_parts.append("\n" + "="*50)
    return "\n".join(context_parts)


def ask_rag_assistant(
    question:        str,
    current_context: dict = None,
    top_k:           int  = 3,
    force_retrieval: bool = False
) -> dict:
    client = get_openai_client()

    use_incident_retrieval = force_retrieval or needs_retrieval(question)
    use_metadata_retrieval = force_retrieval or needs_metadata_retrieval(question)

    logger.info("Question: %s", question)
    logger.info("Incident retrieval needed: %s", use_incident_retrieval)
    logger.info("Metadata retrieval needed: %s", use_metadata_retrieval)

    retrieved_incidents = []
    incident_context_text = ""

    if use_incident_retrieval:
        logger.info("Loading FAISS incident index...")
        index, metadata = build_faiss_index(force_rebuild=False)

        if index.ntotal > 0:
            logger.info("Searching %d historical incidents...", index.ntotal)
            retrieved_incidents = search_similar_incidents(
                query    = question,
                index    = index,
                metadata = metadata,
                top_k    = top_k
            )
            incident_context_text = build_context_from_incidents(retrieved_incidents)
            logger.info("Retrieved %d incidents", len(retrieved_incidents))
        else:
            incident_context_text = "No historical incidents indexed yet."
            logger.info("No incidents in index")

    retrieved_columns = []
    metadata_context_text = ""

    if use_metadata_retrieval:
        logger.info("Loading FAISS schema metadata index...")
        try:
            meta_index, meta_records = build_metadata_index(force_rebuild=False)
        except FileNotFoundError as e:
            logger.warning("Schema metadata unavailable: %s", e)
            meta_index, meta_records = None, []

        if meta_index is not None and meta_index.ntotal > 0:
            logger.info("Searching %d schema columns...", meta_index.ntotal)
            retrieved_columns = search_similar_columns(
                query    = question,
                index    = meta_index,
                metadata = meta_records,
                top_k    = top_k
            )
            metadata_context_text = build_context_from_columns(retrieved_columns)
            logger.info("Retrieved %d columns", len(retrieved_columns))
        else:
            metadata_context_text = "No schema metadata indexed yet."
            logger.info("No columns in metadata index")

    current_state = ""
    if current_context:
        null_trend   = current_context.get(
            "null_rates", {}).get("trend_direction", "unknown")
        drift_count  = current_context.get(
            "schema_drift", {}).get("drift_count", 0)
        violations   = current_context.get(
            "rule_violations", {}).get("total_violations", 0)
        current_state = f"""
CURRENT PIPELINE STATE:
  Null rate trend:   {null_trend}
  Schema drift:      {drift_count} events
  Total violations:  {violations}
"""

    user_prompt = f"""
{incident_context_text}

{metadata_context_text}

{current_state}

USER QUESTION:
{question}

Please answer based on the context above. Reference specific
incidents by date, and specific columns by name, when relevant.
"""

    logger.info("Generating answer...")
    response = client.chat.completions.create(
        model       = os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        messages    = [
            {"role": "system", "content": RAG_SYSTEM_PROMPT},
            {"role": "user",   "content": user_prompt}
        ],
        temperature = 0.3,
        max_tokens  = 800,
    )

    answer      = response.choices[0].message.content
    tokens_used = response.usage.total_tokens
    cost        = round(tokens_used * 0.00000015, 6)

    logger.info("Answer generated (%d tokens, ~$%s)", tokens_used, cost)

    return {
        "question":             question,
        "answer":               answer,
        "retrieved_incidents":  retrieved_incidents,
        "retrieved_columns":    retrieved_columns,
        "retrieval_used":       use_incident_retrieval,
        "metadata_retrieval_used": use_metadata_retrieval,
        "tokens_used":          tokens_used,
        "cost_usd":             cost
    }


def run_interactive_session():
    """CLI-only REPL for local testing — not called by the API or
    orchestrator, so this stays print()-driven human UX throughout,
    except the metrics-loading fallback below, which is a real
    degraded-state condition worth a log record regardless of UI context.
    """
    print("\n" + "="*60)
    print("  AI DATA QUALITY RAG ASSISTANT")
    print("="*60)
    print("Ask questions about your pipeline health or schema.")
    print("Type 'quit' to exit.\n")

    current_context = None
    try:
        from dq_metrics.spark_session import (
            create_spark_session,
            stop_spark_session
        )
        from llm_agent.metrics_reader import build_full_context
        print("Loading current pipeline metrics...")
        spark = create_spark_session(
            app_name="RAG-Assistant",
            env="local"
        )
        current_context = build_full_context(spark)
        stop_spark_session(spark)
        print("✅ Current metrics loaded\n")
    except Exception as e:
        logger.warning("Could not load current metrics: %s", e)
        print("   Continuing with historical context only\n")

    demo_questions = [
        "Has schema drift happened before in this pipeline?",
        "What was the worst health score we've seen?",
        "What does the discount_pct column mean and when was it added?",
    ]

    print("💡 Demo questions (or type your own):")
    for i, q in enumerate(demo_questions, 1):
        print(f"   {i}. {q}")
    print()

    while True:
        question = input("\n🤔 Your question: ").strip()

        if question.lower() in ("quit", "exit", "q"):
            print("\nGoodbye! 👋")
            break

        if not question:
            continue

        if question in ("1", "2", "3"):
            question = demo_questions[int(question) - 1]
            print(f"   → {question}")

        result = ask_rag_assistant(
            question         = question,
            current_context  = current_context,
            force_retrieval  = True
        )

        print("\n" + "─"*60)
        print("📖 ANSWER:")
        print("─"*60)
        print(result["answer"])
        print("─"*60)
        print(f"💰 Cost: ${result['cost_usd']} | "
              f"Tokens: {result['tokens_used']} | "
              f"Incidents: {len(result['retrieved_incidents'])} | "
              f"Columns: {len(result['retrieved_columns'])}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
        result   = ask_rag_assistant(
            question        = question,
            force_retrieval = True
        )
        print("\n" + "="*60)
        print("ANSWER:")
        print("="*60)
        print(result["answer"])
        print(f"\nCost: ${result['cost_usd']}")
    else:
        run_interactive_session()