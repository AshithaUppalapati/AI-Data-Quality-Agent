"""
RAG Analytics Assistant
========================
Combines FAISS vector search with GPT-4o-mini to answer
questions about pipeline health using historical context.

RAG PATTERN (Interview Talking Point):
  Retrieval Augmented Generation solves the fundamental
  limitation of LLMs — they don't know YOUR data.
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

You have access to historical DQ incident reports from
the user's pipeline. Use this context to give specific,
grounded answers about pipeline health and past incidents.

Guidelines:
- Reference specific dates and metrics from the context
- Connect current issues to historical patterns
- Give actionable recommendations based on past fixes
- If no relevant history exists, say so clearly
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


def ask_rag_assistant(
    question:      str,
    current_context: dict = None,
    top_k:         int    = 3,
    force_retrieval: bool = False
) -> dict:
    client      = get_openai_client()
    use_retrieval = force_retrieval or needs_retrieval(question)

    logger.info("Question: %s", question)
    logger.info("Retrieval needed: %s", use_retrieval)

    retrieved_incidents = []
    context_text        = ""

    if use_retrieval:
        logger.info("Loading FAISS index...")
        index, metadata = build_faiss_index(force_rebuild=False)

        if index.ntotal > 0:
            logger.info("Searching %d historical incidents...", index.ntotal)
            retrieved_incidents = search_similar_incidents(
                query    = question,
                index    = index,
                metadata = metadata,
                top_k    = top_k
            )
            context_text = build_context_from_incidents(retrieved_incidents)
            logger.info("Retrieved %d incidents", len(retrieved_incidents))
        else:
            context_text = "No historical incidents indexed yet."
            logger.info("No incidents in index")

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
{context_text}

{current_state}

USER QUESTION:
{question}

Please answer based on the historical context above.
Reference specific incidents by date when relevant.
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
        "question":           question,
        "answer":             answer,
        "retrieved_incidents": retrieved_incidents,
        "retrieval_used":     use_retrieval,
        "tokens_used":        tokens_used,
        "cost_usd":           cost
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
    print("Ask questions about your pipeline health.")
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
        "Have we had negative price issues before and how were they fixed?",
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
              f"Sources: {len(result['retrieved_incidents'])}")


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