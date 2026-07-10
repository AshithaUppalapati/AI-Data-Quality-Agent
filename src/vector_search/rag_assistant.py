"""
RAG Analytics Assistant
========================
Combines FAISS vector search with GPT-4o-mini to answer
questions about pipeline health using historical context.

RAG PATTERN (Interview Talking Point):
  Retrieval Augmented Generation solves the fundamental
  limitation of LLMs — they don't know YOUR data.

  Pure LLM:   "What caused schema drift in my pipeline?"
              → generic answer based on training data
              → not grounded in your actual history

  RAG:        1. Retrieve: find similar past incidents
                 from your FAISS index
              2. Augment:  add retrieved context to prompt
              3. Generate: LLM answers using YOUR history
              → specific, grounded, actionable answer

WHY THIS MATTERS FOR DATA ENGINEERING:
  Data pipelines have institutional knowledge —
  "every January we see null spikes because of
   the quarterly data refresh" — that LLMs don't have.

  RAG gives the LLM access to YOUR pipeline's history
  so answers improve over time as you accumulate reports.

TWO QUERY MODES:
  1. INCIDENT ANALYSIS: "what happened before with X?"
     → searches FAISS, returns grounded historical answer

  2. GENERAL QUESTION: "explain what schema drift is"
     → no retrieval needed, LLM answers directly
     → RAG system detects which mode to use
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

load_dotenv()


def get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found.")
    return OpenAI(api_key=api_key)


# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# QUERY CLASSIFIER
# ─────────────────────────────────────────────────────────────────────────────

def needs_retrieval(question: str) -> bool:
    """
    Determine if a question needs historical context retrieval.

    Questions about YOUR pipeline → need FAISS retrieval
    General knowledge questions  → answer directly

    Simple keyword-based classifier.
    In production: use LLM to classify.

    INTERVIEW NOTE:
      This is the routing layer of a RAG system.
      Production systems use more sophisticated routing —
      sometimes a separate LLM call to classify intent,
      or a fine-tuned classifier model.
    """
    retrieval_keywords = [
        "before", "previous", "last time", "history",
        "past", "similar", "happened", "incident",
        "when did", "has this", "before this",
        "pattern", "recurring", "trend", "again"
    ]
    question_lower = question.lower()
    return any(kw in question_lower for kw in retrieval_keywords)


# ─────────────────────────────────────────────────────────────────────────────
# CONTEXT BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_context_from_incidents(incidents: list) -> str:
    """
    Format retrieved incidents as readable context for LLM.

    Good context formatting is critical for RAG quality.
    We structure it clearly so LLM can reference specific
    incidents without confusion.
    """
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

        # Include analysis snippet
        text = incident.get("text", "")
        if "Analysis:" in text:
            analysis = text.split("Analysis:")[-1].strip()[:600]
            context_parts.append(f"  Analysis:     {analysis}")

        context_parts.append(
            f"  Similarity:   {incident.get('similarity_score',0)}"
        )

    context_parts.append("\n" + "="*50)
    return "\n".join(context_parts)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN RAG FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def ask_rag_assistant(
    question:      str,
    current_context: dict = None,
    top_k:         int    = 3,
    force_retrieval: bool = False
) -> dict:
    """
    Ask the RAG assistant a question about pipeline health.

    Args:
        question:        Natural language question
        current_context: Current pipeline metrics context
                         from build_full_context() (optional)
        top_k:           Number of similar incidents to retrieve
        force_retrieval: Always retrieve even for general questions

    Returns:
        dict with answer, retrieved incidents, cost info

    💰 COST:
      Embedding query:  ~$0.000002
      LLM answer:       ~$0.0002-0.0005
      Total per query:  ~$0.0003
    """
    client      = get_openai_client()
    use_retrieval = force_retrieval or needs_retrieval(question)

    print(f"\n[RAGAssistant] Question: {question}")
    print(f"[RAGAssistant] Retrieval needed: {use_retrieval}")

    retrieved_incidents = []
    context_text        = ""

    # ── Retrieval step ────────────────────────────────────────────────────────
    if use_retrieval:
        print("[RAGAssistant] Loading FAISS index...")
        index, metadata = build_faiss_index(force_rebuild=False)

        if index.ntotal > 0:
            print(f"[RAGAssistant] Searching {index.ntotal} "
                  f"historical incidents...")
            retrieved_incidents = search_similar_incidents(
                query    = question,
                index    = index,
                metadata = metadata,
                top_k    = top_k
            )
            context_text = build_context_from_incidents(
                retrieved_incidents
            )
            print(f"[RAGAssistant] Retrieved "
                  f"{len(retrieved_incidents)} incidents")
        else:
            context_text = "No historical incidents indexed yet."
            print("[RAGAssistant] No incidents in index")

    # ── Build prompt ──────────────────────────────────────────────────────────
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

    # ── LLM generation ────────────────────────────────────────────────────────
    print("[RAGAssistant] Generating answer...")
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

    print(f"[RAGAssistant] Answer generated "
          f"({tokens_used} tokens, ~${cost})")

    return {
        "question":           question,
        "answer":             answer,
        "retrieved_incidents": retrieved_incidents,
        "retrieval_used":     use_retrieval,
        "tokens_used":        tokens_used,
        "cost_usd":           cost
    }


# ─────────────────────────────────────────────────────────────────────────────
# INTERACTIVE SESSION
# ─────────────────────────────────────────────────────────────────────────────

def run_interactive_session():
    """
    Run an interactive RAG assistant session.
    User can ask multiple questions in sequence.
    """
    print("\n" + "="*60)
    print("  AI DATA QUALITY RAG ASSISTANT")
    print("="*60)
    print("Ask questions about your pipeline health.")
    print("Type 'quit' to exit.\n")

    # Load current context if available
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
        print(f"⚠️  Could not load current metrics: {e}")
        print("   Continuing with historical context only\n")

    # Sample questions to demonstrate
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

        # Check if user typed a number for demo question
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


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Single question mode
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
        # Interactive session
        run_interactive_session()