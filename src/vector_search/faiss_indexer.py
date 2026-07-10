"""
FAISS Vector Search — Historical Incident Indexer
==================================================
Indexes historical DQ reports as vectors and enables
similarity search for finding past similar incidents.

HOW IT WORKS (Interview Talking Point):
  1. EMBED: Convert anomaly text → numerical vectors
             using OpenAI text-embedding-3-small
             Each description becomes a 1536-dim vector

  2. INDEX: Store vectors in FAISS IndexFlatL2
             L2 = Euclidean distance
             Flat = exact search (no approximation)
             Good for: small-medium datasets (<100K vectors)

  3. SEARCH: New anomaly → embed → find nearest vectors
             Returns top-K most similar past incidents
             with similarity scores

WHY FAISS OVER OTHER OPTIONS:
  Pinecone/Weaviate: managed vector DBs, great for production
                     but require accounts and API keys
  FAISS:             runs locally, no external dependencies
                     perfect for portfolio project
                     same concepts apply to managed DBs

EMBEDDING MODEL CHOICE:
  text-embedding-3-small:
    1536 dimensions, fast, cheap ($0.00002/1K tokens)
    Perfect for semantic similarity on short texts

  text-embedding-3-large:
    3072 dimensions, more accurate, more expensive
    Use when precision > cost

💰 COST: ~$0.000002 per anomaly description embedded
         (~$0.0001 for a full report with 10 anomalies)
"""

import os
import sys
import json
import pickle
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from dotenv import load_dotenv
from openai import OpenAI
import faiss
import numpy as np

load_dotenv()

# ── Paths ─────────────────────────────────────────────────────────────────────
REPORTS_DIR = os.path.join(PROJECT_ROOT, "data", "reports")
INDEX_DIR   = os.path.join(PROJECT_ROOT, "data", "faiss_index")
INDEX_PATH  = os.path.join(INDEX_DIR, "dq_incidents.index")
META_PATH   = os.path.join(INDEX_DIR, "dq_incidents_meta.pkl")

# ── Embedding config ──────────────────────────────────────────────────────────
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIM   = 1536  # dimensions for text-embedding-3-small


# ─────────────────────────────────────────────────────────────────────────────
# 1. EMBEDDING FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found in .env")
    return OpenAI(api_key=api_key)


def embed_text(text: str, client: OpenAI) -> np.ndarray:
    """
    Convert text to embedding vector using OpenAI.

    💰 COST: ~$0.000002 per call (extremely cheap)

    Returns numpy array of shape (1536,)
    """
    response = client.embeddings.create(
        model = EMBEDDING_MODEL,
        input = text
    )
    vector = np.array(
        response.data[0].embedding,
        dtype=np.float32
    )
    return vector


def embed_texts(texts: list, client: OpenAI) -> np.ndarray:
    """
    Batch embed multiple texts.
    More efficient than calling embed_text() in a loop.

    Returns numpy array of shape (n_texts, 1536)
    """
    response = client.embeddings.create(
        model = EMBEDDING_MODEL,
        input = texts
    )
    vectors = np.array(
        [item.embedding for item in response.data],
        dtype=np.float32
    )
    return vectors


# ─────────────────────────────────────────────────────────────────────────────
# 2. REPORT PARSER
# ─────────────────────────────────────────────────────────────────────────────

def extract_incidents_from_report(report: dict) -> list:
    """
    Extract indexable incident descriptions from a DQ report.

    Each incident becomes one searchable entry in FAISS.
    We extract from anomaly_detector step output.

    WHY INDIVIDUAL ANOMALIES not full report:
      Searching at anomaly level gives more precise results.
      "find incidents similar to THIS specific anomaly"
      is more useful than "find reports similar to THIS report"
    """
    incidents = []
    report_path      = report.get("report_path", "unknown")
    run_timestamp    = report.get("run_timestamp", "unknown")
    pipeline_name    = report.get("pipeline_name", "unknown")
    health_score     = report.get("health_score", 0)

    # Get anomaly detector results
    anomaly_step = report.get("steps", {}).get("anomaly_detector", {})
    if anomaly_step.get("status") != "success":
        return incidents

    # Get full anomaly report from orchestrator output
    # We stored health_score and counts but not full anomaly list
    # So we reconstruct context from what we saved

    # Build one incident per report (simplified for now)
    # In production you'd store full anomaly list in report
    rca = report.get("steps", {}).get("root_cause_analyzer", {})
    analysis = rca.get("analysis", "")

    if analysis:
        incidents.append({
            "id":             f"{run_timestamp}",
            "report_path":    report_path,
            "run_timestamp":  run_timestamp,
            "pipeline_name":  pipeline_name,
            "health_score":   health_score,
            "critical_count": anomaly_step.get("critical", 0),
            "warning_count":  anomaly_step.get("warnings", 0),
            "text":           f"Pipeline: {pipeline_name}. "
                              f"Health: {health_score}/100. "
                              f"Critical: {anomaly_step.get('critical',0)}, "
                              f"Warnings: {anomaly_step.get('warnings',0)}. "
                              f"Analysis: {analysis[:500]}"
        })

    return incidents


def load_all_reports() -> list:
    """
    Load all JSON reports from data/reports/ directory.
    Returns list of report dicts.
    """
    reports = []
    if not os.path.exists(REPORTS_DIR):
        print(f"[FAISSIndexer] No reports directory found: {REPORTS_DIR}")
        return reports

    report_files = list(Path(REPORTS_DIR).glob("dq_report_*.json"))
    print(f"[FAISSIndexer] Found {len(report_files)} reports")

    for path in report_files:
        with open(path, "r") as f:
            report = json.load(f)
            reports.append(report)

    return reports


# ─────────────────────────────────────────────────────────────────────────────
# 3. FAISS INDEX BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_faiss_index(force_rebuild: bool = False) -> tuple:
    """
    Build FAISS index from all historical DQ reports.

    Args:
        force_rebuild: If True, rebuild even if index exists.

    Returns:
        (index, metadata) tuple

    FAISS INDEX TYPE — IndexFlatL2:
      Flat = stores all vectors, exact search
      L2   = Euclidean distance metric
      Good for: datasets under 100K vectors
      For larger: use IndexIVFFlat (approximate, faster)
    """
    os.makedirs(INDEX_DIR, exist_ok=True)

    # Return cached index if exists
    if not force_rebuild and \
       os.path.exists(INDEX_PATH) and \
       os.path.exists(META_PATH):
        print("[FAISSIndexer] Loading existing index...")
        index    = faiss.read_index(INDEX_PATH)
        with open(META_PATH, "rb") as f:
            metadata = pickle.load(f)
        print(f"[FAISSIndexer] Loaded index with "
              f"{index.ntotal} vectors")
        return index, metadata

    # Build fresh index
    print("[FAISSIndexer] Building new FAISS index...")
    client = get_openai_client()

    # Load all reports
    reports = load_all_reports()
    if not reports:
        print("[FAISSIndexer] No reports to index")
        # Return empty index
        index    = faiss.IndexFlatL2(EMBEDDING_DIM)
        metadata = []
        return index, metadata

    # Extract incidents from all reports
    all_incidents = []
    for report in reports:
        incidents = extract_incidents_from_report(report)
        all_incidents.extend(incidents)

    if not all_incidents:
        print("[FAISSIndexer] No incidents extracted from reports")
        index    = faiss.IndexFlatL2(EMBEDDING_DIM)
        metadata = []
        return index, metadata

    print(f"[FAISSIndexer] Embedding {len(all_incidents)} incidents...")

    # Embed all incident texts
    # 💰 COST: ~$0.000002 × n_incidents
    texts   = [inc["text"] for inc in all_incidents]
    vectors = embed_texts(texts, client)

    print(f"[FAISSIndexer] Vector shape: {vectors.shape}")

    # Build FAISS index
    index = faiss.IndexFlatL2(EMBEDDING_DIM)
    index.add(vectors)

    # Store metadata (text + report info) separately
    # FAISS only stores vectors, not the original text
    metadata = all_incidents

    # Persist index and metadata
    faiss.write_index(index, INDEX_PATH)
    with open(META_PATH, "wb") as f:
        pickle.dump(metadata, f)

    print(f"[FAISSIndexer] Index built: {index.ntotal} vectors")
    print(f"[FAISSIndexer] Saved to {INDEX_DIR}")

    return index, metadata


# ─────────────────────────────────────────────────────────────────────────────
# 4. SIMILARITY SEARCH
# ─────────────────────────────────────────────────────────────────────────────

def search_similar_incidents(
    query:    str,
    index,
    metadata: list,
    top_k:    int = 3
) -> list:
    """
    Search for incidents similar to a query text.

    Args:
        query:    Text description of current anomaly
        index:    FAISS index
        metadata: List of incident dicts (parallel to index)
        top_k:    Number of similar incidents to return

    Returns:
        List of similar incidents with similarity scores

    HOW SIMILARITY IS CALCULATED:
      1. Embed query → vector
      2. FAISS computes L2 distance to all stored vectors
      3. Returns indices of top_k closest vectors
      4. Lower L2 distance = more similar

    L2 DISTANCE INTERPRETATION:
      0.0       = identical
      < 0.5     = very similar
      0.5 - 1.0 = somewhat similar
      > 1.0     = different
    """
    if index.ntotal == 0:
        return []

    client = get_openai_client()

    # Embed the query
    query_vector = embed_text(query, client)
    query_vector = query_vector.reshape(1, -1)  # FAISS needs 2D array

    # Search
    distances, indices = index.search(query_vector, top_k)

    results = []
    for dist, idx in zip(distances[0], indices[0]):
        if idx == -1:  # FAISS returns -1 for empty slots
            continue
        incident = metadata[idx].copy()
        incident["similarity_score"] = round(float(dist), 4)
        incident["similarity_label"] = (
           "very similar"     if dist < 0.8
            else "similar"     if dist < 1.1
            else "related"     if dist < 1.5
            else "different"
        )
        results.append(incident)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# CLI — build and test the index
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("="*60)
    print("FAISS VECTOR SEARCH — BUILD & TEST")
    print("="*60)

    # Step 1 — Build index from historical reports
    print("\n📚 Step 1: Building FAISS index from reports...")
    index, metadata = build_faiss_index(force_rebuild=True)

    print(f"\n✅ Index built: {index.ntotal} vectors indexed")
    print(f"   Incidents indexed: {len(metadata)}")

    if index.ntotal == 0:
        print("\n⚠️  No reports found to index.")
        print("   Run agent_orchestrator.py first to generate reports.")
    else:
        # Step 2 — Test similarity search
        print("\n🔍 Step 2: Testing similarity search...")

        test_queries = [
            "schema drift detected — columns removed from pipeline",
            "null rate spiking on order_status column",
            "negative unit prices found in orders table",
        ]

        for query in test_queries:
            print(f"\nQuery: '{query}'")
            results = search_similar_incidents(
                query    = query,
                index    = index,
                metadata = metadata,
                top_k    = 2
            )
            if results:
                for r in results:
                    print(f"  → [{r['similarity_label']}] "
                          f"Score: {r['similarity_score']} | "
                          f"Health: {r['health_score']}/100 | "
                          f"Time: {r['run_timestamp'][:19]}")
            else:
                print("  → No similar incidents found")

    print("\n" + "="*60)
    print("FAISS indexing complete")
    print("="*60)