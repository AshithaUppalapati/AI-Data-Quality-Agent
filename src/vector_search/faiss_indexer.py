"""
FAISS Vector Search — Historical Incident Indexer
==================================================
Indexes historical DQ reports as vectors and enables
similarity search for finding past similar incidents.

HOW IT WORKS (Interview Talking Point):
  1. EMBED: Convert anomaly text → numerical vectors
  2. INDEX: Store vectors in FAISS IndexFlatL2
  3. SEARCH: New anomaly → embed → find nearest vectors

WHY FAISS OVER OTHER OPTIONS:
  FAISS: runs locally, no external dependencies,
         perfect for portfolio project
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
from logging_config import get_logger

logger = get_logger(__name__)

load_dotenv()

REPORTS_DIR = os.path.join(PROJECT_ROOT, "data", "reports")
INDEX_DIR   = os.path.join(PROJECT_ROOT, "data", "faiss_index")
INDEX_PATH  = os.path.join(INDEX_DIR, "dq_incidents.index")
META_PATH   = os.path.join(INDEX_DIR, "dq_incidents_meta.pkl")

EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIM   = 1536


def get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found in .env")
    return OpenAI(api_key=api_key)


def embed_text(text: str, client: OpenAI) -> np.ndarray:
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
    response = client.embeddings.create(
        model = EMBEDDING_MODEL,
        input = texts
    )
    vectors = np.array(
        [item.embedding for item in response.data],
        dtype=np.float32
    )
    return vectors


def extract_incidents_from_report(report: dict) -> list:
    incidents = []
    report_path      = report.get("report_path", "unknown")
    run_timestamp    = report.get("run_timestamp", "unknown")
    pipeline_name    = report.get("pipeline_name", "unknown")
    health_score     = report.get("health_score", 0)

    anomaly_step = report.get("steps", {}).get("anomaly_detector", {})
    if anomaly_step.get("status") != "success":
        return incidents

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
    reports = []
    if not os.path.exists(REPORTS_DIR):
        logger.warning("No reports directory found: %s", REPORTS_DIR)
        return reports

    report_files = list(Path(REPORTS_DIR).glob("dq_report_*.json"))
    logger.info("Found %d reports", len(report_files))

    for path in report_files:
        with open(path, "r") as f:
            report = json.load(f)
            reports.append(report)

    return reports


def build_faiss_index(force_rebuild: bool = False) -> tuple:
    os.makedirs(INDEX_DIR, exist_ok=True)

    if not force_rebuild and \
       os.path.exists(INDEX_PATH) and \
       os.path.exists(META_PATH):
        logger.info("Loading existing index...")
        index    = faiss.read_index(INDEX_PATH)
        with open(META_PATH, "rb") as f:
            metadata = pickle.load(f)
        logger.info("Loaded index with %d vectors", index.ntotal)
        return index, metadata

    logger.info("Building new FAISS index...")
    client = get_openai_client()

    reports = load_all_reports()
    if not reports:
        logger.warning("No reports to index")
        index    = faiss.IndexFlatL2(EMBEDDING_DIM)
        metadata = []
        return index, metadata

    all_incidents = []
    for report in reports:
        incidents = extract_incidents_from_report(report)
        all_incidents.extend(incidents)

    if not all_incidents:
        logger.warning("No incidents extracted from reports")
        index    = faiss.IndexFlatL2(EMBEDDING_DIM)
        metadata = []
        return index, metadata

    logger.info("Embedding %d incidents...", len(all_incidents))

    texts   = [inc["text"] for inc in all_incidents]
    vectors = embed_texts(texts, client)

    logger.info("Vector shape: %s", vectors.shape)

    index = faiss.IndexFlatL2(EMBEDDING_DIM)
    index.add(vectors)

    metadata = all_incidents

    faiss.write_index(index, INDEX_PATH)
    with open(META_PATH, "wb") as f:
        pickle.dump(metadata, f)

    logger.info("Index built: %d vectors", index.ntotal)
    logger.info("Saved to %s", INDEX_DIR)

    return index, metadata


def search_similar_incidents(
    query:    str,
    index,
    metadata: list,
    top_k:    int = 3
) -> list:
    if index.ntotal == 0:
        return []

    client = get_openai_client()

    query_vector = embed_text(query, client)
    query_vector = query_vector.reshape(1, -1)

    distances, indices = index.search(query_vector, top_k)

    results = []
    for dist, idx in zip(distances[0], indices[0]):
        if idx == -1:
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


if __name__ == "__main__":
    print("="*60)
    print("FAISS VECTOR SEARCH — BUILD & TEST")
    print("="*60)

    print("\n📚 Step 1: Building FAISS index from reports...")
    index, metadata = build_faiss_index(force_rebuild=True)

    print(f"\n✅ Index built: {index.ntotal} vectors indexed")
    print(f"   Incidents indexed: {len(metadata)}")

    if index.ntotal == 0:
        print("\n⚠️  No reports found to index.")
        print("   Run agent_orchestrator.py first to generate reports.")
    else:
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