"""
FAISS Vector Search — Schema Metadata Indexer
================================================
Indexes table/column metadata (a dbt-style schema.yml) as vectors so
the RAG assistant can answer schema-aware questions ("what does
discount_pct mean", "which columns were added partway through the
batches", "is order_id unique") the same way faiss_indexer.py lets it
answer incident-history questions.

WHY A SEPARATE INDEX FROM dq_incidents (Interview Talking Point):
  Incidents and schema metadata answer fundamentally different question
  types — history vs. structure — and change on different cadences.
  Incidents accumulate every pipeline run; schema metadata only changes
  when the table itself changes. Keeping them as two FAISS indexes means
  rebuilding one never touches the other, and rag_assistant.py decides
  per-question which one (or both) to search, instead of forcing every
  query through one undifferentiated index.

WHY schema.yml INSTEAD OF EMBEDDING sql_remediation.py's TABLE_SCHEMA
DIRECTLY:
  TABLE_SCHEMA is a prompt string, formatted for an LLM to read inline
  in a remediation prompt. This index needs one column = one retrievable
  chunk with a stable name/description/data_type/meta shape. schema.yml
  is that shape — and it happens to be exactly the format dbt itself
  uses to document models, so it's a legitimate artifact on its own,
  not just a workaround for not having dbt in this project.
"""

import os
import sys
import pickle

import yaml
import faiss

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

from vector_search.faiss_indexer import (
    get_openai_client,
    embed_text,
    embed_texts,
    EMBEDDING_DIM,
)
from logging_config import get_logger

logger = get_logger(__name__)

SCHEMA_DIR  = os.path.join(PROJECT_ROOT, "data", "schema")
SCHEMA_PATH = os.path.join(SCHEMA_DIR, "orders_schema.yml")
INDEX_DIR   = os.path.join(PROJECT_ROOT, "data", "faiss_index")
INDEX_PATH  = os.path.join(INDEX_DIR, "dq_metadata.index")
META_PATH   = os.path.join(INDEX_DIR, "dq_metadata_meta.pkl")


def load_schema_yml(path: str = SCHEMA_PATH) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"schema.yml not found at {path}. This is the metadata "
            f"source for the schema index — it must exist before the "
            f"index can be built."
        )
    with open(path, "r") as f:
        return yaml.safe_load(f)


def extract_columns_from_schema(schema: dict) -> list:
    """Flatten schema.yml into one retrievable chunk per column."""
    chunks = []

    for model in schema.get("models", []):
        model_name = model.get("name", "unknown")
        model_desc = " ".join(model.get("description", "").split())

        for col in model.get("columns", []):
            col_name  = col.get("name", "unknown")
            data_type = col.get("data_type", "unknown")
            desc      = " ".join(col.get("description", "").split())
            meta      = col.get("meta", {}) or {}
            tests     = col.get("tests", []) or []

            text_parts = [
                f"Table: {model_name}. Column: {col_name} ({data_type}).",
                desc,
            ]

            if "introduced_in_batch" in meta:
                text_parts.append(
                    f"Introduced in batch {meta['introduced_in_batch']}; "
                    f"null in earlier batches by design — expected schema "
                    f"evolution, not a data quality defect."
                )
            if meta.get("note"):
                text_parts.append(" ".join(meta["note"].split()))

            if tests:
                test_names = [
                    t if isinstance(t, str) else list(t.keys())[0]
                    for t in tests
                ]
                text_parts.append(f"Constraints: {', '.join(test_names)}.")

            chunks.append({
                "id":          f"{model_name}.{col_name}",
                "model_name":  model_name,
                "model_desc":  model_desc,
                "column_name": col_name,
                "data_type":   data_type,
                "description": desc,
                "meta":        meta,
                "tests":       tests,
                "text":        " ".join(text_parts),
            })

    return chunks


def build_metadata_index(force_rebuild: bool = False) -> tuple:
    os.makedirs(INDEX_DIR, exist_ok=True)

    if not force_rebuild and \
       os.path.exists(INDEX_PATH) and \
       os.path.exists(META_PATH):
        logger.info("Loading existing metadata index...")
        index = faiss.read_index(INDEX_PATH)
        with open(META_PATH, "rb") as f:
            metadata = pickle.load(f)
        logger.info("Loaded metadata index with %d vectors", index.ntotal)
        return index, metadata

    logger.info("Building new schema metadata index...")
    client = get_openai_client()

    schema  = load_schema_yml()
    columns = extract_columns_from_schema(schema)

    if not columns:
        logger.warning("No columns extracted from schema.yml")
        index    = faiss.IndexFlatL2(EMBEDDING_DIM)
        metadata = []
        return index, metadata

    logger.info("Embedding %d columns...", len(columns))
    texts   = [c["text"] for c in columns]
    vectors = embed_texts(texts, client)

    index = faiss.IndexFlatL2(EMBEDDING_DIM)
    index.add(vectors)
    metadata = columns

    faiss.write_index(index, INDEX_PATH)
    with open(META_PATH, "wb") as f:
        pickle.dump(metadata, f)

    logger.info("Metadata index built: %d vectors", index.ntotal)
    logger.info("Saved to %s", INDEX_DIR)

    return index, metadata


def search_similar_columns(
    query:    str,
    index,
    metadata: list,
    top_k:    int = 3
) -> list:
    if index.ntotal == 0:
        return []

    client = get_openai_client()
    query_vector = embed_text(query, client).reshape(1, -1)

    distances, indices = index.search(query_vector, top_k)

    results = []
    for dist, idx in zip(distances[0], indices[0]):
        if idx == -1:
            continue
        col = metadata[idx].copy()
        col["similarity_score"] = round(float(dist), 4)
        results.append(col)

    return results


if __name__ == "__main__":
    print("=" * 60)
    print("SCHEMA METADATA INDEX — BUILD & TEST")
    print("=" * 60)

    print("\n📚 Step 1: Building metadata index from schema.yml...")
    index, metadata = build_metadata_index(force_rebuild=True)

    print(f"\n✅ Index built: {index.ntotal} vectors indexed")
    print(f"   Columns indexed: {len(metadata)}")

    if index.ntotal == 0:
        print("\n⚠️  No schema.yml found, or no columns in it.")
        print(f"   Expected at: {SCHEMA_PATH}")
    else:
        print("\n🔍 Step 2: Testing similarity search...")

        test_queries = [
            "what does discount_pct mean",
            "which columns were added partway through the batches",
            "is order_id unique",
        ]

        for query in test_queries:
            print(f"\nQuery: '{query}'")
            results = search_similar_columns(
                query=query, index=index, metadata=metadata, top_k=2
            )
            for r in results:
                print(f"  → [{r['similarity_score']}] "
                      f"{r['model_name']}.{r['column_name']} "
                      f"({r['data_type']}): {r['description'][:80]}")

    print("\n" + "=" * 60)
    print("Metadata indexing complete")
    print("=" * 60)