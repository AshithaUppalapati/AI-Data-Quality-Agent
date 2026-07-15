import pytest
import sys
import os
import json

# Add src/api/ to path so `main` is importable, same convention as your
# other test files adding src/ to reach their target module.
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "src", "api")
))

from fastapi.testclient import TestClient
import main as main_module
from main import app

client = TestClient(app)


# ─────────────────────────────────────────────────────────────────────────
# /health
# ─────────────────────────────────────────────────────────────────────────

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "AI Data Quality Agent API"}


# ─────────────────────────────────────────────────────────────────────────
# /run-agent — mocked, never touches real Spark or OpenAI
# ─────────────────────────────────────────────────────────────────────────

def test_run_agent_success(monkeypatch):
    fake_result = {
        "pipeline_name": "E-commerce Orders Pipeline",
        "status": "completed",
        "health_score": 85,
        "health_status": "HEALTHY",
        "total_cost_usd": 0.001,
    }

    monkeypatch.setattr(main_module, "create_spark_session", lambda app_name: "FAKE_SPARK")
    monkeypatch.setattr(main_module, "stop_spark_session", lambda spark: None)
    monkeypatch.setattr(
        main_module, "run_dq_agent",
        lambda spark, pipeline_name: fake_result
    )

    response = client.post("/run-agent", json={})
    assert response.status_code == 200
    assert response.json() == fake_result


def test_run_agent_propagates_failure(monkeypatch):
    """
    This is the test that proves the earlier fix mattered: if run_dq_agent
    raises and the endpoint's except block doesn't re-raise, this endpoint
    would silently return null with a 200 instead of failing loudly.
    If you skipped adding `raise` in the except block, this test will fail
    — that failure is the point.
    """
    monkeypatch.setattr(main_module, "create_spark_session", lambda app_name: "FAKE_SPARK")
    monkeypatch.setattr(main_module, "stop_spark_session", lambda spark: None)

    def boom(spark, pipeline_name):
        raise RuntimeError("simulated pipeline failure")

    monkeypatch.setattr(main_module, "run_dq_agent", boom)

    no_raise_client = TestClient(app, raise_server_exceptions=False)
    response = no_raise_client.post("/run-agent", json={})
    assert response.status_code == 500


# ─────────────────────────────────────────────────────────────────────────
# /ask — mocked, never calls OpenAI or FAISS for real
# ─────────────────────────────────────────────────────────────────────────

def test_ask_without_current_context(monkeypatch):
    captured = {}

    def fake_ask_rag_assistant(question, current_context, top_k, force_retrieval):
        captured["current_context"] = current_context
        return {
            "question": question,
            "answer": "fake answer",
            "retrieved_incidents": [],
            "retrieval_used": True,
            "tokens_used": 10,
            "cost_usd": 0.0001,
        }

    monkeypatch.setattr(main_module, "ask_rag_assistant", fake_ask_rag_assistant)

    response = client.post("/ask", json={"question": "Has this happened before?"})
    assert response.status_code == 200
    body = response.json()
    assert body["answer"] == "fake answer"
    # default should skip building live pipeline context (no Spark spin-up)
    assert captured["current_context"] is None


def test_ask_with_current_context(monkeypatch):
    monkeypatch.setattr(main_module, "create_spark_session", lambda app_name: "FAKE_SPARK")
    monkeypatch.setattr(main_module, "stop_spark_session", lambda spark: None)
    monkeypatch.setattr(main_module, "build_full_context", lambda spark: {"fake": "context"})

    captured = {}

    def fake_ask_rag_assistant(question, current_context, top_k, force_retrieval):
        captured["current_context"] = current_context
        return {"question": question, "answer": "fake answer", "retrieved_incidents": [],
                 "retrieval_used": True, "tokens_used": 10, "cost_usd": 0.0001}

    monkeypatch.setattr(main_module, "ask_rag_assistant", fake_ask_rag_assistant)

    response = client.post("/ask", json={
        "question": "How healthy is the pipeline right now?",
        "include_current_context": True,
    })
    assert response.status_code == 200
    assert captured["current_context"] == {"fake": "context"}


# ─────────────────────────────────────────────────────────────────────────
# /reports and /reports/{report_id} — real file I/O against a tmp_path,
# never touches your actual data/reports/ directory
# ─────────────────────────────────────────────────────────────────────────

def _write_fake_report(reports_dir, report_id, **overrides):
    data = {
        "pipeline_name": "E-commerce Orders Pipeline",
        "run_timestamp": "2026-07-15T15:12:18.935957",
        "status": "completed",
        "steps": {},
        "total_cost_usd": 0.001,
        "health_score": 85,
        "health_status": "HEALTHY",
    }
    data.update(overrides)
    path = os.path.join(reports_dir, f"dq_report_{report_id}.json")
    with open(path, "w") as f:
        json.dump(data, f)
    return path


def test_list_reports_empty(monkeypatch, tmp_path):
    monkeypatch.setattr(main_module, "REPORTS_DIR", str(tmp_path))
    response = client.get("/reports")
    assert response.status_code == 200
    assert response.json() == []


def test_list_reports_returns_newest_first(monkeypatch, tmp_path):
    monkeypatch.setattr(main_module, "REPORTS_DIR", str(tmp_path))
    _write_fake_report(tmp_path, "20260710_120000", health_score=50)
    _write_fake_report(tmp_path, "20260715_150000", health_score=90)

    response = client.get("/reports")
    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    assert body[0]["id"] == "20260715_150000"
    assert body[1]["id"] == "20260710_120000"


def test_list_reports_skips_corrupted_file(monkeypatch, tmp_path):
    monkeypatch.setattr(main_module, "REPORTS_DIR", str(tmp_path))
    _write_fake_report(tmp_path, "20260715_150000")
    with open(os.path.join(tmp_path, "dq_report_broken.json"), "w") as f:
        f.write("{not valid json")

    response = client.get("/reports")
    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["id"] == "20260715_150000"


def test_get_report_success(monkeypatch, tmp_path):
    monkeypatch.setattr(main_module, "REPORTS_DIR", str(tmp_path))
    _write_fake_report(tmp_path, "20260715_150000", health_score=90)

    response = client.get("/reports/20260715_150000")
    assert response.status_code == 200
    assert response.json()["health_score"] == 90


def test_get_report_not_found(monkeypatch, tmp_path):
    monkeypatch.setattr(main_module, "REPORTS_DIR", str(tmp_path))
    response = client.get("/reports/does_not_exist")
    assert response.status_code == 404