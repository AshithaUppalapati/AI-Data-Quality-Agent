import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import sys
# Add project root to path
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
))
# Add src/ to path so dq_metrics package is importable
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))
import json
import glob
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dq_metrics.spark_session import create_spark_session, stop_spark_session
from llm_agent.agent_orchestrator import run_dq_agent, REPORTS_DIR
from llm_agent.metrics_reader import build_full_context
from vector_search.rag_assistant import ask_rag_assistant
from typing import Optional

class RunAgentRequest(BaseModel):
    pipeline_name: str = "E-commerce Orders Pipeline"
    recent_batches: Optional[int] = None
class AskRequest(BaseModel):
    question: str
    top_k: int = 3
    force_retrieval: bool = False
    include_current_context: bool = False

app = FastAPI()

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "AI Data Quality Agent API"}

@app.post("/run-agent")
def run_agent(request: RunAgentRequest):
    try:
        spark = create_spark_session(app_name="DQ-Agent-API")
        return run_dq_agent(spark, pipeline_name=request.pipeline_name, recent_batches=request.recent_batches)
    except Exception as e:
        print(f"\n>>> ERROR CAUGHT: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        stop_spark_session(spark)

@app.post("/ask")
def ask(request: AskRequest):
    current_context = None
    spark = None
    try:
        if request.include_current_context:
            spark = create_spark_session(app_name="DQ-Agent-Ask")
            current_context = build_full_context(spark)

        return ask_rag_assistant(
            question=request.question,
            current_context=current_context,
            top_k=request.top_k,
            force_retrieval=request.force_retrieval
        )
    finally:
        if spark:
            stop_spark_session(spark)

@app.get("/reports")
def list_reports():
    if not os.path.isdir(REPORTS_DIR):
        return []

    files = sorted(
        glob.glob(os.path.join(REPORTS_DIR, "dq_report_*.json")),
        reverse=True  # newest first — timestamp format sorts correctly as a string
    )

    summaries = []
    for path in files:
        try:
            with open(path) as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue  # skip a corrupted/partial report rather than failing the whole list

        report_id = os.path.basename(path).removeprefix("dq_report_").removesuffix(".json")
        summaries.append({
            "id": report_id,
            "pipeline_name": data.get("pipeline_name"),
            "run_timestamp": data.get("run_timestamp"),
            "status": data.get("status"),
            "health_score": data.get("health_score"),
            "health_status": data.get("health_status"),
            "total_cost_usd": data.get("total_cost_usd"),
        })

    return summaries


@app.get("/reports/{report_id}")
def get_report(report_id: str):
    path = os.path.join(REPORTS_DIR, f"dq_report_{report_id}.json")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail=f"Report '{report_id}' not found")

    with open(path) as f:
        return json.load(f)
