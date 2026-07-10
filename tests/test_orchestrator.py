import pytest
import sys
import os

# Add src/ to path
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "src")
))

from llm_agent.agent_orchestrator import combine_anomaly_reports

def test_combine_anomaly_reports_basic():
    rule_report = {
        "anomalies": {
            "critical": [{"description": "test critical", "details": {}}], 
            "warnings": [{"description": "test warning", "details": {}}], 
            "info": [{"description": "test info", "details": {}}]
        }
    }
    stat_report = {
        "anomalies": {
            "critical": [{"description": "stat critical", "stats": {}}], 
            "warnings": []
        }
    }
    result = combine_anomaly_reports(rule_report, stat_report)
    assert result["critical_count"] == 2
    assert result["warning_count"] == 1
    assert result["info_count"] == 1
    assert "details" in result["anomalies"]["critical"][1]


