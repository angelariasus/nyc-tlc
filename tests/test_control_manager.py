"""
Unit tests for the ControlManager audit framework.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.audit.control_manager import (
    ControlManager,
    ExecutionStatus,
    QualityStatus,
)


@pytest.fixture
def control(tmp_path):
    """ControlManager with a temporary audit directory."""
    with patch("core.audit.control_manager.settings") as mock_settings:
        mock_settings.AUDIT_DIR = tmp_path / "audit"
        mock_settings.LOGS_DIR  = tmp_path / "logs"
        mock_settings.QUALITY_THRESHOLD = 0.05
        mock_settings.mongo_uri.return_value = "mongodb://localhost:27017/"
        mock_settings.MONGO_DB_AUDIT = "tlc_audit"

        cm = ControlManager("test_pipeline")
        cm.AUDIT_DIR = tmp_path / "audit"
        yield cm


def test_start_creates_execution(control):
    execution = control.start({"years": [2024]})
    assert execution.pipeline_name == "test_pipeline"
    assert execution.status == ExecutionStatus.RUNNING
    assert execution.execution_id is not None


def test_end_sets_status_and_summary(control):
    control.start({"years": [2024]})
    control.end(ExecutionStatus.SUCCESS, {"records": 1000})
    report = control.get_report()
    assert report["status"] == "SUCCESS"
    assert report["output_summary"]["records"] == 1000
    assert report["duration_seconds"] is not None


def test_quality_check_passed(control):
    control.start({})
    result = control.log_quality_check(
        check_name="no_nulls",
        dataset="test_ds",
        records_checked=1000,
        records_failed=0,
    )
    assert result.status == QualityStatus.PASSED
    assert result.failure_rate == 0.0


def test_quality_check_warning(control):
    control.start({})
    result = control.log_quality_check(
        check_name="sparse_check",
        dataset="test_ds",
        records_checked=1000,
        records_failed=30,  # 3% < 5% threshold
    )
    assert result.status == QualityStatus.WARNING


def test_quality_check_failed(control):
    control.start({})
    result = control.log_quality_check(
        check_name="critical_check",
        dataset="test_ds",
        records_checked=1000,
        records_failed=100,  # 10% >= 5% threshold
    )
    assert result.status == QualityStatus.FAILED


def test_log_error_appended(control):
    control.start({})
    control.log_error("TestError", "Something went wrong", {"context": "abc"})
    report = control.get_report()
    assert len(report["errors"]) == 1
    assert report["errors"][0]["error_type"] == "TestError"


def test_audit_json_saved(control, tmp_path):
    with patch.object(control, "_save_mongo"):  # skip Mongo in unit tests
        control.start({"test": True})
        control.end(ExecutionStatus.SUCCESS, {})

    audit_files = list((tmp_path / "audit").glob("*.json"))
    assert len(audit_files) == 1

    report = json.loads(audit_files[0].read_text())
    assert report["pipeline_name"] == "test_pipeline"
    assert report["status"] == "SUCCESS"
