"""
Pipeline Execution Audit System.

Tracks the full lifecycle of every pipeline run:
  - Start / end timestamps and duration
  - Input parameters
  - Records processed, written, and quarantined
  - Data quality check results (per rule)
  - Errors captured during processing

Execution reports are persisted as JSON files in data/audit/executions/
AND inserted into the MongoDB ``tlc_audit.pipeline_runs`` collection for
centralised, queryable audit history across all pipeline layers.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.audit.logger import setup_logger
from core.config.settings import settings

logger = setup_logger(
    "tlc.audit.control_manager",
    log_dir=settings.LOGS_DIR,
)


# ── Status Enums ──────────────────────────────────────────────────────────────

class ExecutionStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED  = "FAILED"


class QualityStatus(str, Enum):
    PASSED  = "PASSED"
    WARNING = "WARNING"
    FAILED  = "FAILED"


# ── Data Models ───────────────────────────────────────────────────────────────

@dataclass
class QualityCheckResult:
    check_id:        str
    check_name:      str
    dataset:         str
    status:          QualityStatus
    records_checked: int
    records_passed:  int
    records_failed:  int
    failure_rate:    float
    details:         Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_id":        self.check_id,
            "check_name":      self.check_name,
            "dataset":         self.dataset,
            "status":          self.status.value,
            "records_checked": self.records_checked,
            "records_passed":  self.records_passed,
            "records_failed":  self.records_failed,
            "failure_rate":    round(self.failure_rate, 6),
            "details":         self.details,
        }


@dataclass
class ExecutionRecord:
    pipeline_name:    str
    input_parameters: Dict[str, Any]
    execution_id:     str            = field(default_factory=lambda: str(uuid.uuid4())[:8])
    start_time:       datetime       = field(default_factory=datetime.now)
    end_time:         Optional[datetime] = None
    status:           ExecutionStatus = ExecutionStatus.RUNNING
    output_summary:   Dict[str, Any] = field(default_factory=dict)
    quality_checks:   List[QualityCheckResult] = field(default_factory=list)
    errors:           List[Dict[str, Any]]      = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_id":     self.execution_id,
            "pipeline_name":    self.pipeline_name,
            "status":           self.status.value,
            "start_time":       self.start_time.isoformat(),
            "end_time":         self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": (
                round((self.end_time - self.start_time).total_seconds(), 2)
                if self.end_time else None
            ),
            "input_parameters": self.input_parameters,
            "output_summary":   self.output_summary,
            "quality_checks":   [qc.to_dict() for qc in self.quality_checks],
            "quality_passed":   sum(
                1 for qc in self.quality_checks
                if qc.status == QualityStatus.PASSED
            ),
            "errors": self.errors,
        }


# ── ControlManager ────────────────────────────────────────────────────────────

class ControlManager:
    """
    Manages the lifecycle of a single pipeline execution.

    Typical usage
    -------------
    ::

        control = ControlManager("bronze_ingestion")
        execution = control.start({"years": [2024, 2025], "vehicle_type": "yellow"})

        control.log_quality_check(
            check_name="negative_fares",
            dataset="yellow_2025_01",
            records_checked=1_500_000,
            records_failed=320,
        )

        control.end(ExecutionStatus.SUCCESS, {"records_written": 1_499_680})
    """

    AUDIT_DIR: Path = settings.AUDIT_DIR

    def __init__(self, pipeline_name: str) -> None:
        self.pipeline_name = pipeline_name
        self.execution: Optional[ExecutionRecord] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self, input_parameters: Dict[str, Any]) -> ExecutionRecord:
        """Initialise a new execution record and log the start."""
        self.execution = ExecutionRecord(
            pipeline_name=self.pipeline_name,
            input_parameters=input_parameters,
        )
        logger.info(
            f"[AUDIT] Execution started | pipeline={self.pipeline_name} "
            f"id={self.execution.execution_id}"
        )
        return self.execution

    def end(
        self,
        status: ExecutionStatus,
        output_summary: Dict[str, Any],
    ) -> ExecutionRecord:
        """
        Close the active execution, compute duration, persist the report
        (JSON + MongoDB), and log the result.
        """
        if not self.execution:
            raise RuntimeError("No active execution. Call start() first.")

        self.execution.end_time      = datetime.now()
        self.execution.status        = status
        self.execution.output_summary = output_summary

        duration = (self.execution.end_time - self.execution.start_time).total_seconds()
        logger.info(
            f"[AUDIT] Execution finished | id={self.execution.execution_id} "
            f"status={status.value} duration={duration:.1f}s"
        )

        self._save_json()
        self._save_mongo()

        return self.execution

    # ── Quality Checks ────────────────────────────────────────────────────────

    def log_quality_check(
        self,
        check_name: str,
        dataset: str,
        records_checked: int,
        records_failed: int,
        details: Optional[Dict[str, Any]] = None,
        threshold: float | None = None,
    ) -> QualityCheckResult:
        """
        Register a data quality check result against the active execution.

        Parameters
        ----------
        check_name:
            Human-readable rule name (e.g. ``"negative_fares"``).
        dataset:
            Identifier of the dataset / file inspected.
        records_checked:
            Total records evaluated.
        records_failed:
            Records that did not pass the rule.
        details:
            Optional extra metadata (thresholds, sample values, etc.).
        threshold:
            Failure-rate boundary for WARNING (default: ``settings.QUALITY_THRESHOLD``).
            Records with ``failure_rate >= threshold`` are marked FAILED;
            ``0 < failure_rate < threshold`` → WARNING; ``0`` → PASSED.

        Returns
        -------
        QualityCheckResult
        """
        if not self.execution:
            raise RuntimeError("No active execution. Call start() first.")

        threshold = threshold if threshold is not None else settings.QUALITY_THRESHOLD
        records_passed = records_checked - records_failed
        failure_rate = (
            records_failed / records_checked if records_checked > 0 else 0.0
        )

        if failure_rate == 0.0:
            q_status = QualityStatus.PASSED
        elif failure_rate < threshold:
            q_status = QualityStatus.WARNING
        else:
            q_status = QualityStatus.FAILED

        result = QualityCheckResult(
            check_id        = f"{self.execution.execution_id}_{check_name}",
            check_name      = check_name,
            dataset         = dataset,
            status          = q_status,
            records_checked = records_checked,
            records_passed  = records_passed,
            records_failed  = records_failed,
            failure_rate    = failure_rate,
            details         = details or {},
        )

        self.execution.quality_checks.append(result)
        logger.info(
            f"[QUALITY] {check_name} | dataset={dataset} "
            f"status={q_status.value} failure_rate={failure_rate:.2%}"
        )
        return result

    # ── Error Logging ─────────────────────────────────────────────────────────

    def log_error(
        self,
        error_type: str,
        message: str,
        context: Dict[str, Any] | None = None,
    ) -> None:
        """Append a structured error to the active execution record."""
        if not self.execution:
            raise RuntimeError("No active execution. Call start() first.")

        entry = {
            "error_type":    error_type,
            "error_message": message,
            "context":       context or {},
            "timestamp":     datetime.now().isoformat(),
        }
        self.execution.errors.append(entry)
        logger.error(f"[ERROR] {error_type}: {message} | context={context}")

    # ── Reporting ─────────────────────────────────────────────────────────────

    def get_report(self) -> Dict[str, Any]:
        """Return the current execution report as a plain dict."""
        if not self.execution:
            return {"error": "No active execution."}
        return self.execution.to_dict()

    # ── Persistence ───────────────────────────────────────────────────────────

    def _save_json(self) -> None:
        """Persist the execution report as a JSON file (local backup)."""
        self.AUDIT_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{ts}_{self.execution.execution_id}_{self.pipeline_name}.json"
        path = self.AUDIT_DIR / filename
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(self.execution.to_dict(), fh, indent=2, ensure_ascii=False)
        logger.info(f"[AUDIT] Report saved → {path}")

    def _save_mongo(self) -> None:
        """
        Insert the execution report into ``tlc_audit.pipeline_runs`` in MongoDB.
        Fails silently with a warning so a Mongo connectivity issue never
        breaks the pipeline itself.
        """
        try:
            from pymongo import MongoClient

            client = MongoClient(settings.mongo_uri(), serverSelectionTimeoutMS=3000)
            db = client[settings.MONGO_DB_AUDIT]
            db["pipeline_runs"].insert_one(self.execution.to_dict())
            logger.info(
                f"[AUDIT] Report inserted into MongoDB "
                f"tlc_audit.pipeline_runs (id={self.execution.execution_id})"
            )
        except Exception as exc:  # pragma: no cover
            logger.warning(
                f"[AUDIT] Could not persist report to MongoDB: {exc}. "
                "Continuing without remote audit."
            )
