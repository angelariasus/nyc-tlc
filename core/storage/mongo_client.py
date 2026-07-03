"""
MongoDB connection factory for the TLC Data Lake.
Provides a singleton-style client and per-layer database accessors.
"""
from __future__ import annotations

from typing import Optional

from pymongo import MongoClient
from pymongo.database import Database

from core.audit.logger import setup_logger
from core.config.settings import settings

logger = setup_logger("tlc.storage.mongo_client")

_client: Optional[MongoClient] = None


def get_client() -> MongoClient:
    """Return a cached MongoClient, creating one if necessary."""
    global _client
    if _client is None:
        _client = MongoClient(
            settings.mongo_uri(),
            serverSelectionTimeoutMS=5000,
        )
        logger.info(
            f"[MONGO] Connected to {settings.MONGO_HOST}:{settings.MONGO_PORT}"
        )
    return _client


def get_bronze_db() -> Database:
    return get_client()[settings.MONGO_DB_BRONZE]


def get_silver_db() -> Database:
    return get_client()[settings.MONGO_DB_SILVER]


def get_gold_db() -> Database:
    return get_client()[settings.MONGO_DB_GOLD]


def get_audit_db() -> Database:
    return get_client()[settings.MONGO_DB_AUDIT]


def quarantine_record(
    record: dict,
    run_id: str,
    pipeline: str,
    reason: str,
) -> None:
    """
    Insert a rejected record into the ``tlc_audit.quarantine`` collection.

    Parameters
    ----------
    record:
        The raw document that failed a quality rule.
    run_id:
        The ``execution_id`` of the pipeline run that rejected it.
    pipeline:
        Name of the pipeline stage (e.g. ``"silver_yellow"``).
    reason:
        Human-readable explanation of why the record was rejected.
    """
    from datetime import datetime

    doc = {
        "quarantined_at": datetime.utcnow(),
        "run_id":         run_id,
        "pipeline":       pipeline,
        "reason":         reason,
        "raw_record":     record,
    }
    get_audit_db()["quarantine"].insert_one(doc)
