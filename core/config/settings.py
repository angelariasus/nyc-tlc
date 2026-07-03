"""
Central configuration for the NYC TLC Trip Record Data Lake.
Driven by environment variables and a .env file at the project root.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv

# ── Project root is 3 levels up from this file: core/config/settings.py
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

load_dotenv(PROJECT_ROOT / ".env", override=True)

# ── TLC CloudFront base URL (no trailing slash)
_TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
_TLC_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc"

# ── File name patterns for each vehicle type
TLC_FILE_PATTERNS: Dict[str, str] = {
    "yellow": "yellow_tripdata_{year}-{month:02d}.parquet",
    "green":  "green_tripdata_{year}-{month:02d}.parquet",
    "fhv":    "fhv_tripdata_{year}-{month:02d}.parquet",
    "hvfhv":  "fhvhv_tripdata_{year}-{month:02d}.parquet",
}


@dataclass
class Settings:
    # ── TLC Data Source
    TLC_DATA_URL:   str = os.getenv("TLC_DATA_URL", _TLC_BASE_URL)
    TLC_LOOKUP_URL: str = os.getenv("TLC_LOOKUP_URL", _TLC_LOOKUP_URL)
    TLC_FILE_PATTERNS: Dict[str, str] = field(
        default_factory=lambda: TLC_FILE_PATTERNS
    )

    # ── Download scope (can be overridden per notebook)
    YEARS: List[int] = field(
        default_factory=lambda: [
            int(y)
            for y in os.getenv("TLC_YEARS", "2024,2025").split(",")
            if y.strip()
        ]
    )
    MONTHS: List[int] = field(
        default_factory=lambda: list(range(1, 13))
    )
    VEHICLE_TYPES: List[str] = field(
        default_factory=lambda: os.getenv(
            "TLC_VEHICLE_TYPES", "yellow,green"
        ).split(",")
    )

    # ── MongoDB
    MONGO_HOST:     str = os.getenv("MONGO_HOST",     "mongodb")
    MONGO_PORT:     int = int(os.getenv("MONGO_PORT", "27017"))
    MONGO_USER:     str = os.getenv("MONGO_USER",     "admin")
    MONGO_PASSWORD: str = os.getenv("MONGO_PASSWORD", "password123")
    MONGO_AUTH_DB:  str = os.getenv("MONGO_AUTH_DB",  "admin")

    MONGO_DB_BRONZE: str = os.getenv("MONGO_DB_BRONZE", "tlc_bronze")
    MONGO_DB_SILVER: str = os.getenv("MONGO_DB_SILVER", "tlc_silver")
    MONGO_DB_GOLD:   str = os.getenv("MONGO_DB_GOLD",   "tlc_gold")
    MONGO_DB_AUDIT:  str = os.getenv("MONGO_DB_AUDIT",  "tlc_audit")

    # ── Paths
    PROJECT_ROOT: Path = PROJECT_ROOT
    DATA_DIR:     Path = PROJECT_ROOT / "data"
    RAW_DIR:      Path = PROJECT_ROOT / "data" / "raw"
    AUDIT_DIR:    Path = PROJECT_ROOT / "data" / "audit" / "executions"
    LOGS_DIR:     Path = PROJECT_ROOT / "logs"
    STATE_FILE:   Path = PROJECT_ROOT / "data" / ".state.json"

    # ── Spark
    SPARK_APP_NAME:       str = os.getenv("SPARK_APP_NAME", "TLC_Pipeline")
    SPARK_MASTER:         str = os.getenv("SPARK_MASTER",   "local[*]")
    SPARK_DRIVER_MEMORY:  str = os.getenv("SPARK_DRIVER_MEMORY",  "4g")
    SPARK_EXECUTOR_MEMORY: str = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")

    # ── HTTP
    HTTP_TIMEOUT:    int   = int(os.getenv("HTTP_TIMEOUT",    "300"))
    MAX_RETRIES:     int   = int(os.getenv("MAX_RETRIES",     "3"))
    BACKOFF_FACTOR:  float = float(os.getenv("BACKOFF_FACTOR", "2.0"))

    # ── Quality checks
    QUALITY_THRESHOLD: float = float(os.getenv("QUALITY_THRESHOLD", "0.05"))

    def mongo_uri(self) -> str:
        """Build the full MongoDB connection URI."""
        return (
            f"mongodb://{self.MONGO_USER}:{self.MONGO_PASSWORD}"
            f"@{self.MONGO_HOST}:{self.MONGO_PORT}/"
            f"?authSource={self.MONGO_AUTH_DB}"
        )

    def mongo_spark_uri(self, database: str, collection: str) -> str:
        """Build a Spark-compatible MongoDB URI for a specific collection."""
        return (
            f"mongodb://{self.MONGO_USER}:{self.MONGO_PASSWORD}"
            f"@{self.MONGO_HOST}:{self.MONGO_PORT}"
            f"/{database}.{collection}"
            f"?authSource={self.MONGO_AUTH_DB}"
        )


# Singleton loaded at import time
settings = Settings()
