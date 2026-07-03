"""
Path resolver for the TLC Data Lake.
Maps vehicle types and years/months to raw data file paths.
Provides Spark-safe string paths (forward slashes).
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterator, List, Optional

from core.config.settings import settings

# ── Root directories ──────────────────────────────────────────────────────────

PROJECT_ROOT: Path = settings.PROJECT_ROOT
DATA_ROOT:    Path = settings.DATA_DIR

RAW = {
    "yellow": DATA_ROOT / "raw" / "yellow",
    "green":  DATA_ROOT / "raw" / "green",
    "fhv":    DATA_ROOT / "raw" / "fhv",
    "hvfhv":  DATA_ROOT / "raw" / "hvfhv",
    "lookup": DATA_ROOT / "raw" / "lookup",
}

AUDIT_DIR: Path = settings.AUDIT_DIR
STATE_FILE: Path = settings.STATE_FILE
LOGS_DIR:   Path = settings.LOGS_DIR

TAXI_ZONE_LOOKUP: Path = RAW["lookup"] / "taxi_zone_lookup.csv"


def str_path(p: Path) -> str:
    """Return a Spark-safe path string (forward slashes on all platforms)."""
    return str(p).replace("\\", "/")


def ensure_dirs() -> None:
    """Create all required data directories if they don't exist."""
    for path in RAW.values():
        path.mkdir(parents=True, exist_ok=True)
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


# ── File path resolution ──────────────────────────────────────────────────────

def raw_file_path(vehicle_type: str, year: int, month: int) -> Path:
    """
    Return the expected local path for a raw TLC Parquet file.

    Example
    -------
    ::

        raw_file_path("yellow", 2025, 3)
        # → data/raw/yellow/yellow_tripdata_2025-03.parquet
    """
    pattern = settings.TLC_FILE_PATTERNS[vehicle_type]
    filename = pattern.format(year=year, month=month)
    return RAW[vehicle_type] / filename


def raw_file_url(vehicle_type: str, year: int, month: int) -> str:
    """Return the CloudFront download URL for a raw TLC Parquet file."""
    pattern = settings.TLC_FILE_PATTERNS[vehicle_type]
    filename = pattern.format(year=year, month=month)
    return f"{settings.TLC_DATA_URL}/{filename}"


def list_raw_files(
    vehicle_type: str,
    years: Optional[List[int]] = None,
) -> List[Path]:
    """
    List all downloaded raw Parquet files for a given vehicle type.

    Parameters
    ----------
    vehicle_type:
        One of ``"yellow"``, ``"green"``, ``"fhv"``, ``"hvfhv"``.
    years:
        Optional year filter. Returns files for all years when ``None``.
    """
    base = RAW.get(vehicle_type)
    if base is None or not base.exists():
        return []

    files = sorted(base.glob("*.parquet"))
    if years:
        files = [
            f for f in files
            if any(str(y) in f.name for y in years)
        ]
    return files


def iter_download_targets(
    vehicle_types: Optional[List[str]] = None,
    years: Optional[List[int]] = None,
    months: Optional[List[int]] = None,
    skip_existing: bool = True,
) -> Iterator[tuple[str, int, int, str, Path]]:
    """
    Yield ``(vehicle_type, year, month, url, local_path)`` tuples for
    all requested file combinations.

    Parameters
    ----------
    vehicle_types:
        Defaults to ``settings.VEHICLE_TYPES``.
    years:
        Defaults to ``settings.YEARS``.
    months:
        Defaults to all 12 months.
    skip_existing:
        When ``True`` (default), skips files that are already on disk.

    Yields
    ------
    tuple[str, int, int, str, Path]
        ``(vehicle_type, year, month, download_url, local_path)``
    """
    vehicle_types = vehicle_types or settings.VEHICLE_TYPES
    years         = years         or settings.YEARS
    months        = months        or list(range(1, 13))

    for vt in vehicle_types:
        for year in years:
            for month in months:
                path = raw_file_path(vt, year, month)
                url  = raw_file_url(vt, year, month)
                if skip_existing and path.exists():
                    continue
                yield vt, year, month, url, path
