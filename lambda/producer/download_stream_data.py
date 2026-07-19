"""
Auto-generated from 00_streaming_download.ipynb
"""
import sys
sys.path.insert(0, "/home/jovyan/work")


from src.paths import ensure_dirs, iter_download_targets, str_path
from core.config.settings import settings

# ── Historical scope (Bulk Batch for Bronze) ──────────────────────────────────
YEARS         = [2019, 2020, 2021, 2022, 2023, 2024, 2025]
MONTHS        = list(range(1, 13))  # 1-12 for all months
VEHICLE_TYPES = ["yellow", "green", "fhv", "hvfhv"]  # Add "fhv", "hvfhv" as needed
SKIP_EXISTING = True  # Set False to re-download existing files
# ─────────────────────────────────────────────────────────────────────────────

ensure_dirs()
print(f"Project root : {settings.PROJECT_ROOT}")
print(f"TLC base URL : {settings.TLC_DATA_URL}")
print(f"Years        : {YEARS}")
print(f"Vehicle types: {VEHICLE_TYPES}")

import requests
from pathlib import Path
from src.paths import TAXI_ZONE_LOOKUP

LOOKUP_URL = f"{settings.TLC_LOOKUP_URL}/taxi_zone_lookup.csv"

if not TAXI_ZONE_LOOKUP.exists():
    print(f"Downloading zone lookup → {TAXI_ZONE_LOOKUP}")
    resp = requests.get(LOOKUP_URL, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
    resp.raise_for_status()
    TAXI_ZONE_LOOKUP.parent.mkdir(parents=True, exist_ok=True)
    TAXI_ZONE_LOOKUP.write_bytes(resp.content)
    print(f"Saved {len(resp.content):,} bytes")
else:
    print(f"Zone lookup already present: {TAXI_ZONE_LOOKUP}")

import datetime

YEAR_2026 = 2026
VEHICLE_TYPES_2026 = ["yellow", "green", "fhv", "hvfhv"]  # match your streaming consumer

today = datetime.date.today()
# TLC publishes with ~2-month lag; probe up to current month
months_to_probe = list(range(1, today.month + 1)) if today.year == YEAR_2026 else list(range(1, 13))

print(f"Probing TLC CDN for {YEAR_2026} data ({len(months_to_probe)} month(s) to check)...")
print(f"Today: {today}")

downloaded_2026 = []

for vt in VEHICLE_TYPES_2026:
    for month in months_to_probe:
        filename = settings.TLC_FILE_PATTERNS[vt].format(year=YEAR_2026, month=month)
        url      = f"{settings.TLC_DATA_URL}/{filename}"

        # Determine destination path
        dest = settings.RAW_DIR / vt / filename
        dest.parent.mkdir(parents=True, exist_ok=True)

        if dest.exists() and SKIP_EXISTING:
            size_mb = dest.stat().st_size / 1_048_576
            print(f"  ✓ {filename} already present ({size_mb:.1f} MB) — skipping")
            downloaded_2026.append(dest)
            continue

        # Probe with HEAD request (zero bandwidth cost)
        try:
            head = requests.head(url, timeout=10, allow_redirects=True)
            if head.status_code == 200:
                print(f"  ↓ {filename} available — downloading...", end=" ")
                size = download_file(url, dest)
                print(f"OK ({size / 1_048_576:.1f} MB)")
                downloaded_2026.append(dest)
            elif head.status_code == 404:
                print(f"  ○ {filename} not yet published (404) — skipping")
            else:
                print(f"  ? {filename} unexpected status {head.status_code}")
        except requests.RequestException as exc:
            print(f"  ✗ {filename} network error: {exc}")

print(f"\n2026 files ready for Kafka streaming: {len(downloaded_2026)}")
for f in downloaded_2026:
    print(f"  → {f}")

from src.paths import RAW

print("File inventory by vehicle type:")
print("-" * 55)
grand_total_mb = 0
for vt in VEHICLE_TYPES + [v for v in VEHICLE_TYPES_2026 if v not in VEHICLE_TYPES]:
    files = sorted(RAW[vt].glob("*.parquet")) if RAW[vt].exists() else []
    total_mb = sum(f.stat().st_size for f in files) / 1_048_576
    grand_total_mb += total_mb
    print(f"{vt:10s}: {len(files):3d} files | {total_mb:>8,.1f} MB")

print("-" * 55)
print(f"{'TOTAL':10s}: {'':7} | {grand_total_mb:>8,.1f} MB")

