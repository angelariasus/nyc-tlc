"""
Unit tests for TLC data quality rules.
Uses a lightweight in-memory PySpark session.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(scope="module")
def spark():
    """Minimal SparkSession for unit tests (no Mongo connector needed)."""
    import os
    import sys
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName("TLC_Unit_Tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture
def sample_yellow_df(spark):
    from pyspark.sql.types import (
        DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
    )
    from datetime import datetime

    schema = StructType([
        StructField("VendorID",                IntegerType()),
        StructField("tpep_pickup_datetime",    TimestampType()),
        StructField("tpep_dropoff_datetime",   TimestampType()),
        StructField("passenger_count",         IntegerType()),
        StructField("trip_distance",           DoubleType()),
        StructField("PULocationID",            IntegerType()),
        StructField("DOLocationID",            IntegerType()),
        StructField("fare_amount",             DoubleType()),
        StructField("total_amount",            DoubleType()),
        StructField("RatecodeID",              IntegerType()),
        StructField("payment_type",            IntegerType()),
    ])

    t1 = datetime(2025, 1, 15, 10, 0, 0)
    t2 = datetime(2025, 1, 15, 10, 20, 0)
    t0 = datetime(2025, 1, 15, 9, 0, 0)  # before pickup (invalid)

    data = [
        # Valid record
        (1, t1, t2, 2, 4.5, 142, 230, 18.5, 24.0, 1, 1),
        # Invalid: zero distance
        (1, t1, t2, 1, 0.0, 100, 200, 10.0, 12.0, 1, 1),
        # Invalid: negative fare
        (1, t1, t2, 1, 3.0, 50, 60, -5.0, -5.0, 1, 1),
        # Invalid: dropoff before pickup
        (1, t1, t0, 1, 2.0, 75, 80, 8.0, 10.0, 1, 1),
        # Invalid: out-of-range location
        (1, t1, t2, 1, 1.0, 0, 266, 5.0, 6.0, 1, 1),
    ]

    return spark.createDataFrame(data, schema)


def test_valid_records_pass_all_rules(sample_yellow_df):
    from src.transformations.tlc_rules import YELLOW_GREEN_RULES, apply_rules

    valid_df, rejected_df = apply_rules(sample_yellow_df, YELLOW_GREEN_RULES)
    assert valid_df.count() == 1
    assert rejected_df.count() == 4


def test_rejection_reasons_populated(sample_yellow_df):
    from src.transformations.tlc_rules import YELLOW_GREEN_RULES, apply_rules

    _, rejected_df = apply_rules(sample_yellow_df, YELLOW_GREEN_RULES)
    reasons = [r["_rejection_reason"] for r in rejected_df.select("_rejection_reason").collect()]
    # Each rejected record must have a non-null reason
    assert all(r is not None for r in reasons)


def test_zero_distance_rejected(sample_yellow_df):
    from src.transformations.tlc_rules import YELLOW_GREEN_RULES, apply_rules

    _, rejected_df = apply_rules(sample_yellow_df, YELLOW_GREEN_RULES)
    zero_dist = rejected_df.filter(
        rejected_df["_rejection_reason"].contains("zero")
    ).count()
    assert zero_dist >= 1
