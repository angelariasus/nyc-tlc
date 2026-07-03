"""
Data quality filter rules for TLC trip records.

Each rule is a named predicate that can be applied to a PySpark DataFrame.
Invalid records are routed to the quarantine collection in MongoDB rather
than being silently dropped.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List, Tuple

from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F

from core.audit.logger import setup_logger

logger = setup_logger("tlc.transformations.tlc_rules")


# ── Rule definition ───────────────────────────────────────────────────────────

@dataclass
class Rule:
    name: str
    description: str
    condition: Callable[[DataFrame], Column]  # Returns a boolean Column (True = PASS)


# ── Yellow / Green shared rules ───────────────────────────────────────────────

YELLOW_GREEN_RULES: List[Rule] = [
    Rule(
        name="no_zero_distance",
        description="Trip distance must be greater than zero.",
        condition=lambda df: F.col("trip_distance") > 0,
    ),
    Rule(
        name="no_negative_fare",
        description="Fare amount must not be negative.",
        condition=lambda df: F.col("fare_amount") >= 0,
    ),
    Rule(
        name="no_negative_total",
        description="Total amount must not be negative.",
        condition=lambda df: F.col("total_amount") >= 0,
    ),
    Rule(
        name="valid_pickup_location",
        description="Pickup location ID must be in [1, 265].",
        condition=lambda df: (
            F.col("PULocationID").between(1, 265)
        ),
    ),
    Rule(
        name="valid_dropoff_location",
        description="Dropoff location ID must be in [1, 265].",
        condition=lambda df: (
            F.col("DOLocationID").between(1, 265)
        ),
    ),
    Rule(
        name="valid_time_order",
        description="Dropoff datetime must be after pickup datetime.",
        condition=lambda df: (
            F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")
        ),
    ),
    Rule(
        name="valid_passenger_count",
        description="Passenger count must be between 1 and 8.",
        condition=lambda df: (
            F.col("passenger_count").between(1, 8)
        ),
    ),
]

GREEN_EXTRA_RULES: List[Rule] = [
    Rule(
        name="valid_time_order_green",
        description="Dropoff datetime must be after pickup datetime (Green field names).",
        condition=lambda df: (
            F.col("lpep_dropoff_datetime") > F.col("lpep_pickup_datetime")
        ),
    ),
]

# ── FHV rules ─────────────────────────────────────────────────────────────────

FHV_RULES: List[Rule] = [
    Rule(
        name="valid_pickup_location",
        description="Pickup location ID must be in [1, 265].",
        condition=lambda df: (
            F.col("PULocationID").between(1, 265)
        ),
    ),
    Rule(
        name="valid_dropoff_location",
        description="Dropoff location ID must be in [1, 265].",
        condition=lambda df: (
            F.col("DOLocationID").between(1, 265)
        ),
    ),
    Rule(
        name="valid_time_order",
        description="Dropoff datetime must be after pickup datetime.",
        condition=lambda df: (
            F.col("dropoff_datetime") > F.col("pickup_datetime")
        ),
    ),
]

# ── HVFHV rules ───────────────────────────────────────────────────────────────

HVFHV_RULES: List[Rule] = [
    Rule(
        name="valid_pickup_location",
        description="Pickup location ID must be in [1, 265].",
        condition=lambda df: (
            F.col("PULocationID").between(1, 265)
        ),
    ),
    Rule(
        name="valid_dropoff_location",
        description="Dropoff location ID must be in [1, 265].",
        condition=lambda df: (
            F.col("DOLocationID").between(1, 265)
        ),
    ),
    Rule(
        name="valid_time_order",
        description="Dropoff datetime must be after pickup datetime.",
        condition=lambda df: (
            F.col("dropoff_datetime") > F.col("pickup_datetime")
        ),
    ),
    Rule(
        name="no_negative_pay",
        description="Driver pay and base passenger fare must not be negative.",
        condition=lambda df: (
            (F.col("driver_pay") >= 0) & (F.col("base_passenger_fare") >= 0)
        ),
    ),
]



# ── Rule application ──────────────────────────────────────────────────────────

def apply_rules(
    df: DataFrame,
    rules: List[Rule],
    reject_column: str = "_rejected",
    reason_column: str = "_rejection_reason",
) -> Tuple[DataFrame, DataFrame]:
    """
    Apply a list of :class:`Rule` objects to a DataFrame.

    Records that pass **all** rules are returned in ``valid_df``.
    Records that fail **any** rule are returned in ``rejected_df``
    with an additional ``_rejection_reason`` column explaining the first
    failing rule.

    Parameters
    ----------
    df:
        Input DataFrame (typically read from the Bronze layer).
    rules:
        List of :class:`Rule` to evaluate in order.
    reject_column:
        Internal boolean flag column name.
    reason_column:
        Column name for the rejection reason string.

    Returns
    -------
    tuple[DataFrame, DataFrame]
        ``(valid_df, rejected_df)``
    """
    # Build a rejection reason expression using nested `when` chains
    reason_expr: Column = F.lit(None).cast("string")
    is_valid_expr: Column = F.lit(True)

    for rule in reversed(rules):
        passes = rule.condition(df)
        is_valid_expr = is_valid_expr & passes
        reason_expr = F.when(~passes, F.lit(rule.description)).otherwise(reason_expr)

    df = df.withColumn(reject_column, ~is_valid_expr) \
           .withColumn(reason_column, reason_expr)

    valid_df    = df.filter(~F.col(reject_column)).drop(reject_column, reason_column)
    rejected_df = df.filter( F.col(reject_column)).drop(reject_column)

    return valid_df, rejected_df


def print_rejection_summary(rejected_df: DataFrame, reason_column: str = "_rejection_reason") -> None:
    """Log a breakdown of rejection counts per rule."""
    total = rejected_df.count()
    if total == 0:
        logger.info("[RULES] No records rejected.")
        return

    logger.info(f"[RULES] Total records rejected: {total:,}")
    breakdown = (
        rejected_df.groupBy(reason_column)
        .count()
        .orderBy("count", ascending=False)
    )
    for row in breakdown.collect():
        logger.info(f"         ↳ '{row[reason_column]}': {row['count']:,}")
