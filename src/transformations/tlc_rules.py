"""
Data quality rules for TLC trip records.

Each rule is a named predicate applied to a PySpark DataFrame.
Instead of silently dropping invalid records, the engine TAGS every record
with a ``quality_flags`` struct and an ``is_valid`` boolean column.

Records that fail any critical rule are ROUTED to the quarantine collection
in MongoDB, but the original data is NEVER deleted from the pipeline.
Valid records flow to Silver WITH their quality flags intact (full traceability).

Strategy
--------
1. ``apply_quality_flags(df, rules)``
   Annotates the DataFrame with ``quality_flags`` (struct of per-rule booleans)
   and ``is_valid`` (True if all flags pass). No filtering happens here.

2. ``route_quarantine(df)``
   Splits the flagged DataFrame into ``(valid_df, rejected_df)``.
   Both DataFrames RETAIN the ``quality_flags`` column.
   ``valid_df`` → Silver layer (with flags as audit trail).
   ``rejected_df`` → tlc_audit.quarantine (with reason).

3. ``apply_rules(df, rules)`` [compatibility alias]
   Calls the above two steps in sequence.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List, Tuple

from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F

from core.audit.logger import setup_logger

logger = setup_logger("tlc.transformations.tlc_rules")


# ── Rule definition ────────────────────────────────────────────────────────────

@dataclass
class Rule:
    name: str
    description: str
    condition: Callable[[DataFrame], Column]  # Returns a boolean Column (True = PASS)


# ── Shared rules (safe for ALL vehicle types with these columns) ───────────────

# Rules that apply ONLY to Yellow taxi (uses tpep_* datetime column names)
YELLOW_RULES: List[Rule] = [
    Rule(
        name="valid_distance",
        description="Trip distance must be greater than zero.",
        condition=lambda df: F.col("trip_distance") > 0,
    ),
    Rule(
        name="valid_fare",
        description="Fare amount must not be negative.",
        condition=lambda df: F.col("fare_amount") >= 0,
    ),
    Rule(
        name="valid_total",
        description="Total amount must not be negative.",
        condition=lambda df: F.col("total_amount") >= 0,
    ),
    Rule(
        name="valid_pickup_zone",
        description="Pickup location ID must be in [1, 265].",
        condition=lambda df: F.col("PULocationID").between(1, 265),
    ),
    Rule(
        name="valid_dropoff_zone",
        description="Dropoff location ID must be in [1, 265].",
        condition=lambda df: F.col("DOLocationID").between(1, 265),
    ),
    Rule(
        name="valid_time_order",
        description="Dropoff datetime must be after pickup datetime.",
        condition=lambda df: (
            F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")
        ),
    ),
    Rule(
        name="valid_passengers",
        description="Passenger count must be between 1 and 8.",
        condition=lambda df: F.col("passenger_count").between(1, 8),
    ),
]

# Rules that are truly shared between Yellow AND Green (no tpep_*/lpep_* references).
# The time-order rule is intentionally EXCLUDED here because Yellow uses tpep_*
# and Green uses lpep_* — applying the wrong one silently fails every record.
YELLOW_GREEN_SHARED_RULES: List[Rule] = [
    Rule(
        name="valid_distance",
        description="Trip distance must be greater than zero.",
        condition=lambda df: F.col("trip_distance") > 0,
    ),
    Rule(
        name="valid_fare",
        description="Fare amount must not be negative.",
        condition=lambda df: F.col("fare_amount") >= 0,
    ),
    Rule(
        name="valid_total",
        description="Total amount must not be negative.",
        condition=lambda df: F.col("total_amount") >= 0,
    ),
    Rule(
        name="valid_pickup_zone",
        description="Pickup location ID must be in [1, 265].",
        condition=lambda df: F.col("PULocationID").between(1, 265),
    ),
    Rule(
        name="valid_dropoff_zone",
        description="Dropoff location ID must be in [1, 265].",
        condition=lambda df: F.col("DOLocationID").between(1, 265),
    ),
    Rule(
        name="valid_passengers",
        description="Passenger count must be between 1 and 8.",
        condition=lambda df: F.col("passenger_count").between(1, 8),
    ),
]

# Backwards-compatibility alias — existing notebooks that import YELLOW_GREEN_RULES
# and use it with Yellow data will continue to work correctly since YELLOW_RULES
# is a strict superset of the shared rules.
# ⚠️  Do NOT use YELLOW_GREEN_RULES with Green data — use YELLOW_GREEN_SHARED_RULES
# combined with GREEN_EXTRA_RULES instead.
YELLOW_GREEN_RULES: List[Rule] = YELLOW_RULES


GREEN_EXTRA_RULES: List[Rule] = [
    Rule(
        name="valid_time_order_green",
        description="Dropoff datetime must be after pickup datetime (Green lpep_* field names).",
        condition=lambda df: (
            F.col("lpep_dropoff_datetime") > F.col("lpep_pickup_datetime")
        ),
    ),
]

# Full Green rules = shared column rules + lpep_* time-order rule
GREEN_RULES: List[Rule] = YELLOW_GREEN_SHARED_RULES + GREEN_EXTRA_RULES

# ── FHV rules ──────────────────────────────────────────────────────────────────

FHV_RULES: List[Rule] = [
    Rule(
        name="valid_pickup_zone",
        description="Pickup location ID must be in [1, 265].",
        condition=lambda df: F.col("PULocationID").between(1, 265),
    ),
    Rule(
        name="valid_dropoff_zone",
        description="Dropoff location ID must be in [1, 265].",
        condition=lambda df: F.col("DOLocationID").between(1, 265),
    ),
    Rule(
        name="valid_time_order",
        description="Dropoff datetime must be after pickup datetime.",
        condition=lambda df: (
            F.col("dropoff_datetime") > F.col("pickup_datetime")
        ),
    ),
]

# ── HVFHV rules ────────────────────────────────────────────────────────────────

HVFHV_RULES: List[Rule] = [
    Rule(
        name="valid_pickup_zone",
        description="Pickup location ID must be in [1, 265].",
        condition=lambda df: F.col("PULocationID").between(1, 265),
    ),
    Rule(
        name="valid_dropoff_zone",
        description="Dropoff location ID must be in [1, 265].",
        condition=lambda df: F.col("DOLocationID").between(1, 265),
    ),
    Rule(
        name="valid_time_order",
        description="Dropoff datetime must be after pickup datetime.",
        condition=lambda df: (
            F.col("dropoff_datetime") > F.col("pickup_datetime")
        ),
    ),
    Rule(
        name="valid_pay",
        description="Driver pay and base passenger fare must not be negative.",
        condition=lambda df: (
            (F.col("driver_pay") >= 0) & (F.col("base_passenger_fare") >= 0)
        ),
    ),
]


# ── Core: Flag-based quality annotation ───────────────────────────────────────

def apply_quality_flags(
    df: DataFrame,
    rules: List[Rule],
    flags_column: str = "quality_flags",
    is_valid_column: str = "is_valid",
    reason_column: str = "_rejection_reason",
) -> DataFrame:
    """
    Annotate every record with quality flags. NO records are dropped.

    Adds two columns to the DataFrame:

    * ``quality_flags`` — a struct with one boolean field per rule
      (field name = ``rule.name``).  ``True`` means the rule PASSED.
    * ``is_valid`` — ``True`` if **all** per-rule flags are ``True``.
    * ``_rejection_reason`` — human-readable description of the first
      failing rule, or ``None`` for valid records.

    Parameters
    ----------
    df:
        Input DataFrame (typically read from the Bronze layer).
    rules:
        List of :class:`Rule` to evaluate in order.
    flags_column:
        Name of the struct column that will hold per-rule booleans.
    is_valid_column:
        Name of the overall validity boolean column.
    reason_column:
        Column name for the rejection reason string (first failing rule).

    Returns
    -------
    DataFrame
        The original DataFrame with ``quality_flags``, ``is_valid``,
        and ``_rejection_reason`` columns appended.
    """
    # Build individual flag expressions
    flag_columns: list[Column] = []
    is_valid_expr: Column = F.lit(True)
    reason_expr: Column = F.lit(None).cast("string")

    for rule in rules:
        passes = rule.condition(df)
        flag_columns.append(passes.alias(rule.name))
        is_valid_expr = is_valid_expr & passes
        # reason_expr captures the FIRST failing rule
        reason_expr = F.when(~passes, F.lit(rule.description)).otherwise(reason_expr)

    df = (
        df
        .withColumn(flags_column, F.struct(*flag_columns))
        .withColumn(is_valid_column, is_valid_expr)
        .withColumn(reason_column, reason_expr)
    )

    logger.info(
        f"[RULES] Quality flags applied | rules={[r.name for r in rules]}"
    )
    return df


def route_quarantine(
    df: DataFrame,
    is_valid_column: str = "is_valid",
    reason_column: str = "_rejection_reason",
) -> Tuple[DataFrame, DataFrame]:
    """
    Split a flagged DataFrame into valid and rejected partitions.

    Both partitions RETAIN the ``quality_flags`` column so that the full
    audit trail is preserved. The ``is_valid`` helper column is dropped
    from both to keep the schema clean; ``_rejection_reason`` is kept only
    on ``rejected_df`` for quarantine documentation.

    Parameters
    ----------
    df:
        DataFrame previously annotated by :func:`apply_quality_flags`.
    is_valid_column:
        Name of the validity boolean column (created by ``apply_quality_flags``).
    reason_column:
        Name of the rejection reason column.

    Returns
    -------
    tuple[DataFrame, DataFrame]
        ``(valid_df, rejected_df)``
        * ``valid_df`` flows to the Silver layer — includes ``quality_flags``.
        * ``rejected_df`` flows to quarantine — includes ``quality_flags``
          and ``_rejection_reason``.
    """
    valid_df = (
        df.filter(F.col(is_valid_column))
          .drop(is_valid_column, reason_column)
    )
    rejected_df = (
        df.filter(~F.col(is_valid_column))
          .drop(is_valid_column)          # keep reason_column for quarantine
    )
    return valid_df, rejected_df


def apply_rules(
    df: DataFrame,
    rules: List[Rule],
    reject_column: str = "_rejected",
    reason_column: str = "_rejection_reason",
) -> Tuple[DataFrame, DataFrame]:
    """
    Compatibility wrapper — flags records then routes them.

    Equivalent to calling :func:`apply_quality_flags` followed by
    :func:`route_quarantine`. Kept for notebooks that import this directly.

    Returns
    -------
    tuple[DataFrame, DataFrame]
        ``(valid_df, rejected_df)``
    """
    df_flagged = apply_quality_flags(df, rules, reason_column=reason_column)
    return route_quarantine(df_flagged, reason_column=reason_column)


# ── Reporting ──────────────────────────────────────────────────────────────────

def print_rejection_summary(
    rejected_df: DataFrame,
    reason_column: str = "_rejection_reason",
) -> None:
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
        logger.info(f"         \u21b3 '{row[reason_column]}': {row['count']:,}")
