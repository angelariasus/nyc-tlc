"""
Silver document schema builder for TLC trip records.

Transforms a raw Bronze DataFrame into the normalized nested document
structure used in the Silver layer of MongoDB.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, LongType

from core.audit.logger import setup_logger

logger = setup_logger("tlc.transformations.schema")

# Payment type decoder
PAYMENT_TYPE_MAP = {
    1: "Credit Card",
    2: "Cash",
    3: "No Charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided Trip",
}

def _safe_col(df: DataFrame, col_name: str, cast_type: str = "double"):
    """
    Safely retrieves a column if it exists in the DataFrame,
    otherwise returns a literal Null of the specified type.
    Crucial for Schema Evolution (handling historical data before 2019).
    """
    if col_name in df.columns:
        return F.col(col_name).cast(cast_type)
    return F.lit(None).cast(cast_type)


def build_yellow_silver(df: DataFrame, run_id: str) -> DataFrame:
    """
    Transform a Yellow Taxi Bronze DataFrame into the Silver schema.

    Produces a normalized nested structure with sub-structs for:
    - ``datetimes``      (pickup, dropoff, duration_minutes)
    - ``locations``      (pickup/dropoff zone_id, borough, zone, after enrichment)
    - ``metrics``        (distance_miles, passenger_count)
    - ``financials``     (fare_amount, tip, tolls, cbd_congestion_fee, total, payment)
    - ``quality_flags``  (per-rule boolean pass/fail audit flags)
    - ``_meta``          (run_id, bronze_run_id, source_file, vehicle_type,
                          processed_at, source_year, source_month)

    The ``_meta.bronze_run_id`` and ``_meta.source_file`` fields propagate the
    upstream Bronze execution ID and original filename, providing a full
    Bronze → Silver data-lineage chain.

    Parameters
    ----------
    df:
        Enriched Bronze DataFrame (after :func:`~src.transformations.enrichment.enrich_trip_locations`).
        Must have been annotated by ``apply_quality_flags`` before calling this.
    run_id:
        The ``execution_id`` from the active :class:`~core.audit.control_manager.ControlManager`.

    Returns
    -------
    DataFrame
        Silver-schema DataFrame, ready to be written to ``tlc_silver.trips_yellow``.
    """
    from pyspark.sql.functions import from_unixtime, unix_timestamp

    duration_min = (
        unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")
    ) / 60.0

    payment_map_expr = F.create_map(
        *[item for pair in [(F.lit(k), F.lit(v)) for k, v in PAYMENT_TYPE_MAP.items()] for item in pair]
    )

    # quality_flags column is present after apply_quality_flags()
    has_flags = "quality_flags" in df.columns

    silver = df.select(
        # ── Identifiers ──────────────────────────────────────────────────────
        F.col("VendorID").cast("int").alias("vendor_id"),

        # ── Datetimes ────────────────────────────────────────────────────────
        F.struct(
            F.col("tpep_pickup_datetime").alias("pickup"),
            F.col("tpep_dropoff_datetime").alias("dropoff"),
            F.round(duration_min, 2).alias("duration_minutes"),
        ).alias("datetimes"),

        # ── Locations (enriched by zone broadcast join) ───────────────────────
        F.struct(
            F.struct(
                F.col("PULocationID").alias("zone_id"),
                F.col("pu_borough").alias("borough"),
                F.col("pu_zone").alias("zone"),
                F.col("pu_service_zone").alias("service_zone"),
            ).alias("pickup"),
            F.struct(
                F.col("DOLocationID").alias("zone_id"),
                F.col("do_borough").alias("borough"),
                F.col("do_zone").alias("zone"),
                F.col("do_service_zone").alias("service_zone"),
            ).alias("dropoff"),
        ).alias("locations"),

        # ── Trip Metrics ─────────────────────────────────────────────────────
        F.struct(
            F.col("trip_distance").alias("distance_miles"),
            F.col("passenger_count").cast("int").alias("passenger_count"),
            F.col("RatecodeID").cast("int").alias("rate_code_id"),
        ).alias("metrics"),

        # ── Financials ───────────────────────────────────────────────────────
        F.struct(
            F.col("fare_amount").alias("fare_amount"),
            F.col("extra").alias("extra"),
            F.col("mta_tax").alias("mta_tax"),
            F.col("tip_amount").alias("tip_amount"),
            F.col("tolls_amount").alias("tolls_amount"),
            F.col("improvement_surcharge").alias("improvement_surcharge"),
            _safe_col(df, "congestion_surcharge").alias("congestion_surcharge"),
            _safe_col(df, "airport_fee").alias("airport_fee"),
            _safe_col(df, "cbd_congestion_fee").alias("cbd_congestion_fee"),
            F.col("total_amount").alias("total_amount"),
            payment_map_expr[F.col("payment_type").cast("int")].alias("payment_type"),
        ).alias("financials"),

        # ── Quality Flags (audit trail, preserved from rule engine) ───────────
        *([
            F.col("quality_flags")
        ] if has_flags else []),

        # ── Metadata (with full Bronze → Silver lineage) ──────────────────────
        F.struct(
            F.lit("yellow").alias("vehicle_type"),
            F.lit(run_id).alias("run_id"),
            # Bronze lineage — traceable back to the ingestion run and raw file
            F.col("_meta.run_id").alias("bronze_run_id"),
            F.col("_meta.source_file").alias("source_file"),
            F.current_timestamp().alias("processed_at"),
            F.year("tpep_pickup_datetime").alias("source_year"),
            F.month("tpep_pickup_datetime").alias("source_month"),
        ).alias("_meta"),
    )

    logger.info("[SCHEMA] Yellow Silver schema applied (with bronze lineage + quality_flags).")
    return silver


def build_green_silver(df: DataFrame, run_id: str) -> DataFrame:
    """
    Transform a Green Taxi Bronze DataFrame into the Silver schema.

    Identical nested structure to :func:`build_yellow_silver` but uses
    Green-specific column names (``lpep_*`` for datetimes,
    ``trip_type`` instead of ``RatecodeID``).
    Propagates ``bronze_run_id`` and ``source_file`` from the Bronze ``_meta``
    and preserves ``quality_flags`` when present.
    """
    duration_min = (
        F.unix_timestamp("lpep_dropoff_datetime") - F.unix_timestamp("lpep_pickup_datetime")
    ) / 60.0

    payment_map_expr = F.create_map(
        *[item for pair in [(F.lit(k), F.lit(v)) for k, v in PAYMENT_TYPE_MAP.items()] for item in pair]
    )

    has_flags = "quality_flags" in df.columns

    silver = df.select(
        F.col("VendorID").cast("int").alias("vendor_id"),

        F.struct(
            F.col("lpep_pickup_datetime").alias("pickup"),
            F.col("lpep_dropoff_datetime").alias("dropoff"),
            F.round(duration_min, 2).alias("duration_minutes"),
        ).alias("datetimes"),

        F.struct(
            F.struct(
                F.col("PULocationID").alias("zone_id"),
                F.col("pu_borough").alias("borough"),
                F.col("pu_zone").alias("zone"),
                F.col("pu_service_zone").alias("service_zone"),
            ).alias("pickup"),
            F.struct(
                F.col("DOLocationID").alias("zone_id"),
                F.col("do_borough").alias("borough"),
                F.col("do_zone").alias("zone"),
                F.col("do_service_zone").alias("service_zone"),
            ).alias("dropoff"),
        ).alias("locations"),

        F.struct(
            F.col("trip_distance").alias("distance_miles"),
            F.col("passenger_count").cast("int").alias("passenger_count"),
            F.col("trip_type").cast("int").alias("trip_type"),
        ).alias("metrics"),

        F.struct(
            F.col("fare_amount").alias("fare_amount"),
            F.col("extra").alias("extra"),
            F.col("mta_tax").alias("mta_tax"),
            F.col("tip_amount").alias("tip_amount"),
            F.col("tolls_amount").alias("tolls_amount"),
            F.col("improvement_surcharge").alias("improvement_surcharge"),
            _safe_col(df, "congestion_surcharge").alias("congestion_surcharge"),
            _safe_col(df, "airport_fee").alias("airport_fee"),
            _safe_col(df, "cbd_congestion_fee").alias("cbd_congestion_fee"),
            F.col("total_amount").alias("total_amount"),
            payment_map_expr[F.col("payment_type").cast("int")].alias("payment_type"),
        ).alias("financials"),

        # ── Quality Flags ─────────────────────────────────────────────────────
        *([
            F.col("quality_flags")
        ] if has_flags else []),

        # ── Metadata (with Bronze lineage) ────────────────────────────────────
        F.struct(
            F.lit("green").alias("vehicle_type"),
            F.lit(run_id).alias("run_id"),
            F.col("_meta.run_id").alias("bronze_run_id"),
            F.col("_meta.source_file").alias("source_file"),
            F.current_timestamp().alias("processed_at"),
            F.year("lpep_pickup_datetime").alias("source_year"),
            F.month("lpep_pickup_datetime").alias("source_month"),
        ).alias("_meta"),
    )

    logger.info("[SCHEMA] Green Silver schema applied (with bronze lineage + quality_flags).")
    return silver


def build_fhv_silver(df: DataFrame, run_id: str) -> DataFrame:
    """
    Transform a For-Hire Vehicle (FHV) Bronze DataFrame into the Silver schema.

    Since FHV has very few fields compared to Yellow/Green, missing fields
    in ``metrics`` and ``financials`` are filled with nulls to maintain
    schema consistency across vehicle types.
    Propagates ``bronze_run_id`` and ``source_file`` from the Bronze ``_meta``
    and preserves ``quality_flags`` when present.
    """
    duration_min = (
        F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")
    ) / 60.0

    has_flags = "quality_flags" in df.columns

    silver = df.select(
        F.lit(None).cast("int").alias("vendor_id"),

        F.struct(
            F.col("pickup_datetime").alias("pickup"),
            F.col("dropoff_datetime").alias("dropoff"),
            F.round(duration_min, 2).alias("duration_minutes"),
        ).alias("datetimes"),

        F.struct(
            F.struct(
                F.col("PULocationID").alias("zone_id"),
                F.col("pu_borough").alias("borough"),
                F.col("pu_zone").alias("zone"),
                F.col("pu_service_zone").alias("service_zone"),
            ).alias("pickup"),
            F.struct(
                F.col("DOLocationID").alias("zone_id"),
                F.col("do_borough").alias("borough"),
                F.col("do_zone").alias("zone"),
                F.col("do_service_zone").alias("service_zone"),
            ).alias("dropoff"),
        ).alias("locations"),

        F.struct(
            F.lit(None).cast("double").alias("distance_miles"),
            F.lit(None).cast("int").alias("passenger_count"),
            F.lit(None).cast("int").alias("trip_type"),
        ).alias("metrics"),

        F.struct(
            F.lit(None).cast("double").alias("fare_amount"),
            F.lit(None).cast("double").alias("extra"),
            F.lit(None).cast("double").alias("mta_tax"),
            F.lit(None).cast("double").alias("tip_amount"),
            F.lit(None).cast("double").alias("tolls_amount"),
            F.lit(None).cast("double").alias("improvement_surcharge"),
            F.lit(None).cast("double").alias("congestion_surcharge"),
            F.lit(None).cast("double").alias("airport_fee"),
            F.lit(None).cast("double").alias("cbd_congestion_fee"),
            F.lit(None).cast("double").alias("total_amount"),
            F.lit(None).cast("string").alias("payment_type"),
        ).alias("financials"),

        # ── Quality Flags ─────────────────────────────────────────────────────
        *([
            F.col("quality_flags")
        ] if has_flags else []),

        # ── Metadata (with Bronze lineage) ────────────────────────────────────
        F.struct(
            F.lit("fhv").alias("vehicle_type"),
            F.lit(run_id).alias("run_id"),
            F.col("_meta.run_id").alias("bronze_run_id"),
            F.col("_meta.source_file").alias("source_file"),
            F.current_timestamp().alias("processed_at"),
            F.year("pickup_datetime").alias("source_year"),
            F.month("pickup_datetime").alias("source_month"),
        ).alias("_meta"),

        # FHV specific fields not fitting the standard pattern perfectly
        F.col("dispatching_base_num").alias("dispatching_base_num"),
        F.col("Affiliated_base_number").alias("affiliated_base_number"),
        F.col("SR_Flag").alias("sr_flag"),
    )

    logger.info("[SCHEMA] FHV Silver schema applied (with bronze lineage + quality_flags).")
    return silver


def build_hvfhv_silver(df: DataFrame, run_id: str) -> DataFrame:
    """
    Transform a High Volume FHV (HVFHV) Bronze DataFrame into the Silver schema.

    Propagates ``bronze_run_id`` and ``source_file`` from the Bronze ``_meta``
    and preserves ``quality_flags`` when present.
    """
    duration_min = (
        F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")
    ) / 60.0

    has_flags = "quality_flags" in df.columns

    # Fetch safe columns for computation
    safe_airport_fee = _safe_col(df, "airport_fee")
    safe_congestion  = _safe_col(df, "congestion_surcharge")
    safe_cbd         = _safe_col(df, "cbd_congestion_fee")

    # Coalesce to 0 for math so the total isn't nulled out
    math_airport    = F.coalesce(safe_airport_fee, F.lit(0.0))
    math_congestion = F.coalesce(safe_congestion, F.lit(0.0))

    silver = df.select(
        F.lit(None).cast("int").alias("vendor_id"),

        F.struct(
            F.col("pickup_datetime").alias("pickup"),
            F.col("dropoff_datetime").alias("dropoff"),
            F.round(duration_min, 2).alias("duration_minutes"),
        ).alias("datetimes"),

        F.struct(
            F.struct(
                F.col("PULocationID").alias("zone_id"),
                F.col("pu_borough").alias("borough"),
                F.col("pu_zone").alias("zone"),
                F.col("pu_service_zone").alias("service_zone"),
            ).alias("pickup"),
            F.struct(
                F.col("DOLocationID").alias("zone_id"),
                F.col("do_borough").alias("borough"),
                F.col("do_zone").alias("zone"),
                F.col("do_service_zone").alias("service_zone"),
            ).alias("dropoff"),
        ).alias("locations"),

        F.struct(
            F.col("trip_miles").alias("distance_miles"),
            F.lit(None).cast("int").alias("passenger_count"),
            F.lit(None).cast("int").alias("trip_type"),
        ).alias("metrics"),

        F.struct(
            F.col("base_passenger_fare").alias("fare_amount"),
            safe_airport_fee.alias("extra"),
            F.col("sales_tax").alias("mta_tax"),
            F.col("tips").alias("tip_amount"),
            F.col("tolls").alias("tolls_amount"),
            F.col("bcf").alias("improvement_surcharge"),
            safe_congestion.alias("congestion_surcharge"),
            safe_airport_fee.alias("airport_fee"),
            safe_cbd.alias("cbd_congestion_fee"),
            # Compute total roughly for HVFHV
            (
                F.col("base_passenger_fare") + F.col("tolls") + F.col("sales_tax")
                + math_congestion + F.col("tips") + F.col("bcf")
                + math_airport
            ).alias("total_amount"),
            F.lit(None).cast("string").alias("payment_type"),
        ).alias("financials"),

        # ── Quality Flags ─────────────────────────────────────────────────────
        *([
            F.col("quality_flags")
        ] if has_flags else []),

        # ── Metadata (with Bronze lineage) ────────────────────────────────────
        F.struct(
            F.lit("hvfhv").alias("vehicle_type"),
            F.lit(run_id).alias("run_id"),
            F.col("_meta.run_id").alias("bronze_run_id"),
            F.col("_meta.source_file").alias("source_file"),
            F.current_timestamp().alias("processed_at"),
            F.year("pickup_datetime").alias("source_year"),
            F.month("pickup_datetime").alias("source_month"),
        ).alias("_meta"),

        # HVFHV specific fields
        F.col("hvfhs_license_num").alias("hvfhs_license_num"),
        F.col("dispatching_base_num").alias("dispatching_base_num"),
        F.col("originating_base_num").alias("originating_base_num"),
        F.col("driver_pay").alias("driver_pay"),
    )

    logger.info("[SCHEMA] HVFHV Silver schema applied (with bronze lineage + quality_flags).")
    return silver

