"""
Spatial enrichment for TLC trip records.

Performs a broadcast join against the TLC Taxi Zone Lookup table to replace
raw location IDs (PULocationID / DOLocationID) with human-readable
Borough and Zone names.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from core.audit.logger import setup_logger
from src.paths import TAXI_ZONE_LOOKUP, str_path

logger = setup_logger("tlc.transformations.enrichment")


def load_zone_lookup(spark: SparkSession) -> DataFrame:
    """
    Load the TLC Taxi Zone lookup CSV as a broadcast-ready DataFrame.

    The lookup file maps ``LocationID`` to ``Borough``, ``Zone``, and
    ``service_zone``.

    Parameters
    ----------
    spark:
        Active SparkSession.

    Returns
    -------
    DataFrame
        Columns: ``LocationID``, ``Borough``, ``Zone``, ``service_zone``.
    """
    path = str_path(TAXI_ZONE_LOOKUP)
    zones = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )
    logger.info(f"[ENRICH] Zone lookup loaded: {zones.count()} zones from {path}")
    return zones


def enrich_trip_locations(
    df: DataFrame,
    zone_df: DataFrame,
    pickup_id_col: str = "PULocationID",
    dropoff_id_col: str = "DOLocationID",
) -> DataFrame:
    """
    Enrich a trip DataFrame with Borough and Zone name columns for both
    pickup and dropoff locations.

    The zone lookup is broadcast to avoid a full shuffle.

    Parameters
    ----------
    df:
        Input trip DataFrame.
    zone_df:
        Zone lookup DataFrame from :func:`load_zone_lookup`.
    pickup_id_col:
        Name of the pickup location ID column (Yellow/Green differ from FHV).
    dropoff_id_col:
        Name of the dropoff location ID column.

    Returns
    -------
    DataFrame
        Original columns + pickup/dropoff Borough and Zone columns.
    """
    zones_broadcast = F.broadcast(zone_df)

    # Pickup enrichment
    pickup_alias = zones_broadcast.toDF(
        "pu_location_id", "pu_borough", "pu_zone", "pu_service_zone"
    )
    df = df.join(
        pickup_alias,
        df[pickup_id_col] == pickup_alias["pu_location_id"],
        how="left",
    ).drop("pu_location_id")

    # Dropoff enrichment
    dropoff_alias = zones_broadcast.toDF(
        "do_location_id", "do_borough", "do_zone", "do_service_zone"
    )
    df = df.join(
        dropoff_alias,
        df[dropoff_id_col] == dropoff_alias["do_location_id"],
        how="left",
    ).drop("do_location_id")

    logger.info("[ENRICH] Location IDs enriched with Borough and Zone names.")
    return df
