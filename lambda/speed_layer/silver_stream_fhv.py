"""
Auto-generated from 03_silver_stream_fhv.ipynb
"""
import sys
sys.path.insert(0, "/home/jovyan/work")


import os
import json
import datetime
import uuid
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.spark_utils import get_spark_streaming, write_mongo
from src.transformations.tlc_rules import (
    FHV_RULES, apply_quality_flags, route_quarantine
)
from src.transformations.enrichment import load_zone_lookup, enrich_trip_locations
from src.transformations.schema import build_fhv_silver
from core.config.settings import settings
from core.storage.mongo_client import get_audit_db

# ── Kafka configuration ───────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC_FHV_2026",  "tlc-fhv-2026")
VEHICLE_TYPE    = "fhv"

# ── Checkpoint — critical for exactly-once and safe restarts ──────────────────
# This path persists across notebook restarts.
# Delete this folder only if you want to reprocess from the beginning.
CHECKPOINT_BASE = "/home/jovyan/work/data/checkpoints"
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/silver_stream_{VEHICLE_TYPE}_2026"

# ── Streaming trigger ─────────────────────────────────────────────────────────
# processingTime: how often Spark polls Kafka for new records
TRIGGER_INTERVAL = "30 seconds"   # adjust to "5 seconds" for faster demo

# ── Kafka read settings ───────────────────────────────────────────────────────
STARTING_OFFSETS = "earliest"   # process from start; use "latest" after 1st run
MAX_OFFSETS_PER_TRIGGER = 5000  # max records per micro-batch (controls memory)

print(f"Kafka bootstrap  : {KAFKA_BOOTSTRAP}")
print(f"Kafka topic      : {KAFKA_TOPIC}")
print(f"Checkpoint path  : {CHECKPOINT_PATH}")
print(f"Trigger interval : {TRIGGER_INTERVAL}")
print(f"Max offsets/batch: {MAX_OFFSETS_PER_TRIGGER:,}")

# Create checkpoint directory
Path(CHECKPOINT_PATH).mkdir(parents=True, exist_ok=True)
print(f"\n✓ Checkpoint directory ready.")

spark = get_spark_streaming(
    app_name="TLC_Silver_Stream_FHV_2026",
    kafka_bootstrap=KAFKA_BOOTSTRAP,
)
print(f"Spark version: {spark.version}")
print(f"Spark UI: http://localhost:4060")

# Schema for the _stream_meta envelope injected by the Producer
STREAM_META_SCHEMA = T.StructType([
    T.StructField("produced_at",  T.StringType(),  True),
    T.StructField("source_file",  T.StringType(),  True),
    T.StructField("vehicle_type", T.StringType(),  True),
    T.StructField("batch_id",     T.IntegerType(), True),
])

# Schema for Yellow Taxi trip record fields
FHV_RECORD_SCHEMA = T.StructType([
    T.StructField("dispatching_base_num",   T.StringType(),  True),
    T.StructField("pickup_datetime",        T.StringType(),  True),
    T.StructField("dropoff_datetime",       T.StringType(),  True),
    T.StructField("PULocationID",           T.IntegerType(), True),
    T.StructField("DOLocationID",           T.IntegerType(), True),
    T.StructField("SR_Flag",                T.DoubleType(),  True),
    T.StructField("Affiliated_base_number", T.StringType(),  True),
    T.StructField("_stream_meta",           STREAM_META_SCHEMA, True),
])

print("JSON schema defined.")

zone_df_broadcast = load_zone_lookup(spark)
# Cache the zone lookup — it's tiny (265 rows) and used in every batch
zone_df_broadcast.cache()
zone_count = zone_df_broadcast.count()
print(f"✓ Zone lookup cached: {zone_count} zones")

# Shared batch counter (mutable via list trick)
_batch_stats = {"total_silver": 0, "total_quarantine": 0, "batches_processed": 0}


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch callback — receives a STATIC micro-batch DataFrame.

    This function is called by Spark Structured Streaming every trigger interval.
    All .count(), .filter(), and .write() operations are SAFE here.

    Parameters
    ----------
    batch_df:
        Raw Kafka micro-batch. Schema: [key, value, topic, partition, offset, timestamp, ...]
    batch_id:
        Monotonically increasing batch sequence number (for idempotency).
    """
    batch_start = datetime.datetime.utcnow()
    stream_run_id = f"stream_{batch_id:06d}_{str(uuid.uuid4())[:8]}"

    print(f"\n── Batch {batch_id} | run_id={stream_run_id} | {batch_start.isoformat()} ──")

    # ── Step 1: Skip empty batches ─────────────────────────────────────────────
    if batch_df.rdd.isEmpty():
        print(f"  [Batch {batch_id}] Empty — no records from Kafka. Waiting...")
        return

    # ── Step 2: Parse Kafka value bytes → JSON → typed schema ──────────────────
    parsed_df = (
        batch_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                FHV_RECORD_SCHEMA
            ).alias("data"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
        )
        .select("data.*", "_kafka_partition", "_kafka_offset")
    )

    # Cast ISO datetime strings → Spark timestamps
    parsed_df = (
        parsed_df
        .withColumn("pickup_datetime",  F.to_timestamp("pickup_datetime"))
        .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
        
    )

    # ── Step 3: Inject Bronze-equivalent _meta from _stream_meta ───────────────
    # In streaming, Bronze is bypassed. We attach equivalent lineage metadata
    # using the Producer's _stream_meta envelope.
    parsed_df = parsed_df.withColumn(
        "_meta",
        F.struct(
            F.col("_stream_meta.vehicle_type").alias("vehicle_type"),
            F.lit(stream_run_id).alias("run_id"),          # this IS the bronze_run_id
            F.current_timestamp().alias("ingestion_time"),
            F.col("_stream_meta.source_file").alias("source_file"),
        )
    ).drop("_stream_meta")

    # Drop Kafka internal columns before quality checks
    parsed_df = parsed_df.drop("_kafka_partition", "_kafka_offset")

    # Count bronze-equivalent records (SAFE: batch_df is static)
    records_in = parsed_df.count()
    print(f"  Records received from Kafka: {records_in:,}")

    if records_in == 0:
        print(f"  [Batch {batch_id}] All records failed JSON parsing. Skipping.")
        return

    # ── Step 4: Apply quality flags ────────────────────────────────────────────
    flagged_df = apply_quality_flags(parsed_df, FHV_RULES)

    # ── Step 5: Route valid → Silver, rejected → Quarantine ────────────────────
    valid_df, rejected_df = route_quarantine(flagged_df)

    # Count both partitions (SAFE in foreachBatch)
    records_valid    = valid_df.count()
    records_rejected = rejected_df.count()
    quar_rate        = records_rejected / records_in * 100 if records_in > 0 else 0

    print(f"  Valid     : {records_valid:,}")
    print(f"  Rejected  : {records_rejected:,}  ({quar_rate:.2f}% quarantine rate)")

    # ── Step 6: Enrich valid records with zone metadata ────────────────────────
    if records_valid > 0:
        enriched_df = enrich_trip_locations(valid_df, zone_df_broadcast)

        # ── Step 7: Build Silver schema ────────────────────────────────────────
        # stream_run_id acts as both the Silver run_id and the bronze_run_id
        # (since we bypass Bronze in streaming mode)
        silver_df = build_fhv_silver(enriched_df, run_id=stream_run_id)

        # ── Step 8a: Write Silver records → MongoDB ────────────────────────────
        n_silver = write_mongo(
            silver_df,
            settings.MONGO_DB_SILVER,
            "trips_fhv",
            mode="append",
        )
        print(f"  ✓ Silver written: {n_silver:,} rows → tlc_silver.trips_fhv")
        _batch_stats["total_silver"] += n_silver

    # ── Step 8b: Write Quarantine records → MongoDB ────────────────────────────
    if records_rejected > 0:
        seen_cols = set()
        raw_cols = []
        for c in rejected_df.columns:
            if c not in ("_rejection_reason", "quality_flags", "_meta") and c.lower() not in seen_cols:
                raw_cols.append(c)
                seen_cols.add(c.lower())
        
        quarantine_df_mapped = rejected_df.select(
            F.current_timestamp().alias("quarantined_at"),
            F.lit(stream_run_id).alias("silver_run_id"),
            F.lit(stream_run_id).alias("bronze_run_id"),
            F.col("_meta.source_file").alias("source_file"),
            F.lit("silver_stream_fhv").alias("pipeline"),
            F.col("_rejection_reason").alias("reason"),
            F.col("quality_flags").alias("quality_flags"),
            F.lit(batch_id).alias("kafka_batch_id"),
            F.struct(*[F.col(c) for c in raw_cols]).alias("raw_record")
        )
        
        write_mongo(quarantine_df_mapped, settings.MONGO_DB_AUDIT, "quarantine", mode="append")
        print(f"  ✓ Quarantine: {records_rejected:,} records → tlc_audit.quarantine (Distributed)")
        _batch_stats["total_quarantine"] += records_rejected

    # ── Step 9: Log streaming batch audit record ───────────────────────────────
    batch_end       = datetime.datetime.utcnow()
    duration_s      = (batch_end - batch_start).total_seconds()
    throughput      = records_in / duration_s if duration_s > 0 else 0

    audit_record = {
        "execution_id":   stream_run_id,
        "pipeline_name":  "silver_stream_fhv",
        "status":         "SUCCESS",
        "kafka_batch_id": batch_id,
        "start_time":     batch_start.isoformat(),
        "end_time":       batch_end.isoformat(),
        "duration_seconds": round(duration_s, 2),
        "output_summary": {
            "records_read_from_bronze":    records_in,
            "records_written_to_silver":   records_valid,
            "records_quarantined":         records_rejected,
            "quarantine_rate_pct":         round(quar_rate, 4),
            "throughput_records_per_s":    round(throughput, 1),
        },
        "quality_checks": [],
        "quality_passed": 0 if records_rejected > 0 else 1,
        "errors": [],
        "mode": "streaming_kafka",
    }

    try:
        get_audit_db()["pipeline_runs"].insert_one(audit_record)
    except Exception as e:
        print(f"   Audit log write failed: {e}")

    _batch_stats["batches_processed"] += 1

    # ── Batch summary ──────────────────────────────────────────────────────────
    print(f"  Duration  : {duration_s:.1f}s  |  Throughput: {throughput:,.0f} rec/s")
    print(f"  Cumulative: Silver={_batch_stats['total_silver']:,}  "
          f"Quarantine={_batch_stats['total_quarantine']:,}  "
          f"Batches={_batch_stats['batches_processed']}")


print("✓ foreachBatch function 'process_batch' defined.")

#  df_stream is a STREAMING DataFrame — NEVER call .count(), .show(), .collect()
#   on this object outside of foreachBatch!

df_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    .option("kafka.group.id", "tlc_silver_consumer")
    # Fault tolerance: don't fail if topic doesn't exist yet
    .option("kafka.max.poll.records", "1000")
    .option("failOnDataLoss", "false")
    .load()
)

print(f"✓ Streaming DataFrame created from topic '{KAFKA_TOPIC}'")
print(f"  Schema: {df_stream.schema.simpleString()}")
print(f"  isStreaming: {df_stream.isStreaming}")

