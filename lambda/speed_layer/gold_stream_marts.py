"""
Auto-generated from 01_gold_stream_marts.ipynb
"""
import sys
sys.path.insert(0, "/home/jovyan/work")


import os
import time
import datetime
import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

from src.spark_utils import get_spark, read_mongo
from core.config.settings import settings
from core.storage.mongo_client import get_mongo_client

# ── Polling interval ─────────────────────────────────────────────────────────
# How often this notebook polls Silver for new 2026 records.
POLL_INTERVAL_SECONDS = 30

# ── Only process 2026 data (Speed Layer scope) ───────────────────────────────
STREAM_YEAR = 2026

# ── Silver collections to poll ───────────────────────────────────────────────
SILVER_COLLECTIONS = ['trips_yellow', 'trips_green', 'trips_fhv', 'trips_hvfhv']

print('Imports OK.')
print(f'Gold Stream DB   : {settings.MONGO_DB_GOLD_STREAM}')
print(f'Poll interval    : {POLL_INTERVAL_SECONDS}s')
print(f'Streaming year   : {STREAM_YEAR}')

spark = get_spark('TLC_Gold_Stream_Marts')
spark.conf.set('spark.sql.shuffle.partitions', '50')  # small batches
print(f'Spark {spark.version} ready.')

# Load dim_zone once — it's static (265 rows)
dim_zone = read_mongo(spark, settings.MONGO_DB_GOLD, 'dim_zone').cache()
print(f'dim_zone loaded: {dim_zone.count()} zones')

def upsert_demand(db, rows):
    """
    Upsert demand KPIs into stream_mart_demand.
    Uses $inc to accumulate counts across polls (additive semantics).
    Key: (vehicle_type, year, month, day, hour, pickup_zone_id)
    """
    if not rows:
        return 0
    ops = []
    for r in rows:
        key = {
            'vehicle_type':   r['vehicle_type'],
            'year':           r['year'],
            'month':          r['month'],
            'day':            r['day'],
            'hour':           r['hour'],
            'pickup_zone_id': r.get('pickup_zone_id'),
            'zone_name':      r.get('zone_name'),
            'borough':        r.get('borough'),
        }
        ops.append(UpdateOne(
            filter=key,
            update={
                '$inc': {
                    'total_trips':      r['total_trips'],
                    'total_passengers': r['total_passengers'],
                },
                '$set': {
                    'avg_duration_min': r['avg_duration_min'],
                    'updated_at':       datetime.datetime.utcnow(),
                    'stream_year':      STREAM_YEAR,
                }
            },
            upsert=True
        ))
    try:
        res = db['stream_mart_demand'].bulk_write(ops, ordered=False)
        return res.upserted_count + res.modified_count
    except BulkWriteError as bwe:
        print(f'  BulkWriteError (demand): {bwe.details["nInserted"]} inserted, {len(bwe.details["writeErrors"])} errors')
        return 0


def upsert_financials(db, rows):
    """
    Upsert financial KPIs into stream_mart_financials.
    Key: (vehicle_type, year, month, day, hour, pickup_zone_id)
    """
    if not rows:
        return 0
    ops = []
    for r in rows:
        key = {
            'vehicle_type':   r['vehicle_type'],
            'year':           r['year'],
            'month':          r['month'],
            'day':            r['day'],
            'hour':           r['hour'],
            'pickup_zone_id': r.get('pickup_zone_id'),
            'zone_name':      r.get('zone_name'),
            'borough':        r.get('borough'),
        }
        ops.append(UpdateOne(
            filter=key,
            update={
                '$inc': {
                    'total_trips':   r['total_trips'],
                    'total_revenue': r['total_revenue'],
                    'total_tips':    r['total_tips'],
                    'total_tolls':   r['total_tolls'],
                },
                '$set': {
                    'avg_revenue_per_trip': r['avg_revenue_per_trip'],
                    'avg_tip':              r['avg_tip'],
                    'updated_at':           datetime.datetime.utcnow(),
                    'stream_year':          STREAM_YEAR,
                }
            },
            upsert=True
        ))
    try:
        res = db['stream_mart_financials'].bulk_write(ops, ordered=False)
        return res.upserted_count + res.modified_count
    except BulkWriteError as bwe:
        print(f'  BulkWriteError (financials): {len(bwe.details["writeErrors"])} errors')
        return 0


print('Upsert functions defined.')

def poll_and_aggregate(spark, db, dim_zone, last_processed_ts):
    """
    One polling cycle: reads new 2026 Silver records added since last_processed_ts,
    calculates aggregations, and upserts them into tlc_gold_stream.

    Returns the new high-watermark timestamp.
    """
    from functools import reduce
    poll_start = datetime.datetime.utcnow()

    dfs = []
    for coll in SILVER_COLLECTIONS:
        try:
            df = read_mongo(spark, settings.MONGO_DB_SILVER, coll)
            # Filter only 2026 records ingested since last poll
            df = df.filter(
                (F.col('_meta.source_year') == STREAM_YEAR) &
                (F.col('_meta.processed_at') > F.lit(last_processed_ts).cast('timestamp'))
            )
            dfs.append(df)
        except Exception as e:
            print(f'  ✗ Could not read {coll}: {e}')

    if not dfs:
        print('  No Silver collections available.')
        return last_processed_ts

    unified = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    total_new = unified.count()

    if total_new == 0:
        print(f'  No new 2026 records since {last_processed_ts}. Waiting...')
        return last_processed_ts

    print(f'  New records found: {total_new:,}')

    # Enrich with time columns and zone info
    enriched = (
        unified
        .withColumn('pickup_dt',      F.col('datetimes.pickup'))
        .withColumn('vehicle_type',   F.col('_meta.vehicle_type'))
        .withColumn('pickup_zone_id', F.col('locations.pickup.zone_id'))
        .withColumn('year',           F.year('pickup_dt'))
        .withColumn('month',          F.month('pickup_dt'))
        .withColumn('day',            F.dayofmonth('pickup_dt'))
        .withColumn('hour',           F.hour('pickup_dt'))
    )

    # Join zone dimension for human-readable names
    zone_slim = F.broadcast(dim_zone.select(
        F.col('zone_id'),
        F.col('zone_name'),
        F.col('borough'),
    ))
    enriched = enriched.join(zone_slim, F.col('pickup_zone_id') == F.col('zone_id'), 'left').drop('zone_id')

    # ── Demand aggregation ────────────────────────────────────────────────────
    demand_agg = (
        enriched
        .groupBy('vehicle_type', 'year', 'month', 'day', 'hour', 'pickup_zone_id', 'zone_name', 'borough')
        .agg(
            F.count('*').alias('total_trips'),
            F.sum(F.coalesce(F.col('metrics.passenger_count'), F.lit(0))).alias('total_passengers'),
            F.avg(F.coalesce(F.col('datetimes.duration_minutes'), F.lit(0.0))).alias('avg_duration_min'),
        )
        .collect()
    )

    # ── Financial aggregation ─────────────────────────────────────────────────
    fin_agg = (
        enriched
        .groupBy('vehicle_type', 'year', 'month', 'day', 'hour', 'pickup_zone_id', 'zone_name', 'borough')
        .agg(
            F.count('*').alias('total_trips'),
            F.sum(F.coalesce(F.col('financials.total_amount'), F.lit(0.0))).alias('total_revenue'),
            F.avg(F.coalesce(F.col('financials.total_amount'), F.lit(0.0))).alias('avg_revenue_per_trip'),
            F.sum(F.coalesce(F.col('financials.tip_amount'),   F.lit(0.0))).alias('total_tips'),
            F.avg(F.coalesce(F.col('financials.tip_amount'),   F.lit(0.0))).alias('avg_tip'),
            F.sum(F.coalesce(F.col('financials.tolls_amount'), F.lit(0.0))).alias('total_tolls'),
        )
        .collect()
    )

    # Convert Row objects to plain dicts
    demand_rows = [r.asDict() for r in demand_agg]
    fin_rows    = [r.asDict() for r in fin_agg]

    # ── Upsert into tlc_gold_stream ───────────────────────────────────────────
    n_demand = upsert_demand(db, demand_rows)
    n_fin    = upsert_financials(db, fin_rows)

    poll_end = datetime.datetime.utcnow()
    duration = (poll_end - poll_start).total_seconds()
    print(f'  Demand upserts  : {n_demand:,}')
    print(f'  Financial upserts: {n_fin:,}')
    print(f'  Poll duration   : {duration:.1f}s')

    return poll_end.isoformat()


print('poll_and_aggregate() defined.')

# ── Ensure indexes for fast upserts ──────────────────────────────────────────
client = get_mongo_client()
db = client[settings.MONGO_DB_GOLD_STREAM]

INDEX_KEYS = ['vehicle_type', 'year', 'month', 'day', 'hour', 'pickup_zone_id']

for coll_name in ['stream_mart_demand', 'stream_mart_financials']:
    db[coll_name].create_index(
        [(k, 1) for k in INDEX_KEYS],
        unique=True,
        background=True,
        name='lambda_upsert_key'
    )
    print(f'  Index ensured: tlc_gold_stream.{coll_name}')

print('Indexes ready.')


if __name__ == "__main__":
    # ── Main polling loop ─────────────────────────────────────────────────────────
    # Start watermark from the beginning of 2026 on first run.
    # Subsequent runs will pick up from the last high-watermark.
    last_processed_ts = '2026-01-01T00:00:00'
    poll_count = 0
    
    print(f'Starting Gold Stream polling loop...')
    print(f'Polling every {POLL_INTERVAL_SECONDS}s for new 2026 Silver records.')
    print(f'To stop: interrupt the kernel (■ button) or run the stop cell below.')
    print(f'{"-"*60}')
    
    try:
        while True:
            poll_count += 1
            ts_now = datetime.datetime.utcnow().strftime('%H:%M:%S')
            print(f'\n[Poll #{poll_count} @ {ts_now}]')
    
            last_processed_ts = poll_and_aggregate(
                spark, db, dim_zone, last_processed_ts
            )
    
            print(f'  Next poll in {POLL_INTERVAL_SECONDS}s... (high-watermark: {last_processed_ts})')
            time.sleep(POLL_INTERVAL_SECONDS)
    
    except KeyboardInterrupt:
        print(f'\nPolling loop stopped after {poll_count} polls.')
    
