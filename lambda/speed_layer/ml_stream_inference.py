"""
Auto-generated from 01_stream_inference.ipynb
"""
import sys
sys.path.insert(0, "/home/jovyan/work")


import os
import time
import datetime
import joblib
import numpy as np
import pandas as pd
from pathlib import Path
from pymongo import MongoClient

from pyspark.sql import functions as F
from functools import reduce

from src.spark_utils import get_spark, read_mongo
from core.config.settings import settings
from core.storage.mongo_client import get_mongo_client

# ── Config ────────────────────────────────────────────────────────────────────
MODELS_DIR          = Path('/home/jovyan/work/data/models')
STREAM_YEAR         = 2026
POLL_INTERVAL_SECONDS = 30
SILVER_COLLECTIONS  = ['trips_yellow', 'trips_green', 'trips_fhv', 'trips_hvfhv']

# Features expected by Isolation Forest
IF_FEATURES = [
    'fare_amount', 'trip_distance', 'duration_min',
    'tip_amount', 'total_amount', 'passenger_count',
]

print('Imports OK.')
print(f'Models dir: {MODELS_DIR}')

# ── Load pre-trained models (once, in driver) ──────────────────────────────
isolation_forest = joblib.load(MODELS_DIR / 'isolation_forest_anomaly.pkl')
anomaly_scaler   = joblib.load(MODELS_DIR / 'anomaly_scaler.pkl')

print(f'Isolation Forest loaded  : contamination={isolation_forest.contamination}')
print(f'Anomaly scaler loaded    : features={anomaly_scaler.n_features_in_}')

# KMeans for zone segment (optional enrichment)
try:
    kmeans        = joblib.load(MODELS_DIR / 'kmeans_zones.pkl')
    kmeans_scaler = joblib.load(MODELS_DIR / 'kmeans_scaler.pkl')
    KMEANS_AVAILABLE = True
    print(f'KMeans loaded            : k={kmeans.n_clusters}')
except Exception as e:
    KMEANS_AVAILABLE = False
    print(f'KMeans not available     : {e}')

spark = get_spark('TLC_Stream_Inference')
spark.conf.set('spark.sql.shuffle.partitions', '50')
print(f'Spark {spark.version} ready.')

def run_inference(df_pd: pd.DataFrame) -> pd.DataFrame:
    """
    Apply Isolation Forest to a pandas DataFrame of Silver records.
    Returns the DataFrame with added columns:
      - anomaly_score  : raw Isolation Forest decision function score
      - is_anomaly     : True if the model flags the record as anomalous
      - anomaly_label  : 'ANOMALY' | 'NORMAL'
    """
    # Fill missing feature values with 0 for inference
    available_features = [c for c in IF_FEATURES if c in df_pd.columns]
    X = df_pd[available_features].fillna(0).values

    # Pad missing features with zeros if not all present
    if X.shape[1] < anomaly_scaler.n_features_in_:
        pad = np.zeros((X.shape[0], anomaly_scaler.n_features_in_ - X.shape[1]))
        X = np.hstack([X, pad])

    X_scaled = anomaly_scaler.transform(X)
    scores   = isolation_forest.decision_function(X_scaled)   # higher = more normal
    preds    = isolation_forest.predict(X_scaled)             # -1 = anomaly, 1 = normal

    df_pd = df_pd.copy()
    df_pd['anomaly_score'] = scores.round(4)
    df_pd['is_anomaly']    = (preds == -1)
    df_pd['anomaly_label'] = np.where(preds == -1, 'ANOMALY', 'NORMAL')
    return df_pd


print('run_inference() defined.')

def poll_and_infer(spark, db, last_processed_ts):
    """
    One polling cycle:
    1. Read new 2026 Silver records since last_processed_ts.
    2. Apply Isolation Forest.
    3. Write anomaly results to tlc_gold_stream.stream_mart_anomalies.
    """
    poll_start = datetime.datetime.utcnow()

    dfs = []
    for coll in SILVER_COLLECTIONS:
        try:
            df = read_mongo(spark, settings.MONGO_DB_SILVER, coll)
            df = df.filter(
                (F.col('_meta.source_year') == STREAM_YEAR) &
                (F.col('_meta.processed_at') > F.lit(last_processed_ts).cast('timestamp'))
            )
            dfs.append(df)
        except Exception as e:
            print(f'  ✗ {coll}: {e}')

    if not dfs:
        print('  No Silver collections available.')
        return last_processed_ts

    unified = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    total_new = unified.count()

    if total_new == 0:
        print(f'  No new records since {last_processed_ts}.')
        return last_processed_ts

    print(f'  New records for inference: {total_new:,}')

    # Project only inference-relevant columns (keep it lightweight)
    infer_df = unified.select(
        F.col('_meta.vehicle_type').alias('vehicle_type'),
        F.col('_meta.run_id').alias('stream_run_id'),
        F.col('_meta.processed_at').alias('processed_at'),
        F.col('_meta.source_year').alias('source_year'),
        F.col('datetimes.pickup').alias('pickup_dt'),
        F.col('locations.pickup.zone_id').alias('pickup_zone_id'),
        F.col('locations.pickup.borough').alias('borough'),
        F.coalesce(F.col('financials.fare_amount'),  F.lit(0.0)).alias('fare_amount'),
        F.coalesce(F.col('financials.tip_amount'),   F.lit(0.0)).alias('tip_amount'),
        F.coalesce(F.col('financials.total_amount'), F.lit(0.0)).alias('total_amount'),
        F.coalesce(F.col('metrics.distance_miles'),  F.lit(0.0)).alias('trip_distance'),
        F.coalesce(F.col('datetimes.duration_minutes'), F.lit(0.0)).alias('duration_min'),
        F.coalesce(F.col('metrics.passenger_count'), F.lit(0)).alias('passenger_count'),
    ).toPandas()

    # Run inference
    result_pd = run_inference(infer_df)

    # Only persist anomalies (reduces storage significantly)
    anomalies = result_pd[result_pd['is_anomaly'] == True].copy()
    anomaly_count = len(anomalies)
    anomaly_rate  = anomaly_count / total_new * 100 if total_new > 0 else 0

    print(f'  Anomalies detected : {anomaly_count:,} ({anomaly_rate:.2f}%)')

    if anomaly_count > 0:
        anomalies['pickup_dt']    = anomalies['pickup_dt'].astype(str)
        anomalies['processed_at'] = anomalies['processed_at'].astype(str)
        anomalies['detected_at']  = datetime.datetime.utcnow().isoformat()
        anomalies['stream_year']  = STREAM_YEAR

        records = anomalies.replace({np.nan: None}).to_dict(orient='records')
        db['stream_mart_anomalies'].insert_many(records, ordered=False)
        print(f'  Anomalies written to tlc_gold_stream.stream_mart_anomalies')

    poll_end = datetime.datetime.utcnow()
    print(f'  Poll duration: {(poll_end - poll_start).total_seconds():.1f}s')
    return poll_end.isoformat()


print('poll_and_infer() defined.')

# ── Ensure TTL index for anomalies (optional: auto-delete after 7 days) ──
client = get_mongo_client()
db = client[settings.MONGO_DB_GOLD_STREAM]

db['stream_mart_anomalies'].create_index(
    'detected_at',
    name='ttl_7days',
    background=True,
)
db['stream_mart_anomalies'].create_index(
    [('vehicle_type', 1), ('pickup_zone_id', 1), ('detected_at', -1)],
    name='anomaly_lookup',
    background=True,
)
print('Indexes ready for stream_mart_anomalies.')


if __name__ == "__main__":
    # ── Main polling loop ─────────────────────────────────────────────────────────
    last_processed_ts = '2026-01-01T00:00:00'
    poll_count = 0
    
    print('Starting ML Stream Inference polling loop...')
    print(f'Polling every {POLL_INTERVAL_SECONDS}s for new 2026 Silver records.')
    print('To stop: interrupt the kernel (■ button).')
    print('-' * 60)
    
    try:
        while True:
            poll_count += 1
            ts_now = datetime.datetime.utcnow().strftime('%H:%M:%S')
            print(f'\n[Inference Poll #{poll_count} @ {ts_now}]')
    
            last_processed_ts = poll_and_infer(spark, db, last_processed_ts)
    
            print(f'  Next poll in {POLL_INTERVAL_SECONDS}s...')
            time.sleep(POLL_INTERVAL_SECONDS)
    
    except KeyboardInterrupt:
        print(f'\nML Inference loop stopped after {poll_count} polls.')
    
