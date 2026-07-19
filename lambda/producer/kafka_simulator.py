"""
Auto-generated from 01_kafka_producer.ipynb
"""
import sys
sys.path.insert(0, "/home/jovyan/work")

import os
from pathlib import Path
from core.config.settings import settings
import concurrent.futures

# -- Kafka configuration -------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# -- Producer tuning -----------------------------------------------------------
BATCH_SIZE      = 500     # records per Kafka send batch
DELAY_SECONDS   = 0.3     # pause between batches (visual streaming effect)
MAX_RECORDS     = None    # None = send all; set an int to cap for quick demos
YEAR_2026       = 2026

VEHICLES = ["yellow", "green", "fhv", "hvfhv"]
VEHICLE_TYPE = "all"
if len(sys.argv) > 1:
    VEHICLE_TYPE = sys.argv[1].lower()
    
# -- Select topic by vehicle type ----------------------------------------------
topic_map = {
    "yellow": os.getenv("KAFKA_TOPIC_YELLOW_2026", "tlc-yellow-2026"),
    "green":  os.getenv("KAFKA_TOPIC_GREEN_2026",  "tlc-green-2026"),
    "fhv":    os.getenv("KAFKA_TOPIC_FHV_2026",    "tlc-fhv-2026"),
    "hvfhv":  os.getenv("KAFKA_TOPIC_HVFHV_2026",  "tlc-hvfhv-2026"),
}

target_vehicles = VEHICLES if VEHICLE_TYPE == "all" else [VEHICLE_TYPE]

print(f"Kafka bootstrap : {KAFKA_BOOTSTRAP}")
print(f"Target vehicles : {target_vehicles}")
print(f"Batch size      : {BATCH_SIZE:,} records")
print(f"Delay           : {DELAY_SECONDS}s between batches")
print(f"Max records     : {'All' if MAX_RECORDS is None else MAX_RECORDS:,}")

# Install kafka-python if not already present in the container
import subprocess
subprocess.run([sys.executable, "-m", "pip", "install", "kafka-python", "-q"], check=True)

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import pandas as pd
import json
import time
import datetime

admin_client = KafkaAdminClient(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    client_id="tlc_admin",
    request_timeout_ms=10_000,
)

# Create topics for all target vehicles
for vt in target_vehicles:
    topic_name = topic_map[vt]
    topic_config = NewTopic(name=topic_name, num_partitions=4, replication_factor=1)
    try:
        admin_client.create_topics([topic_config], validate_only=False)
        print(f"? Topic '{topic_name}' created")
    except TopicAlreadyExistsError:
        print(f"? Topic '{topic_name}' already exists")
    except Exception as e:
        print(f" Topic creation warning for {topic_name}: {e}")
admin_client.close()

def json_default(obj):
    if isinstance(obj, (pd.Timestamp, datetime.datetime, datetime.date)):
        return obj.isoformat()
    if hasattr(obj, "item"):
        return obj.item()
    raise TypeError(f"Type {type(obj)} not serialisable")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, default=json_default).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    acks="all",
    retries=3,
    batch_size=16_384,
    linger_ms=50,
    compression_type="gzip",
    max_request_size=10_485_760,
    request_timeout_ms=30_000,
)
print(f"? KafkaProducer connected to {KAFKA_BOOTSTRAP}")

def stream_vehicle(vt):
    topic = topic_map[vt]
    raw_dir = settings.RAW_DIR / vt
    files_2026 = sorted(raw_dir.glob(f"{vt}_tripdata_{YEAR_2026}-*.parquet"))
    
    if not files_2026:
        print(f"[{vt.upper()}] No 2026 Parquet files found. Skipping.")
        return 0, 0

    total_sent = 0
    total_errors = 0
    stream_start = time.time()
    
    for parquet_file in files_2026:
        df = pd.read_parquet(parquet_file)
        if MAX_RECORDS is not None:
            df = df.head(MAX_RECORDS)
            
        records = df.to_dict(orient="records")
        batch_num = 0
        
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]
            batch_num += 1
            
            for record in batch:
                record["_stream_meta"] = {
                    "produced_at":  datetime.datetime.utcnow().isoformat(),
                    "source_file":  parquet_file.name,
                    "vehicle_type": vt,
                    "batch_id":     batch_num,
                }
                try:
                    producer.send(topic=topic, key=vt, value=record)
                    total_sent += 1
                except Exception as exc:
                    total_errors += 1
            producer.flush()
            
            elapsed = time.time() - stream_start
            rec_per_s = total_sent / elapsed if elapsed > 0 else 0
            
            # Solo imprimimos progreso cada ciertos batches para no saturar consola si hay multiples hilos
            if batch_num % 10 == 0:
                print(f"[{vt.upper()}] Sent: {total_sent:>8,} | {rec_per_s:,.0f} rec/s")
                
            time.sleep(DELAY_SECONDS)
            
    print(f"[{vt.upper()}] COMPLETED. Sent {total_sent:,} records to {topic}.")
    return total_sent, total_errors

# Stream concurrently if multiple vehicles
if __name__ == "__main__":
    global_start = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(target_vehicles)) as executor:
        futures = {executor.submit(stream_vehicle, vt): vt for vt in target_vehicles}
        for future in concurrent.futures.as_completed(futures):
            vt = futures[future]
            try:
                sent, errors = future.result()
            except Exception as exc:
                print(f"[{vt.upper()}] generated an exception: {exc}")

    elapsed_total = time.time() - global_start
    print(f"\n{'=' * 60}")
    print(f"ALL STREAMING COMPLETE ({elapsed_total:.1f}s)")
    print(f"{'=' * 60}")
    
    producer.flush(timeout=30)
    producer.close()

