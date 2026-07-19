"""
Auto-generated from 01_kafka_producer.ipynb
"""
import sys
sys.path.insert(0, "/home/jovyan/work")


import os
from pathlib import Path
from core.config.settings import settings
from src import paths as project_paths

# ── Kafka configuration ───────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_YELLOW    = os.getenv("KAFKA_TOPIC_YELLOW_2026", "tlc-yellow-2026")
TOPIC_GREEN     = os.getenv("KAFKA_TOPIC_GREEN_2026",  "tlc-green-2026")

# ── Producer tuning ───────────────────────────────────────────────────────────
BATCH_SIZE      = 500     # records per Kafka send batch
DELAY_SECONDS   = 0.3     # pause between batches (visual streaming effect)
MAX_RECORDS     = None    # None = send all; set an int to cap for quick demos
VEHICLE_TYPE    = "yellow"  # The consumer (02b) is designed specifically for Yellow data
YEAR_2026       = 2026

# ── Select topic by vehicle type ──────────────────────────────────────────────
KAFKA_TOPIC = TOPIC_YELLOW if VEHICLE_TYPE == "yellow" else TOPIC_GREEN

print(f"Kafka bootstrap : {KAFKA_BOOTSTRAP}")
print(f"Kafka topic     : {KAFKA_TOPIC}")
print(f"Batch size      : {BATCH_SIZE:,} records")
print(f"Delay           : {DELAY_SECONDS}s between batches")
print(f"Max records     : {'All' if MAX_RECORDS is None else MAX_RECORDS:,}")

# Install kafka-python if not already present in the container
import subprocess
subprocess.run([sys.executable, "-m", "pip", "install", "kafka-python", "-q"], check=True)
print("kafka-python ready.")

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import pandas as pd
import json
import time
import datetime
import uuid

print("All imports OK.")

admin_client = KafkaAdminClient(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    client_id="tlc_admin",
    request_timeout_ms=10_000,
)

topic_config = NewTopic(
    name=KAFKA_TOPIC,
    num_partitions=4,
    replication_factor=1,
)

try:
    admin_client.create_topics([topic_config], validate_only=False)
    print(f"✓ Topic '{KAFKA_TOPIC}' created (4 partitions, RF=1)")
except TopicAlreadyExistsError:
    print(f"✓ Topic '{KAFKA_TOPIC}' already exists — OK")
except Exception as e:
    print(f" Topic creation warning (may already exist): {e}")
finally:
    admin_client.close()

# Discover all 2026 parquet files for the selected vehicle type
raw_dir = settings.RAW_DIR / VEHICLE_TYPE
files_2026 = sorted(raw_dir.glob(f"{VEHICLE_TYPE}_tripdata_{YEAR_2026}-*.parquet"))

if not files_2026:
    raise FileNotFoundError(
        f"No 2026 Parquet files found in {raw_dir}.\n"
        "Run notebook 00_setup_download.ipynb first to download 2026 data."
    )

print(f"Found {len(files_2026)} file(s) for {VEHICLE_TYPE} 2026:")
for f in files_2026:
    size_mb = f.stat().st_size / 1_048_576
    print(f"  {f.name}  ({size_mb:.1f} MB)")

def json_default(obj):
    """Custom JSON serializer for types not handled by default."""
    if isinstance(obj, (pd.Timestamp, datetime.datetime, datetime.date)):
        return obj.isoformat()
    if hasattr(obj, 'item'):  # numpy scalar
        return obj.item()
    raise TypeError(f"Type {type(obj)} not serialisable")


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    # Serialise each record as JSON bytes
    value_serializer=lambda v: json.dumps(v, default=json_default).encode("utf-8"),
    # Key = vehicle_type string (enables partition routing)
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    # Producer performance settings
    acks="all",            # wait for all in-sync replicas
    retries=3,
    batch_size=16_384,     # 16 KB batch
    linger_ms=50,          # wait up to 50ms to fill batch
    compression_type="gzip",
    max_request_size=10_485_760,   # 10 MB max
    request_timeout_ms=30_000,
)

print(f"✓ KafkaProducer connected to {KAFKA_BOOTSTRAP}")

total_sent   = 0
total_errors = 0
stream_start = time.time()

for parquet_file in files_2026:
    print(f"\n{'='*60}")
    print(f"Streaming: {parquet_file.name}")
    print(f"{'='*60}")

    # Read into Pandas (fast for single-file ingestion)
    df = pd.read_parquet(parquet_file)

    # Cap records if demo mode
    if MAX_RECORDS is not None:
        df = df.head(MAX_RECORDS)

    total_records = len(df)
    print(f"Records to stream: {total_records:,}")

    # Convert to list of dicts once (fast iteration)
    records = df.to_dict(orient="records")

    batch_num = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        batch_num += 1

        for record in batch:
            # Inject streaming metadata into each record
            record["_stream_meta"] = {
                "produced_at":  datetime.datetime.utcnow().isoformat(),
                "source_file":  parquet_file.name,
                "vehicle_type": VEHICLE_TYPE,
                "batch_id":     batch_num,
            }

            try:
                producer.send(
                    topic=KAFKA_TOPIC,
                    key=VEHICLE_TYPE,
                    value=record,
                )
                total_sent += 1
            except Exception as exc:
                total_errors += 1
                print(f"    ✗ Send error: {exc}")

        # Flush after each batch
        producer.flush()

        elapsed   = time.time() - stream_start
        rec_per_s = total_sent / elapsed if elapsed > 0 else 0
        pct_done  = (i + len(batch)) / total_records * 100

        print(
            f"  Batch {batch_num:4d} | Sent: {total_sent:>8,} | "
            f"{pct_done:5.1f}% | {rec_per_s:,.0f} rec/s",
            end="\r",
        )

        # Pause between batches for visual streaming effect
        time.sleep(DELAY_SECONDS)

    print()  # newline after \r progress
    print(f"  ✓ File complete: {total_records:,} records streamed from {parquet_file.name}")

print(f"\n{'='*60}")
print(f"  STREAMING COMPLETE")
print(f"{'='*60}")
elapsed_total = time.time() - stream_start
print(f"  Topic          : {KAFKA_TOPIC}")
print(f"  Records sent   : {total_sent:,}")
print(f"  Errors         : {total_errors:,}")
print(f"  Duration       : {elapsed_total:.1f}s")
print(f"  Avg throughput : {total_sent / elapsed_total:,.0f} records/s")
print(f"\n→ Check Kafka UI at http://localhost:8090 to inspect the topic.")
print(f"→ The Consumer (02b_silver_stream_yellow) is processing these records in real time.")

producer.flush(timeout=30)
producer.close()
print("✓ KafkaProducer closed gracefully.")
print(f"  Total records in topic '{KAFKA_TOPIC}': {total_sent:,}")
print(f"\nMonitor topic offset at: http://localhost:8090/ui/clusters/tlc-cluster/topics/{KAFKA_TOPIC}")

