# Architecture Overview

This project implements a strict **Medallion Architecture** to process the New York City Taxi and Limousine Commission (TLC) dataset. The pipeline is designed to be idempotent, scalable, and fully auditable.

## The Medallion Pattern

1. **Bronze (Raw Ingestion)**: Extracts Parquet files from the TLC CloudFront CDN and loads them directly into MongoDB without structural modification. Lineage metadata (`run_id`, `ingestion_time`) is attached.
2. **Silver (Curated & Enriched)**: Cleanses the data by applying strict business rules. Enriches the data through Broadcast Joins with taxi zone lookup tables. Produces highly standardized nested documents.
3. **Gold (Analytics & Star Schema)**: Transforms the nested Silver documents into a flat **Star Schema** (Fact and Dimension tables) optimized for OLAP (Online Analytical Processing) workloads and BI dashboards.

## Processing Models: Kappa Hybrid

The pipeline employs a **Kappa Hybrid Architecture**, blending traditional batch processing with real-time stream processing without duplicating business logic. This is achieved through PySpark Structured Streaming.

### 1. Bulk Batch (Historical Data: 2023-2025)
For massive historical datasets, processing is done in bulk.
- **Workflow**: `[Parquet Files]` → `[Spark Read]` → `[MongoDB Bronze]`
- **Rationale**: Direct bulk ingestion is significantly faster for historical data and avoids saturating the Docker network or Kafka brokers with millions of past events simultaneously.

### 2. Streaming (Live Data: 2026)
For "live" simulation data (representing real-time ingestion), the pipeline shifts to streaming mode.
- **Workflow**: `[Producer: Parquet → Kafka]` → `[Consumer: Spark readStream]` → `[MongoDB Silver]`
- **Rationale**: Demonstrates real-time event processing capabilities. The Spark Consumer uses `foreachBatch` to apply the **exact same** data quality rules (`tlc_rules.py`) and schema definitions (`schema.py`) as the batch process. This is the hallmark of the Kappa architecture: a single codebase handling both paradigms.

## Technology Stack Justification

- **Apache PySpark**: Chosen for its distributed memory processing capabilities. Handling hundreds of millions of trip records requires horizontal scaling. PySpark's DataFrame API makes complex aggregations and Broadcast Joins highly efficient.
- **Apache Kafka**: Serves as the streaming backbone for the 2026 live data, operating in KRaft mode (Zookeeper-less) to conserve memory. It decouples the data producer from the Spark consumer, ensuring fault tolerance and exactly-once processing guarantees.
- **MongoDB**: Unlike traditional file-based Data Lakes (like pure HDFS/S3 with Parquet), using MongoDB as the storage layer across all medallions provides:
  - **Schema Flexibility**: Critical for the Silver layer where different vehicles (Yellow vs FHV) have varying levels of financial data.
  - **Instant Queryability**: Allows analysts to run fast ad-hoc JSON aggregations immediately after ingestion without spinning up Spark clusters.
- **Jupyter Notebooks**: Used as the orchestration medium to allow interactive execution, debugging, and visualization of the data pipeline stages.
