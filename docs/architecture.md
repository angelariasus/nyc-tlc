# Architecture Overview

This project implements a strict **Medallion Architecture** to process the New York City Taxi and Limousine Commission (TLC) dataset. The pipeline is designed to be idempotent, scalable, and fully auditable.

## The Medallion Pattern

1. **Bronze (Raw Ingestion)**: Extracts Parquet files from the TLC CloudFront CDN and loads them directly into MongoDB without structural modification. Lineage metadata (`run_id`, `ingestion_time`) is attached.
2. **Silver (Curated & Enriched)**: Cleanses the data by applying strict business rules. Enriches the data through Broadcast Joins with taxi zone lookup tables. Produces highly standardized nested documents.
3. **Gold (Analytics & Star Schema)**: Transforms the nested Silver documents into a flat **Star Schema** (Fact and Dimension tables) optimized for OLAP (Online Analytical Processing) workloads and BI dashboards.

## Technology Stack Justification

- **Apache PySpark**: Chosen for its distributed memory processing capabilities. Handling hundreds of millions of trip records requires horizontal scaling. PySpark's DataFrame API makes complex aggregations and Broadcast Joins highly efficient.
- **MongoDB**: Unlike traditional file-based Data Lakes (like pure HDFS/S3 with Parquet), using MongoDB as the storage layer across all medallions provides:
  - **Schema Flexibility**: Critical for the Silver layer where different vehicles (Yellow vs FHV) have varying levels of financial data.
  - **Instant Queryability**: Allows analysts to run fast ad-hoc JSON aggregations immediately after ingestion without spinning up Spark clusters.
- **Jupyter Notebooks**: Used as the orchestration medium to allow interactive execution, debugging, and visualization of the data pipeline stages.
