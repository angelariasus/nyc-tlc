# NYC TLC Medallion Data Lake Documentation

Welcome to the official documentation for the **NYC TLC Trip Record Data Lake** project. This documentation serves as a comprehensive guide to understanding the architecture, data processing logic, and business integrations of the platform.

## Table of Contents

### 1. General Overview
- [Architecture Overview](architecture.md): High-level system design, Medallion pattern explanation, and technology stack justification.
- [Data Quality & Audit](data_quality.md): The `ControlManager` framework, quarantine mechanisms, and rule engines.
- [BI Integration](bi_integration.md): How to connect reporting tools (PowerBI, Tableau) to the analytical Gold layer.

### 2. Medallion Layers Detailed
- [Bronze Layer](layers/bronze.md): Raw data ingestion, file download processes, and EL (Extract & Load) logic.
- [Silver Layer](layers/silver.md): Cleansing, zone enrichment, and nested schema standardization across different vehicle types.
- [Gold Layer](layers/gold.md): The analytical Star Schema (Facts and Dimensions) and business KPI calculations.

## Quick Start
To get started with the data pipeline locally:
```bash
# 1. Start the MongoDB and PySpark containers
docker compose up -d

# 2. Open Jupyter locally and run notebooks sequentially
# http://localhost:8100/
```
