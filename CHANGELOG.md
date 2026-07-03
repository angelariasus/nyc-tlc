# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Silver Layer Extension**: Support for all TLC vehicle types (Yellow, Green, FHV, HVFHV).
  - Specific quality rules (`FHV_RULES`, `HVFHV_RULES`) for non-medallion vehicles.
  - Dynamic schemas (`build_fhv_silver`, `build_hvfhv_silver`) that fill missing financial data with nulls to maintain a unified nested schema.
- **Gold Layer Star Schema**: Full architectural upgrade to support OLAP and BI tools.
  - Creation of static lookup dimensions (`dim_zone`, `dim_date`, `dim_vehicle`, `dim_vendor`, `dim_rate_code`, `dim_payment_type`).
  - Consolidation of all Silver tables into a single unified `fact_trips` table using foreign keys.
  - Refactored `08_gold_metrics.ipynb` to aggregate over the Star Schema.

### Changed
- **Directory Structure Refactoring**: Organized codebase to strictly follow Medallion architecture layers.
  - Notebooks are now nested inside `notebooks/bronze/`, `notebooks/silver/`, `notebooks/gold/`, and `notebooks/analysis/`.
  - Moved transformation rules from `src/transformations` to `src/silver`.
  - Updated all internal notebook imports to reflect the new `src.silver` path.
- **Pipeline Orchestration**: Notebooks sequentially renamed from `00` to `09` for clear execution order.

## [0.1.0] - Initial Setup
### Added
- Project structure initialized (core, src, tests, notebooks).
- Custom `ControlManager` for end-to-end pipeline auditing in MongoDB.
- Docker compose stack with MongoDB 7.0 and Jupyter PySpark with Mongo connector.
- Yellow Taxi ingestion pipeline (Bronze and Silver layers).
