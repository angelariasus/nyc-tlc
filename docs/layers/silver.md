# Silver Layer (Curated & Enriched)

The Silver layer acts as the unified, highly structured, and cleansed version of the trip data. It takes the disparate raw schemas from Bronze and forces them into a standardized Document schema.

## Process Flow

The Silver pipeline is executed via dedicated notebooks for each vehicle type (`02` to `05`). The execution follows three main stages:

### 1. Data Quality Filtering
PySpark evaluates the Bronze records against predefined rules in `src.silver.tlc_rules`. 
- Valid records proceed to enrichment.
- Invalid records are intercepted and written to the `tlc_audit.quarantine` collection in MongoDB along with the exact `_rejection_reason`.

### 2. Zone Enrichment (Broadcast Join)
Raw data only contains `PULocationID` and `DOLocationID` (integers 1-265).
- PySpark loads the `taxi_zone_lookup.csv`.
- A **Broadcast Join** is performed to attach the `borough`, `zone`, and `service_zone` string values directly to the pickup and dropoff locations.

### 3. Schema Standardization (`src.silver.schema`)
The enriched data is mapped into a nested JSON structure for MongoDB. To handle the asymmetrical nature of the vehicle types:
- **Medallion Cabs (Yellow/Green)**: Have rich financial data (`fare_amount`, `tip_amount`, `tolls`).
- **FHV**: Has almost no financial data, only times and locations.
- **Null Filling**: For FHV and HVFHV, missing standard fields in the `metrics` and `financials` structs are safely populated with `null` values. This ensures that the Silver schema is absolutely identical across all collections, allowing for seamless unifications in the Gold layer.

## Persistence

Data is written in append mode to the `tlc_silver` database:
- `tlc_silver.trips_yellow`
- `tlc_silver.trips_green`
- `tlc_silver.trips_fhv`
- `tlc_silver.trips_hvfhv`
