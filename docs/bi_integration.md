# BI Integration

The Gold Layer is designed explicitly as a **Star Schema** to provide seamless integration with standard Business Intelligence (BI) tools such as Microsoft Power BI, Tableau, and Apache Superset.

## Connecting Power BI to MongoDB

Because the data resides in MongoDB `tlc_gold`, there are two primary ways to consume this data in BI tools:

### Option 1: MongoDB BI Connector (Recommended for relational tools)
The MongoDB BI Connector acts as a SQL proxy. It translates SQL queries from Power BI into MongoDB aggregation pipelines.
1. Install the MongoDB ODBC Driver for BI Connector on your workstation.
2. In Power BI, select **Get Data -> ODBC**.
3. Point the DSN to your MongoDB BI Connector instance.
4. The collections `fact_trips`, `dim_date`, `dim_zone`, etc., will appear as standard relational SQL tables.

### Option 2: Direct PySpark / Databricks connection
If you are running a Spark thrift server or connecting Power BI directly to the PySpark cluster:
1. PySpark can read the `tlc_gold` collections using the Mongo Spark Connector.
2. Expose the DataFrames as global temporary views.
3. Connect Power BI to the Spark Thrift Server via the Spark connector.

## Star Schema Usage

When building reports, always use `tlc_gold.fact_trips` as the central Fact table.
- Join `fact_trips.pickup_zone_id` ➔ `dim_zone.zone_id` to get Borough and Zone names.
- Join `fact_trips.pickup_date_id` ➔ `dim_date.date_id` to aggregate by Day of Week or Month.
- Join `fact_trips.vehicle_id` ➔ `dim_vehicle.id` to separate Uber/Lyft metrics from traditional Taxis.

By relying on the predefined relationships in the Star Schema, BI tools can efficiently calculate complex aggregations like "Revenue by Borough and Vehicle Type" with minimal processing overhead.
