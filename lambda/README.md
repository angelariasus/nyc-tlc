# TLC Lambda Architecture

This directory contains the **Speed Layer** of the NYC TLC Data Lakehouse, implemented entirely in pure Python.

While the historical batch processing (the Medallion Architecture for 2019–2025 data) resides in `/notebooks` and `/src`, this folder contains the real-time processing pipelines for the 2026 data stream.

## Structure

### `producer/`
- **`download_stream_data.py`**: A helper script to selectively download only 2026 data.
- **`kafka_simulator.py`**: Reads local Parquet files (2026 data) and publishes them as JSON streams to Kafka topics (`tlc-yellow-2026`, etc.) to simulate live taxi trips.

### `speed_layer/`
- **`silver_stream_*.py`**: Four PySpark Structured Streaming consumers (one for each vehicle type). They subscribe to the Kafka topics, apply the same data quality rules as the batch Bronze layer, and stream valid records into the `tlc_silver` MongoDB database.
- **`gold_stream_marts.py`**: A PySpark micro-batch polling script that reads newly arrived validated records from `tlc_silver` and performs *Upsert* operations into `tlc_gold_stream`. It maintains real-time KPIs (demand and financials).
- **`ml_stream_inference.py`**: A PySpark micro-batch script that applies pre-trained Machine Learning models (Isolation Forest for anomaly detection) on the incoming stream. It writes anomalies to `tlc_gold_stream`.

## How to Run the Lambda Architecture

To see the real-time pipeline and dashboard in action:

1. **Start the Producer (Simulator)**
   By default, it uses a ThreadPool to simulate all 4 vehicle types (`yellow`, `green`, `fhv`, `hvfhv`) concurrently:
   ```bash
   python lambda/producer/kafka_simulator.py
   ```
   *(To test a single type, you can pass it as an argument: `python lambda/producer/kafka_simulator.py yellow`)*
2. **Start the Silver Consumers** (You can run these in separate terminals)
   ```bash
   python lambda/speed_layer/silver_stream_yellow.py
   python lambda/speed_layer/silver_stream_green.py
   ```
3. **Start the Gold & ML Speed Layers**
   ```bash
   python lambda/speed_layer/gold_stream_marts.py
   python lambda/speed_layer/ml_stream_inference.py
   ```
4. **Launch the Dashboard**
   ```bash
   streamlit run src/dashboard/app.py
   ```
   *Navigate to the **Lambda Live (2026)** page to see the real-time charts auto-updating every 10 seconds!*

