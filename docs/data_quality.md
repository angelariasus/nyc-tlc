# Data Quality & Audit Framework

Data reliability is enforced through a strict quality rules engine and a robust audit tracking mechanism known as the **ControlManager**.

## The ControlManager

Every notebook execution creates an active session using the `ControlManager` class (`core.audit.control_manager.py`).
- **Initialization**: Assigns a unique UUID (`execution_id`) for the run.
- **Tracking**: Logs how many records were read, written, and rejected.
- **Persistence**: Upon completion, the audit report is saved locally as a JSON backup (`data/audit/executions/`) and pushed to the `tlc_audit.pipeline_runs` collection in MongoDB.

## Data Quality Rules (`tlc_rules.py`)

During the transition from **Bronze to Silver**, PySpark DataFrames are evaluated against predicates. Records that fail **any** rule are diverted away from the Silver layer and routed to the `tlc_audit.quarantine` collection for further investigation.

### Yellow & Green Taxi Rules
- `no_zero_distance`: Trip distance > 0.
- `no_negative_fare`: Fare amount >= 0.
- `valid_pickup_location`: PULocationID between 1 and 265.
- `valid_dropoff_location`: DOLocationID between 1 and 265.
- `valid_time_order`: Dropoff datetime > Pickup datetime.
- `valid_passenger_count`: Passenger count between 1 and 8.

### FHV Rules
- `valid_pickup_location`, `valid_dropoff_location`, `valid_time_order`.

### HVFHV Rules
- `valid_pickup_location`, `valid_dropoff_location`, `valid_time_order`.
- `no_negative_pay`: Driver pay and base passenger fare >= 0.

By capturing bad data in the `quarantine` collection instead of silently dropping it, data engineers can analyze anomaly trends over time without polluting the analytical Silver/Gold layers.
