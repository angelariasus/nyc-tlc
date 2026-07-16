import streamlit as st
import os

st.set_page_config(
    page_title="TLC Analytics Platform",
    page_icon="🚕",
    layout="wide",
)

st.title("🚖 TLC Analytics Platform")
st.markdown("---")

st.markdown("""
### Welcome to the Data Observability Hub
This platform monitors the health, quality, and performance of the NYC Taxi & Limousine Commission (TLC) Data Lakehouse.

👈 **Please select a dashboard from the sidebar.**

---

#### 📌 Available Dashboards:

1. **Audit & Data Quality Control (`1_Audit_Control.py`)**
   - **Pipeline Health**: Monitor throughput and execution times for both Batch and Streaming (Kappa) pipelines.
   - **Data Quarantine**: Analyze rejection rates, top failure reasons, and inspect raw rejected records in real time.

2. **Time Series Forecasting (Coming Soon)**
   - SARIMA model predictions for hourly taxi demand.

3. **Spatial Segmentation (Coming Soon)**
   - K-Means clustering for high-demand pickup zones.

4. **Classification (Coming Soon)**
   - Random Forest predictions for high-tip probability.

---
*System Architecture: PySpark Structured Streaming + MongoDB + Kafka KRaft + Streamlit*
""")

# Check MongoDB connection silently to ensure the container environment variables are present
try:
    from pymongo import MongoClient
    mongo_host = os.getenv("MONGO_HOST", "mongodb")
    mongo_port = int(os.getenv("MONGO_PORT", 27017))
    mongo_user = os.getenv("MONGO_USER", "admin")
    mongo_password = os.getenv("MONGO_PASSWORD", "password123")
    
    client = MongoClient(
        host=mongo_host,
        port=mongo_port,
        username=mongo_user,
        password=mongo_password,
        serverSelectionTimeoutMS=2000
    )
    client.admin.command('ping')
    st.sidebar.success("🟢 Connected to MongoDB")
except Exception as e:
    st.sidebar.error(f"🔴 MongoDB Connection Failed: {e}")
