import streamlit as st
import pandas as pd
import plotly.express as px
import os
from pymongo import MongoClient

st.set_page_config(page_title="Audit Control", page_icon="🛡️", layout="wide")
st.title("🛡️ Data Observability: Audit & Quarantine Control")
st.markdown("Monitor pipeline health and inspect rejected records in real-time.")

# --- MONGODB CONNECTION ---
@st.cache_resource
def get_db():
    mongo_host = os.getenv("MONGO_HOST", "mongodb")
    mongo_port = int(os.getenv("MONGO_PORT", 27017))
    mongo_user = os.getenv("MONGO_USER", "admin")
    mongo_password = os.getenv("MONGO_PASSWORD", "password123")
    
    client = MongoClient(
        host=mongo_host,
        port=mongo_port,
        username=mongo_user,
        password=mongo_password
    )
    return client["tlc_audit"]

db = get_db()

# --- DATA FETCHING ---
def fetch_pipeline_runs():
    cursor = db["pipeline_runs"].find().sort("start_time", -1).limit(50)
    docs = list(cursor)
    if not docs:
        return pd.DataFrame()
    
    # Flatten the output_summary for easier plotting
    flat_docs = []
    for d in docs:
        flat = {
            "execution_id": d.get("execution_id"),
            "pipeline_name": d.get("pipeline_name"),
            "status": d.get("status"),
            "start_time": pd.to_datetime(d.get("start_time")),
            "mode": d.get("mode", "batch"),
        }
        summary = d.get("output_summary", {})
        flat["records_read"] = summary.get("records_read_from_bronze", 0)
        flat["records_written"] = summary.get("records_written_to_silver", 0)
        flat["records_quarantined"] = summary.get("records_quarantined", 0)
        flat["quarantine_rate_pct"] = summary.get("quarantine_rate_pct", 0)
        flat["throughput"] = summary.get("throughput_records_per_s", 0)
        flat_docs.append(flat)
    return pd.DataFrame(flat_docs)

def fetch_quarantine_reasons():
    pipeline = [
        {"$group": {"_id": "$reason", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    docs = list(db["quarantine"].aggregate(pipeline))
    return pd.DataFrame(docs).rename(columns={"_id": "Reason", "count": "Count"})

def fetch_quarantine_samples():
    cursor = db["quarantine"].find({}, {"_id": 0}).sort("quarantined_at", -1).limit(10)
    return list(cursor)

# --- UI LAYOUT ---
df_runs = fetch_pipeline_runs()

if df_runs.empty:
    st.warning("No pipeline runs found in the database yet. Run the Bronze or Silver notebooks.")
else:
    # 1. KPIs
    st.subheader("📊 Global KPIs (Last 50 Runs)")
    col1, col2, col3 = st.columns(3)
    
    avg_quarantine = df_runs["quarantine_rate_pct"].mean()
    total_processed = df_runs["records_read"].sum()
    avg_throughput = df_runs[df_runs["mode"] == "streaming_kafka"]["throughput"].mean()
    
    col1.metric("Total Records Processed", f"{total_processed:,.0f}")
    col2.metric("Avg Quarantine Rate", f"{avg_quarantine:.2f}%", 
                delta="Warning" if avg_quarantine > 2 else "Healthy", 
                delta_color="inverse")
    
    if pd.isna(avg_throughput):
        col3.metric("Avg Streaming Throughput", "N/A (No streams yet)")
    else:
        col3.metric("Avg Streaming Throughput", f"{avg_throughput:,.0f} rec/s")
    
    st.markdown("---")

    # 2. Charts
    colA, colB = st.columns(2)
    
    with colA:
        st.subheader("📈 Streaming Throughput over Time")
        df_stream = df_runs[df_runs["mode"] == "streaming_kafka"].sort_values("start_time")
        if df_stream.empty:
            st.info("No streaming runs recorded yet.")
        else:
            fig1 = px.line(df_stream, x="start_time", y="throughput", markers=True, 
                           title="Throughput (Records/Sec) - Kafka Consumer")
            st.plotly_chart(fig1, use_container_width=True)

    with colB:
        st.subheader("🛑 Top 5 Rejection Reasons")
        df_reasons = fetch_quarantine_reasons()
        if df_reasons.empty:
            st.success("No rejected records found! Data quality is 100%.")
        else:
            fig2 = px.bar(df_reasons, x="Count", y="Reason", orientation='h',
                          title="Quarantine Frequency by Reason", color="Count",
                          color_continuous_scale="Reds")
            fig2.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig2, use_container_width=True)
            
    st.markdown("---")
    
    # 3. Raw Inspector
    st.subheader("🔍 Quarantine Raw Inspector")
    st.markdown("Inspect the latest records that failed validation rules.")
    samples = fetch_quarantine_samples()
    if not samples:
        st.info("No records in quarantine.")
    else:
        # Create a clean dataframe for display
        display_data = []
        for s in samples:
            display_data.append({
                "Quarantined At": s.get("quarantined_at"),
                "Pipeline": s.get("pipeline"),
                "Source File": s.get("source_file"),
                "Reason": s.get("reason"),
                "Raw Data Preview": str(s.get("raw_record", {}))[:100] + "..."
            })
        st.dataframe(pd.DataFrame(display_data), use_container_width=True)
        
        # Expander for full JSON view
        with st.expander("View Full JSON of Latest Rejected Record"):
            st.json(samples[0])
