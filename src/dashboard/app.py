import streamlit as st
import os

st.set_page_config(
    page_title="TLC Analytics Platform",
    page_icon="🚕",
    layout="wide",
)

# ── Custom CSS — Dark premium theme ───────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .stApp { background: linear-gradient(135deg, #0a0e1a 0%, #0f1729 50%, #0a1628 100%); color: #e2e8f0; }
    .main-title {
        font-size: 2.8rem; font-weight: 700;
        background: linear-gradient(90deg, #f6c90e, #ff6b35, #e040fb);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        margin-bottom: 0.2rem;
    }
    .subtitle { color: #64748b; font-size: 1rem; margin-bottom: 2rem; }
    .page-card {
        background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px; padding: 1.5rem 2rem; margin-bottom: 1rem; transition: border-color 0.3s;
    }
    .page-card:hover { border-color: rgba(246,201,14,0.4); }
    .page-card h3 { color: #f6c90e; margin-bottom: 0.5rem; }
    .page-card p  { color: #94a3b8; margin: 0; font-size: 0.9rem; }
    .badge-live {
        display: inline-block; background: rgba(16,185,129,0.15); border: 1px solid #10b981;
        color: #10b981; border-radius: 20px; padding: 2px 10px; font-size: 0.75rem; font-weight: 600; margin-left: 8px;
    }
    .arch-box {
        background: rgba(246,201,14,0.05); border: 1px solid rgba(246,201,14,0.2);
        border-radius: 12px; padding: 1.2rem 1.5rem; margin-top: 1.5rem; color: #94a3b8; font-size: 0.88rem;
    }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="main-title">🚕 NYC TLC Real-Time Analytics</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Lambda Architecture · Speed Layer (2026)</div>', unsafe_allow_html=True)

st.markdown("""
<div class="page-card">
    <h3>⚡ Lambda Live Dashboard <span class="badge-live">SPEED LAYER</span></h3>
    <p>Visualización en tiempo real del tráfico de Nueva York (2026). Monitorea viajes, 
    ingresos y anomalías detectadas por Machine Learning (Isolation Forest) mientras Kafka produce datos en vivo. 
    Selecciona la página en el menú lateral para iniciar.</p>
</div>
""", unsafe_allow_html=True)

# ── Architecture diagram ───────────────────────────────────────────────────────
st.markdown("""
<div class="arch-box">
<strong style="color:#f6c90e">Arquitectura Lambda — Flujo de Streaming:</strong><br>
<code style="color:#a5b4fc">
[Simulador Parquet] → Kafka Producer → 4 Topics (yellow/green/fhv/hvfhv)<br>
  ↓ <br>
  Silver Streams × 4 → tlc_silver → Gold Stream Marts + ML Inference → tlc_gold_stream<br>
  ↓ <br>
  Streamlit: Combina datos históricos (Power BI/tlc_gold) con la inyección en vivo.
</code>
</div>
""", unsafe_allow_html=True)

# ── Sidebar: connection status ─────────────────────────────────────────────────
st.sidebar.markdown("## 🔌 System Status")
try:
    from pymongo import MongoClient
    client = MongoClient(
        host=os.getenv("MONGO_HOST", "mongodb"),
        port=int(os.getenv("MONGO_PORT", 27017)),
        username=os.getenv("MONGO_USER", "admin"),
        password=os.getenv("MONGO_PASSWORD", "password123"),
        serverSelectionTimeoutMS=2000
    )
    client.admin.command('ping')
    st.sidebar.success("🟢 MongoDB Connected")
    try:
        gold_stream_db = client["tlc_gold_stream"]
        demand_count  = gold_stream_db["stream_mart_demand"].count_documents({})
        anomaly_count = gold_stream_db["stream_mart_anomalies"].count_documents({})
        st.sidebar.metric("Stream demand records", f"{demand_count:,}")
        st.sidebar.metric("Anomalies detected",    f"{anomaly_count:,}")
    except Exception:
        st.sidebar.info("ℹ️ Speed Layer not yet active")
except Exception as e:
    st.sidebar.error(f"🔴 MongoDB Failed: {e}")

st.sidebar.markdown("---")
st.sidebar.markdown("**👈 Select the Live Dashboard**")
st.sidebar.markdown("*v2.0 · Lambda Speed Layer*")
