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
    .badge-batch {
        display: inline-block; background: rgba(99,102,241,0.15); border: 1px solid #6366f1;
        color: #6366f1; border-radius: 20px; padding: 2px 10px; font-size: 0.75rem; font-weight: 600; margin-left: 8px;
    }
    .arch-box {
        background: rgba(246,201,14,0.05); border: 1px solid rgba(246,201,14,0.2);
        border-radius: 12px; padding: 1.2rem 1.5rem; margin-top: 1.5rem; color: #94a3b8; font-size: 0.88rem;
    }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="main-title">🚕 NYC TLC Analytics Platform</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Lambda Architecture · PySpark · Kafka · MongoDB · Streamlit</div>', unsafe_allow_html=True)

# ── Pages ─────────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    <div class="page-card">
        <h3>⚡ Lambda Live (2026) <span class="badge-live">SPEED LAYER</span></h3>
        <p>Dashboard en tiempo real del año 2026. Monitorea viajes, ingresos y anomalías
        detectadas por Isolation Forest mientras Kafka produce datos en vivo.
        Se auto-actualiza cada 10 segundos.</p>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("""
    <div class="page-card">
        <h3>🛡️ Audit &amp; Quality Control</h3>
        <p>Salud del pipeline: throughput, tiempos de ejecución, tasa de cuarentena
        e inspección de registros rechazados para todos los vehículos y modos.</p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div class="page-card">
        <h3>📊 Historical Batch (2019–2025) <span class="badge-batch">BATCH LAYER</span></h3>
        <p>Análisis histórico comparativo de la capa Gold. Tendencias de demanda,
        ingresos por zona, segmentación KMeans y distribución de anomalías
        para contrastar con los datos en vivo de 2026.</p>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("""
    <div class="page-card">
        <h3>🔮 ML Insights (Próximamente)</h3>
        <p>Pronóstico SARIMA de demanda horaria, segmentación geoespacial
        y clasificación de propinas con Random Forest.</p>
    </div>
    """, unsafe_allow_html=True)

# ── Architecture diagram ───────────────────────────────────────────────────────
st.markdown("""
<div class="arch-box">
<strong style="color:#f6c90e">Arquitectura Lambda — Flujo de datos:</strong><br>
<code style="color:#a5b4fc">
[TLC 2026 Parquet] → Kafka Producer → 4 Topics (yellow/green/fhv/hvfhv)<br>
  ↓ Speed Layer (Streaming en vivo)<br>
  Silver Streams × 4 → tlc_silver → Gold Stream Marts + ML Inference → tlc_gold_stream<br>
  ↓ Batch Layer (histórico 2019–2025)<br>
  Bronze Ingestion → Silver → Gold Marts + ML Training → tlc_gold<br>
  ↓ Serving Layer<br>
  Streamlit: combina tlc_gold (histórico) + tlc_gold_stream (2026 en vivo)
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
st.sidebar.markdown("**👈 Select a dashboard**")
st.sidebar.markdown("*v2.0 · Lambda Architecture*")
