"""
2_Lambda_Live.py — Speed Layer Dashboard (2026 en vivo)

Lee desde tlc_gold_stream (stream_mart_demand, stream_mart_financials,
stream_mart_anomalies) y muestra KPIs actualizados en tiempo real.
Se auto-refresca cada 10 segundos sin recargar la página completa.
"""
import streamlit as st
import os
import time
import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient

st.set_page_config(
    page_title="Lambda Live — 2026 Speed Layer",
    page_icon="⚡",
    layout="wide",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .stApp { background: linear-gradient(135deg, #060b18 0%, #0a1120 100%); color: #e2e8f0; }

    .live-header {
        font-size: 2rem; font-weight: 700; color: #10b981;
        display: flex; align-items: center; gap: 12px;
    }
    .pulse {
        width: 14px; height: 14px; border-radius: 50%; background: #10b981;
        display: inline-block;
        animation: pulse 1.5s ease-in-out infinite;
    }
    @keyframes pulse {
        0%, 100% { box-shadow: 0 0 0 0 rgba(16,185,129,0.5); }
        50%       { box-shadow: 0 0 0 10px rgba(16,185,129,0); }
    }
    .kpi-card {
        background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px; padding: 1.2rem 1.5rem; text-align: center;
    }
    .kpi-value { font-size: 2.2rem; font-weight: 700; color: #f6c90e; }
    .kpi-label { font-size: 0.82rem; color: #64748b; margin-top: 4px; }
    .kpi-delta { font-size: 0.82rem; color: #10b981; }
    .section-title {
        font-size: 1.1rem; font-weight: 600; color: #94a3b8;
        border-left: 3px solid #f6c90e; padding-left: 10px; margin: 1.5rem 0 1rem;
    }
    .anomaly-badge {
        display: inline-block; background: rgba(239,68,68,0.15);
        border: 1px solid #ef4444; color: #ef4444;
        border-radius: 6px; padding: 2px 8px; font-size: 0.75rem; font-weight: 600;
    }
    .no-data { color: #64748b; font-style: italic; font-size: 0.9rem; }
</style>
""", unsafe_allow_html=True)

# ── MongoDB connection ─────────────────────────────────────────────────────────
@st.cache_resource
def get_client():
    return MongoClient(
        host=os.getenv("MONGO_HOST", "mongodb"),
        port=int(os.getenv("MONGO_PORT", 27017)),
        username=os.getenv("MONGO_USER", "admin"),
        password=os.getenv("MONGO_PASSWORD", "password123"),
        serverSelectionTimeoutMS=3000
    )

def get_dbs(client):
    return client["tlc_gold_stream"], client["tlc_audit"]

# ── Data loaders ───────────────────────────────────────────────────────────────
def load_demand(db) -> pd.DataFrame:
    docs = list(db["stream_mart_demand"].find({}, {"_id": 0}))
    return pd.DataFrame(docs) if docs else pd.DataFrame()

def load_financials(db) -> pd.DataFrame:
    docs = list(db["stream_mart_financials"].find({}, {"_id": 0}))
    return pd.DataFrame(docs) if docs else pd.DataFrame()

def load_anomalies(db, limit=200) -> pd.DataFrame:
    docs = list(db["stream_mart_anomalies"].find(
        {}, {"_id": 0}, sort=[("detected_at", -1)], limit=limit
    ))
    return pd.DataFrame(docs) if docs else pd.DataFrame()

def load_pipeline_runs(audit_db) -> pd.DataFrame:
    docs = list(audit_db["pipeline_runs"].find(
        {"mode": "streaming_kafka"},
        {"_id": 0, "pipeline_name": 1, "start_time": 1, "duration_seconds": 1,
         "output_summary": 1, "quality_passed": 1},
        sort=[("start_time", -1)],
        limit=100
    ))
    return pd.DataFrame(docs) if docs else pd.DataFrame()

# ── Plotly theme ───────────────────────────────────────────────────────────────
PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(255,255,255,0.02)",
    font=dict(family="Inter", color="#94a3b8"),
    margin=dict(l=20, r=20, t=40, b=20),
    xaxis=dict(gridcolor="rgba(255,255,255,0.06)", linecolor="rgba(255,255,255,0.1)"),
    yaxis=dict(gridcolor="rgba(255,255,255,0.06)", linecolor="rgba(255,255,255,0.1)"),
)

COLOR_MAP = {
    "yellow": "#f6c90e", "green": "#10b981",
    "fhv": "#6366f1",    "hvfhv": "#f97316",
}

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="live-header">
    <span class="pulse"></span>
    ⚡ Lambda Live Dashboard — 2026 (Speed Layer)
</div>
""", unsafe_allow_html=True)
st.caption(f"Auto-refreshes every 10 seconds · {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ── Config sidebar ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Controls")
    auto_refresh = st.toggle("Auto-refresh (10s)", value=True)
    refresh_interval = st.slider("Refresh interval (s)", 5, 60, 10)
    st.markdown("---")
    st.markdown("**Filters**")
    vt_filter = st.multiselect(
        "Vehicle types",
        ["yellow", "green", "fhv", "hvfhv"],
        default=["yellow", "green", "fhv", "hvfhv"]
    )
    st.markdown("---")
    if st.button("🔄 Refresh Now"):
        st.rerun()

# ── Main content loop ──────────────────────────────────────────────────────────
placeholder = st.empty()

def render():
    try:
        client = get_client()
        db, audit_db = get_dbs(client)
    except Exception as e:
        st.error(f"❌ Cannot connect to MongoDB: {e}")
        return

    df_demand = load_demand(db)
    df_fin    = load_financials(db)
    df_anom   = load_anomalies(db)
    df_audit  = load_pipeline_runs(audit_db)

    # Apply vehicle type filter
    if not df_demand.empty and vt_filter:
        df_demand = df_demand[df_demand["vehicle_type"].isin(vt_filter)]
    if not df_fin.empty and vt_filter:
        df_fin = df_fin[df_fin["vehicle_type"].isin(vt_filter)]

    with placeholder.container():
        # ── KPI Cards ───────────────────────────────────────────────────────────
        st.markdown('<div class="section-title">📊 Global KPIs — 2026 Acumulado</div>', unsafe_allow_html=True)
        kc1, kc2, kc3, kc4, kc5 = st.columns(5)

        total_trips   = int(df_demand["total_trips"].sum())   if not df_demand.empty else 0
        total_revenue = float(df_fin["total_revenue"].sum())  if not df_fin.empty else 0.0
        total_tips    = float(df_fin["total_tips"].sum())      if not df_fin.empty else 0.0
        total_anom    = len(df_anom)
        anom_rate     = (total_anom / total_trips * 100) if total_trips > 0 else 0.0

        # Pipeline throughput from audit
        if not df_audit.empty:
            throughput_vals = [
                r.get("throughput_records_per_s", 0)
                for r in df_audit["output_summary"].dropna()
                if isinstance(r, dict)
            ]
            avg_throughput = sum(throughput_vals) / len(throughput_vals) if throughput_vals else 0
        else:
            avg_throughput = 0

        with kc1:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value">{total_trips:,}</div>
                <div class="kpi-label">Total Viajes 2026</div>
            </div>""", unsafe_allow_html=True)
        with kc2:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value">${total_revenue:,.0f}</div>
                <div class="kpi-label">Ingresos Acumulados</div>
            </div>""", unsafe_allow_html=True)
        with kc3:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value">${total_tips:,.0f}</div>
                <div class="kpi-label">Propinas Acumuladas</div>
            </div>""", unsafe_allow_html=True)
        with kc4:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value" style="color:#ef4444">{total_anom:,}</div>
                <div class="kpi-label">Anomalías Detectadas</div>
                <div class="kpi-delta">{anom_rate:.2f}% tasa</div>
            </div>""", unsafe_allow_html=True)
        with kc5:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value" style="color:#10b981">{avg_throughput:,.0f}</div>
                <div class="kpi-label">Throughput (rec/s)</div>
            </div>""", unsafe_allow_html=True)

        # ── Demand over time ──────────────────────────────────────────────────
        st.markdown('<div class="section-title">📈 Demanda por Hora — 2026</div>', unsafe_allow_html=True)

        if not df_demand.empty and {"year","month","day","hour","total_trips"}.issubset(df_demand.columns):
            df_time = df_demand.copy()
            df_time["dt"] = pd.to_datetime(
                df_time[["year","month","day","hour"]].rename(columns={"hour":"hour"})
                .assign(minute=0, second=0)
            )
            hourly = (
                df_time.groupby(["dt","vehicle_type"])["total_trips"]
                .sum().reset_index()
                .sort_values("dt")
            )
            fig = px.line(
                hourly, x="dt", y="total_trips", color="vehicle_type",
                color_discrete_map=COLOR_MAP,
                labels={"dt": "Hora", "total_trips": "Viajes", "vehicle_type": "Vehículo"},
                title="Viajes por hora — 2026 en vivo",
            )
            fig.update_layout(**PLOTLY_LAYOUT)
            fig.update_traces(line=dict(width=2.5))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.markdown('<div class="no-data">Sin datos de demanda todavía. Inicia el Kafka Producer para ver viajes en vivo.</div>', unsafe_allow_html=True)

        # ── Revenue by vehicle + Top zones ───────────────────────────────────
        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown('<div class="section-title">💰 Ingresos por Tipo de Vehículo</div>', unsafe_allow_html=True)
            if not df_fin.empty and "total_revenue" in df_fin.columns:
                rev_by_vt = df_fin.groupby("vehicle_type")["total_revenue"].sum().reset_index()
                fig2 = px.pie(
                    rev_by_vt, names="vehicle_type", values="total_revenue",
                    color="vehicle_type", color_discrete_map=COLOR_MAP,
                    hole=0.55,
                )
                fig2.update_layout(**PLOTLY_LAYOUT)
                fig2.update_traces(textfont_color="#e2e8f0")
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.markdown('<div class="no-data">Sin datos financieros aún.</div>', unsafe_allow_html=True)

        with col_b:
            st.markdown('<div class="section-title">🗺️ Top 10 Zonas por Viajes (2026)</div>', unsafe_allow_html=True)
            if not df_demand.empty and "zone_name" in df_demand.columns:
                top_zones = (
                    df_demand.groupby("zone_name")["total_trips"]
                    .sum().reset_index()
                    .sort_values("total_trips", ascending=False)
                    .head(10)
                )
                fig3 = px.bar(
                    top_zones, x="total_trips", y="zone_name", orientation="h",
                    color="total_trips", color_continuous_scale=["#1e3a5f","#f6c90e"],
                    labels={"total_trips": "Viajes", "zone_name": "Zona"},
                )
                fig3.update_layout(**PLOTLY_LAYOUT, showlegend=False)
                fig3.update_coloraxes(showscale=False)
                st.plotly_chart(fig3, use_container_width=True)
            else:
                st.markdown('<div class="no-data">Sin datos de zonas aún.</div>', unsafe_allow_html=True)

        # ── Anomalies table ───────────────────────────────────────────────────
        st.markdown('<div class="section-title">🚨 Anomalías Detectadas en Tiempo Real (últimas 50)</div>', unsafe_allow_html=True)

        if not df_anom.empty:
            display_cols = [c for c in [
                "detected_at", "vehicle_type", "borough", "pickup_zone_id",
                "fare_amount", "tip_amount", "trip_distance", "duration_min",
                "anomaly_score", "anomaly_label"
            ] if c in df_anom.columns]

            df_display = df_anom[display_cols].head(50).copy()
            if "anomaly_score" in df_display.columns:
                df_display["anomaly_score"] = df_display["anomaly_score"].round(4)

            st.dataframe(
                df_display,
                use_container_width=True,
                height=320,
                column_config={
                    "anomaly_label": st.column_config.TextColumn("Label"),
                    "anomaly_score": st.column_config.NumberColumn("Score", format="%.4f"),
                    "fare_amount":   st.column_config.NumberColumn("Fare $", format="$%.2f"),
                    "tip_amount":    st.column_config.NumberColumn("Tip $",  format="$%.2f"),
                }
            )
        else:
            st.markdown('<div class="no-data">No se han detectado anomalías todavía. Inicia el notebook de ML Inference.</div>', unsafe_allow_html=True)

        # ── Pipeline audit ────────────────────────────────────────────────────
        st.markdown('<div class="section-title">🔧 Pipeline Audit — Streaming Batches</div>', unsafe_allow_html=True)
        if not df_audit.empty:
            df_audit_disp = df_audit[["pipeline_name","start_time","duration_seconds","quality_passed"]].copy()
            df_audit_disp["quality_passed"] = df_audit_disp["quality_passed"].map(
                {1: "✅ Passed", 0: "⚠️ Quarantine"}
            )
            st.dataframe(df_audit_disp.head(20), use_container_width=True, height=250)
        else:
            st.markdown('<div class="no-data">No hay registros de audit streaming todavía.</div>', unsafe_allow_html=True)

        st.caption(f"Last updated: {datetime.datetime.now().strftime('%H:%M:%S')}")


# ── Render + auto-refresh ──────────────────────────────────────────────────────
render()

if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
