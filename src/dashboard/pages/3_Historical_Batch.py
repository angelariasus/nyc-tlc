"""
3_Historical_Batch.py — Batch Layer Dashboard (2019-2025)

Lee desde tlc_gold (mart_demand_volume, mart_financial_performance,
mart_abc_xyz_zones) para mostrar el análisis histórico comparativo.
"""
import streamlit as st
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient

st.set_page_config(
    page_title="Historical Batch — 2019-2025",
    page_icon="📊",
    layout="wide",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .stApp { background: linear-gradient(135deg, #07090f 0%, #0c1120 100%); color: #e2e8f0; }
    .page-header {
        font-size: 2rem; font-weight: 700;
        background: linear-gradient(90deg, #6366f1, #8b5cf6);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .kpi-card {
        background: rgba(99,102,241,0.08); border: 1px solid rgba(99,102,241,0.2);
        border-radius: 14px; padding: 1.2rem 1.5rem; text-align: center;
    }
    .kpi-value { font-size: 2rem; font-weight: 700; color: #a5b4fc; }
    .kpi-label { font-size: 0.82rem; color: #64748b; margin-top: 4px; }
    .section-title {
        font-size: 1.1rem; font-weight: 600; color: #94a3b8;
        border-left: 3px solid #6366f1; padding-left: 10px; margin: 1.5rem 0 1rem;
    }
    .no-data { color: #64748b; font-style: italic; font-size: 0.9rem; }
</style>
""", unsafe_allow_html=True)

# ── MongoDB ────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_client():
    return MongoClient(
        host=os.getenv("MONGO_HOST", "mongodb"),
        port=int(os.getenv("MONGO_PORT", 27017)),
        username=os.getenv("MONGO_USER", "admin"),
        password=os.getenv("MONGO_PASSWORD", "password123"),
        serverSelectionTimeoutMS=3000
    )

@st.cache_data(ttl=300)
def load_demand_volume():
    client = get_client()
    docs = list(client["tlc_gold"]["mart_demand_volume"].find({}, {"_id": 0}))
    return pd.DataFrame(docs) if docs else pd.DataFrame()

@st.cache_data(ttl=300)
def load_financials():
    client = get_client()
    docs = list(client["tlc_gold"]["mart_financial_performance"].find({}, {"_id": 0}))
    return pd.DataFrame(docs) if docs else pd.DataFrame()

@st.cache_data(ttl=300)
def load_abc_zones():
    client = get_client()
    docs = list(client["tlc_gold"]["mart_abc_xyz_zones"].find({}, {"_id": 0}))
    return pd.DataFrame(docs) if docs else pd.DataFrame()

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(255,255,255,0.02)",
    font=dict(family="Inter", color="#94a3b8"),
    margin=dict(l=20, r=20, t=40, b=20),
    xaxis=dict(gridcolor="rgba(255,255,255,0.06)"),
    yaxis=dict(gridcolor="rgba(255,255,255,0.06)"),
)

COLOR_MAP = {
    "yellow": "#f6c90e", "green": "#10b981",
    "fhv": "#6366f1",    "hvfhv": "#f97316",
}

SEGMENT_COLORS = {
    "AX": "#10b981", "AY": "#34d399", "AZ": "#6ee7b7",
    "BX": "#6366f1", "BY": "#818cf8", "BZ": "#a5b4fc",
    "CX": "#f97316", "CY": "#fb923c", "CZ": "#fdba74",
}

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="page-header">📊 Historical Batch Layer — 2019–2025</div>', unsafe_allow_html=True)
st.caption("Datos de la capa Gold (tlc_gold). Cache de 5 minutos.")

# ── Sidebar filters ────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🔍 Filters")
    year_range = st.slider("Year range", 2019, 2025, (2022, 2025))
    vt_filter  = st.multiselect(
        "Vehicle types",
        ["yellow", "green", "fhv", "hvfhv"],
        default=["yellow", "green", "fhv", "hvfhv"]
    )
    st.markdown("---")
    if st.button("🔄 Clear cache & reload"):
        st.cache_data.clear()
        st.rerun()

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner("Loading historical data from tlc_gold..."):
    df_demand = load_demand_volume()
    df_fin    = load_financials()
    df_abc    = load_abc_zones()

# Apply filters
def apply_filters(df):
    if df.empty:
        return df
    if "year" in df.columns:
        df = df[(df["year"] >= year_range[0]) & (df["year"] <= year_range[1])]
    if "vehicle_type" in df.columns and vt_filter:
        df = df[df["vehicle_type"].isin(vt_filter)]
    return df

df_demand = apply_filters(df_demand)
df_fin    = apply_filters(df_fin)

# ── KPI Cards ─────────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">📌 KPIs Históricos (rango seleccionado)</div>', unsafe_allow_html=True)
kc1, kc2, kc3, kc4 = st.columns(4)

total_trips   = int(df_demand["total_trips"].sum())   if not df_demand.empty and "total_trips" in df_demand.columns else 0
total_revenue = float(df_fin["total_revenue"].sum())  if not df_fin.empty and "total_revenue" in df_fin.columns else 0.0
avg_fare      = float(df_fin["avg_revenue_per_trip"].mean()) if not df_fin.empty and "avg_revenue_per_trip" in df_fin.columns else 0.0
avg_tip_rate  = float(df_fin["tip_rate_pct"].mean() * 100) if not df_fin.empty and "tip_rate_pct" in df_fin.columns else 0.0

with kc1:
    st.markdown(f'<div class="kpi-card"><div class="kpi-value">{total_trips/1e6:.1f}M</div><div class="kpi-label">Total Viajes</div></div>', unsafe_allow_html=True)
with kc2:
    st.markdown(f'<div class="kpi-card"><div class="kpi-value">${total_revenue/1e9:.2f}B</div><div class="kpi-label">Ingresos Totales</div></div>', unsafe_allow_html=True)
with kc3:
    st.markdown(f'<div class="kpi-card"><div class="kpi-value">${avg_fare:.2f}</div><div class="kpi-label">Tarifa Promedio</div></div>', unsafe_allow_html=True)
with kc4:
    st.markdown(f'<div class="kpi-card"><div class="kpi-value">{avg_tip_rate:.1f}%</div><div class="kpi-label">Tasa de Propina</div></div>', unsafe_allow_html=True)

# ── Demand trend ───────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">📈 Tendencia de Demanda Mensual</div>', unsafe_allow_html=True)

if not df_demand.empty and {"year","month","total_trips","vehicle_type"}.issubset(df_demand.columns):
    monthly = (
        df_demand
        .groupby(["year","month","vehicle_type"])["total_trips"]
        .sum().reset_index()
    )
    monthly["period"] = pd.to_datetime(
        monthly["year"].astype(str) + "-" + monthly["month"].astype(str).str.zfill(2)
    )
    fig = px.line(
        monthly.sort_values("period"),
        x="period", y="total_trips", color="vehicle_type",
        color_discrete_map=COLOR_MAP,
        labels={"period": "Período", "total_trips": "Viajes", "vehicle_type": "Vehículo"},
        title="Viajes mensuales por tipo de vehículo",
    )
    fig.update_layout(**PLOTLY_LAYOUT)
    fig.update_traces(line=dict(width=2))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.markdown('<div class="no-data">Sin datos de demanda histórica disponibles.</div>', unsafe_allow_html=True)

# ── Revenue by borough + ABC-XYZ ──────────────────────────────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.markdown('<div class="section-title">💰 Top 10 Zonas por Ingreso</div>', unsafe_allow_html=True)
    if not df_fin.empty and {"zone_name","total_revenue"}.issubset(df_fin.columns):
        top_rev = (
            df_fin.groupby("zone_name")["total_revenue"]
            .sum().reset_index()
            .sort_values("total_revenue", ascending=False)
            .head(10)
        )
        fig2 = px.bar(
            top_rev, x="total_revenue", y="zone_name", orientation="h",
            color="total_revenue", color_continuous_scale=["#1e1b4b","#6366f1","#a5b4fc"],
            labels={"total_revenue": "Ingresos ($)", "zone_name": "Zona"},
        )
        fig2.update_layout(**PLOTLY_LAYOUT, showlegend=False)
        fig2.update_coloraxes(showscale=False)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.markdown('<div class="no-data">Sin datos financieros históricos.</div>', unsafe_allow_html=True)

with col_b:
    st.markdown('<div class="section-title">🗂️ Segmentación ABC-XYZ de Zonas</div>', unsafe_allow_html=True)
    if not df_abc.empty and "segment" in df_abc.columns:
        seg_counts = df_abc["segment"].value_counts().reset_index()
        seg_counts.columns = ["segment", "count"]
        seg_counts["color"] = seg_counts["segment"].map(SEGMENT_COLORS).fillna("#64748b")
        fig3 = px.bar(
            seg_counts, x="segment", y="count",
            color="segment",
            color_discrete_map=SEGMENT_COLORS,
            labels={"segment": "Segmento", "count": "Zonas"},
            title="Zonas por segmento ABC-XYZ",
        )
        fig3.update_layout(**PLOTLY_LAYOUT, showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.markdown('<div class="no-data">Sin datos de segmentación disponibles.</div>', unsafe_allow_html=True)

# ── Hourly demand heatmap ──────────────────────────────────────────────────────
st.markdown('<div class="section-title">🕐 Demanda por Hora y Día de Semana</div>', unsafe_allow_html=True)

if not df_demand.empty and {"hour","is_weekend","total_trips"}.issubset(df_demand.columns):
    heatmap_df = (
        df_demand
        .groupby(["hour","is_weekend"])["total_trips"]
        .sum().reset_index()
    )
    heatmap_df["day_type"] = heatmap_df["is_weekend"].map({True: "Weekend", False: "Weekday"})
    fig4 = px.density_heatmap(
        heatmap_df, x="hour", y="day_type", z="total_trips",
        color_continuous_scale="Viridis",
        labels={"hour": "Hora del día", "day_type": "Tipo de día", "total_trips": "Viajes"},
        title="Mapa de calor: Demanda por hora",
    )
    fig4.update_layout(**PLOTLY_LAYOUT)
    st.plotly_chart(fig4, use_container_width=True)
else:
    st.markdown('<div class="no-data">Sin datos suficientes para el mapa de calor.</div>', unsafe_allow_html=True)

# ── Comparison box: Lambda context ────────────────────────────────────────────
st.markdown("---")
st.info("""
**🔀 Comparativa Lambda:** Estos datos históricos (Batch Layer, 2019–2025) conviven con el 
**Speed Layer (2026)** que puedes ver en la página *⚡ Lambda Live*. 
Cuando el Kafka Producer está activo, los dos mundos se actualizan en paralelo — eso es la Arquitectura Lambda en acción.
""")
