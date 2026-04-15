"""
app.py — Dashboard principal : Dispositif de Ramassage de Pneus (SONAGED)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

from utils.database import init_db
from utils.data_loader import load_collectes, load_kpis

# ── Config page ───────────────────────────────────────────────────
st.set_page_config(
    page_title="SONAGED · Pneus Dashboard",
    page_icon="♻️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS personnalisé ──────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap');

  html, body, [class*="css"] { font-family: 'Space Grotesk', sans-serif; }

  /* Header */
  .main-header {
    background: linear-gradient(135deg, #0f4c35 0%, #1a7a55 50%, #0d3d2a 100%);
    padding: 2rem 2.5rem;
    border-radius: 16px;
    margin-bottom: 1.5rem;
    box-shadow: 0 8px 32px rgba(15,76,53,0.3);
  }
  .main-header h1 {
    color: #e8f5e9;
    font-size: 2rem;
    font-weight: 700;
    margin: 0;
    letter-spacing: -0.5px;
  }
  .main-header p { color: #a5d6a7; margin: 0.3rem 0 0; font-size: 0.95rem; }

  /* KPI Cards */
  .kpi-card {
    background: #ffffff;
    border: 1px solid #e8f0ee;
    border-radius: 12px;
    padding: 1.2rem 1.5rem;
    text-align: center;
    box-shadow: 0 2px 12px rgba(0,0,0,0.06);
    transition: transform 0.2s, box-shadow 0.2s;
  }
  .kpi-card:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(0,0,0,0.1); }
  .kpi-value { font-size: 2.4rem; font-weight: 700; color: #0f4c35; font-family: 'JetBrains Mono', monospace; }
  .kpi-label { font-size: 0.78rem; font-weight: 500; color: #6b8f7c; text-transform: uppercase; letter-spacing: 1px; margin-top: 0.2rem; }
  .kpi-delta { font-size: 0.82rem; color: #43a047; margin-top: 0.3rem; }

  /* Alerte rouge */
  .kpi-alert .kpi-value { color: #c62828; }
  .kpi-alert { border-color: #ffcdd2; background: #fff8f8; }

  /* Alerte orange */
  .kpi-warning .kpi-value { color: #e65100; }
  .kpi-warning { border-color: #ffe0b2; background: #fffbf6; }

  /* Section titles */
  .section-title {
    font-size: 0.9rem;
    font-weight: 600;
    color: #1a7a55;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    margin: 1.8rem 0 0.8rem;
    padding-bottom: 0.4rem;
    border-bottom: 2px solid #e8f5e9;
  }

  /* Sidebar */
  [data-testid="stSidebar"] { background: #0f4c35 !important; }
  [data-testid="stSidebar"] * { color: #e8f5e9 !important; }
  [data-testid="stSidebar"] .stSelectbox label,
  [data-testid="stSidebar"] .stMultiSelect label { color: #a5d6a7 !important; font-size: 0.82rem !important; }

  /* Sync button */
  .stButton > button {
    background: #1a7a55 !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    padding: 0.5rem 1.2rem !important;
    width: 100%;
  }
  .stButton > button:hover { background: #0f4c35 !important; }

  /* Masquer le menu hamburger */
  #MainMenu, footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ── Init DB ───────────────────────────────────────────────────────
try:
    init_db()
except Exception as e:
    st.error(f"❌ Connexion Neon impossible : {e}")
    st.info("Vérifiez votre fichier `.env` et la variable `NEON_DATABASE_URL`.")
    st.stop()

# ── Sidebar ───────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ♻️ SONAGED")
    st.markdown("**Dispositif de ramassage de pneus usagés**")
    st.markdown("---")

    # Synchronisation KoboToolbox
    st.markdown("#### 🔄 Synchronisation")
    if st.button("Synchroniser depuis KoboToolbox"):
        with st.spinner("Récupération des données..."):
            try:
                from utils.kobo_sync import sync_kobo_to_neon
                result = sync_kobo_to_neon()
                st.success(f"✅ {result['inserted']} enregistrements synchronisés")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Erreur : {e}")

    st.markdown("---")

    # Chargement des données
    try:
        df_full = load_collectes()
    except Exception as e:
        st.error(f"Erreur chargement : {e}")
        df_full = pd.DataFrame()

    if df_full.empty:
        st.warning("Aucune donnée. Lancez une synchronisation.")
        st.stop()

    # Filtres
    st.markdown("#### 🔍 Filtres")

    # Filtre date
    min_date = df_full["date_collecte"].min().date()
    max_date = df_full["date_collecte"].max().date()
    date_range = st.date_input(
        "Période",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    # Filtre département
    depts = ["Tous"] + sorted(df_full["departement"].dropna().unique().tolist())
    dept_sel = st.selectbox("Département", depts)

    # Filtre état site
    etats = ["Tous"] + sorted(df_full["etat_site"].dropna().unique().tolist())
    etat_sel = st.selectbox("État du site", etats)

    st.markdown("---")
    st.markdown(
        f"<small style='color:#a5d6a7'>Dernière sync : {datetime.now().strftime('%d/%m/%Y %H:%M')}</small>",
        unsafe_allow_html=True
    )

# ── Filtrage ──────────────────────────────────────────────────────
df = df_full.copy()
if len(date_range) == 2:
    d0, d1 = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
    df = df[(df["date_collecte"] >= d0) & (df["date_collecte"] <= d1)]
if dept_sel != "Tous":
    df = df[df["departement"] == dept_sel]
if etat_sel != "Tous":
    df = df[df["etat_site"] == etat_sel]

# ── Header ────────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
  <h1>♻️ Dispositif de ramassage de pneus usagés  SONAGED - SOCOCIM</h1>
  <p>Tableau de bord de suivi opérationnel · Région de Dakar</p>
</div>
""", unsafe_allow_html=True)

# ── KPIs principaux ───────────────────────────────────────────────
kpis = load_kpis(df)
st.markdown('<p class="section-title">📊 Indicateurs Clés</p>', unsafe_allow_html=True)

c1, c2, c3, c4, c5, c6 = st.columns(6)

cards = [
    (c1, f"{kpis['total_pneus']:,}", "Pneus collectés", "", ""),
    (c2, f"{kpis['nb_collectes']}", "Sorties terrain", "", ""),
    (c3, f"{kpis['taux_traitement_global']}%", "Taux de traitement", "", ""),
    (c4, f"{kpis['total_points_traites']:,}", "Points traités", "", ""),
    (c5, f"{kpis['sites_satures']}", "Sites saturés", "", "kpi-alert" if kpis['sites_satures'] > 0 else ""),
    (c6, f"{kpis['besoins_appui']}", "Besoins d'appui", "", "kpi-warning" if kpis['besoins_appui'] > 0 else ""),
]
for col, val, lbl, delta, extra_class in cards:
    with col:
        st.markdown(f"""
        <div class="kpi-card {extra_class}">
          <div class="kpi-value">{val}</div>
          <div class="kpi-label">{lbl}</div>
        </div>
        """, unsafe_allow_html=True)

# ── Ligne 1 : Tendance + Départements ────────────────────────────
st.markdown('<p class="section-title">📈 Évolution & Répartition Géographique</p>', unsafe_allow_html=True)
col_left, col_right = st.columns([2, 1])

with col_left:
    df_time = (
        df.groupby("date_collecte")["pneus_collectes"]
        .sum()
        .reset_index()
        .sort_values("date_collecte")
    )
    df_time["cumul"] = df_time["pneus_collectes"].cumsum()

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Bar(
        x=df_time["date_collecte"], y=df_time["pneus_collectes"],
        name="Collecte du jour", marker_color="#a5d6a7", opacity=0.7
    ))
    fig_trend.add_trace(go.Scatter(
        x=df_time["date_collecte"], y=df_time["cumul"],
        name="Cumul", line=dict(color="#0f4c35", width=2.5),
        yaxis="y2"
    ))
    fig_trend.update_layout(
        title="Pneus collectés — Journalier & Cumulatif",
        yaxis=dict(title="Journalier", showgrid=False),
        yaxis2=dict(title="Cumul", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", y=-0.2),
        plot_bgcolor="white", paper_bgcolor="white",
        margin=dict(t=40, b=40),
        height=300,
        font=dict(family="Space Grotesk"),
    )
    st.plotly_chart(fig_trend, use_container_width=True)

with col_right:
    df_dept = (
        df.groupby("departement")["pneus_collectes"]
        .sum()
        .reset_index()
        .sort_values("pneus_collectes", ascending=True)
    )
    fig_dept = px.bar(
        df_dept, x="pneus_collectes", y="departement",
        orientation="h", title="Pneus par département",
        color="pneus_collectes",
        color_continuous_scale=["#e8f5e9", "#1a7a55", "#0f4c35"],
    )
    fig_dept.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        coloraxis_showscale=False,
        margin=dict(t=40, b=20, l=10, r=10),
        height=300,
        font=dict(family="Space Grotesk"),
    )
    st.plotly_chart(fig_dept, use_container_width=True)

# ── Ligne 2 : Modes collecte + Problèmes + État sites ─────────────
st.markdown('<p class="section-title">🚛 Opérations & Qualité</p>', unsafe_allow_html=True)
col_a, col_b, col_c = st.columns(3)

with col_a:
    df_mode = df["mode_collecte"].value_counts().reset_index()
    df_mode.columns = ["Mode", "Nb"]
    fig_mode = px.pie(
        df_mode, names="Mode", values="Nb",
        title="Modes de collecte",
        color_discrete_sequence=px.colors.sequential.Greens_r,
        hole=0.45,
    )
    fig_mode.update_layout(
        margin=dict(t=40, b=10),
        height=280,
        font=dict(family="Space Grotesk"),
        legend=dict(font=dict(size=11)),
    )
    st.plotly_chart(fig_mode, use_container_width=True)

with col_b:
    df_pb = df[df["type_probleme"].notna() & (df["type_probleme"] != "RAS")]
    df_pb = df_pb["type_probleme"].value_counts().reset_index()
    df_pb.columns = ["Problème", "Nb"]
    if df_pb.empty:
        st.info("✅ Aucun problème signalé sur la période.")
    else:
        fig_pb = px.bar(
            df_pb, x="Nb", y="Problème", orientation="h",
            title="Problèmes rencontrés",
            color="Nb",
            color_continuous_scale=["#fff3e0", "#e65100"],
        )
        fig_pb.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            coloraxis_showscale=False,
            margin=dict(t=40, b=10, l=10, r=10),
            height=280,
            font=dict(family="Space Grotesk"),
        )
        st.plotly_chart(fig_pb, use_container_width=True)

with col_c:
    df_etat = df["etat_site"].value_counts().reset_index()
    df_etat.columns = ["État", "Nb"]
    color_map = {"Normal": "#43a047", "Saturé": "#e53935", "Risque": "#fb8c00"}
    fig_etat = px.pie(
        df_etat, names="État", values="Nb",
        title="État des sites de transit",
        color="État",
        color_discrete_map=color_map,
        hole=0.45,
    )
    fig_etat.update_layout(
        margin=dict(t=40, b=10),
        height=280,
        font=dict(family="Space Grotesk"),
        legend=dict(font=dict(size=11)),
    )
    st.plotly_chart(fig_etat, use_container_width=True)

# ── Ligne 3 : Lieux de collecte + Superviseurs ───────────────────
st.markdown('<p class="section-title">📍 Lieux & Superviseurs</p>', unsafe_allow_html=True)
col_d, col_e = st.columns(2)

with col_d:
    # Éclater les lieux multiples
    lieux_series = df["lieu_de_collecte"].dropna().str.split(" | ").explode()
    df_lieux = lieux_series.value_counts().reset_index()
    df_lieux.columns = ["Lieu", "Nb"]
    fig_lieux = px.bar(
        df_lieux.head(8), x="Lieu", y="Nb",
        title="Lieux de collecte les plus fréquentés",
        color="Nb",
        color_continuous_scale=["#e8f5e9", "#1a7a55"],
    )
    fig_lieux.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        coloraxis_showscale=False,
        margin=dict(t=40, b=60),
        height=300,
        font=dict(family="Space Grotesk"),
        xaxis_tickangle=-30,
    )
    st.plotly_chart(fig_lieux, use_container_width=True)

with col_e:
    df_sup = (
        df.groupby("superviseur")
        .agg(pneus=("pneus_collectes", "sum"), sorties=("id", "count"))
        .reset_index()
        .sort_values("pneus", ascending=False)
    )
    fig_sup = px.bar(
        df_sup, x="superviseur", y="pneus",
        title="Performance par superviseur",
        text="pneus",
        color="pneus",
        color_continuous_scale=["#e8f5e9", "#0f4c35"],
    )
    fig_sup.update_traces(textposition="outside")
    fig_sup.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        coloraxis_showscale=False,
        margin=dict(t=40, b=60),
        height=300,
        font=dict(family="Space Grotesk"),
        xaxis_tickangle=-15,
    )
    st.plotly_chart(fig_sup, use_container_width=True)

# ── Carte GPS ─────────────────────────────────────────────────────
df_geo = df[df["latitude"].notna() & df["longitude"].notna()]
if not df_geo.empty:
    st.markdown('<p class="section-title">🗺️ Localisation des collectes</p>', unsafe_allow_html=True)
    import pydeck as pdk

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df_geo,
        get_position=["longitude", "latitude"],
        get_color=[26, 122, 85, 180],
        get_radius=200,
        pickable=True,
    )
    view = pdk.ViewState(
        latitude=df_geo["latitude"].mean(),
        longitude=df_geo["longitude"].mean(),
        zoom=11,
        pitch=0,
    )
    st.pydeck_chart(pdk.Deck(
        layers=[layer],
        initial_view_state=view,
        tooltip={"text": "{quartier}\n{pneus_collectes} pneus"},
        map_style="mapbox://styles/mapbox/light-v10",
    ))

# ── Tableau détaillé ──────────────────────────────────────────────
st.markdown('<p class="section-title">📋 Détail des collectes</p>', unsafe_allow_html=True)
cols_show = [
    "date_collecte", "departement", "commune", "superviseur",
    "points_visites", "points_traites", "pneus_collectes",
    "taux_traitement", "mode_collecte", "etat_site", "type_probleme"
]
cols_available = [c for c in cols_show if c in df.columns]

st.dataframe(
    df[cols_available].rename(columns={
        "date_collecte":   "Date",
        "departement":     "Département",
        "commune":         "Commune",
        "superviseur":     "Superviseur",
        "points_visites":  "Pts visités",
        "points_traites":  "Pts traités",
        "pneus_collectes": "Pneus",
        "taux_traitement": "Taux (%)",
        "mode_collecte":   "Mode",
        "etat_site":       "État site",
        "type_probleme":   "Problème",
    }),
    use_container_width=True,
    height=350,
)

# Export CSV
csv = df[cols_available].to_csv(index=False).encode("utf-8")
st.download_button(
    label="⬇️ Télécharger en CSV",
    data=csv,
    file_name=f"collectes_pneus_{datetime.now().strftime('%Y%m%d')}.csv",
    mime="text/csv",
)
