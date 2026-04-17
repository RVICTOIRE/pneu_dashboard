"""
pages/2_Livraisons_CTT_SOCOCIM.py
Dashboard de suivi des livraisons de pneus usagés : Départements → CTT → SOCOCIM
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

from utils.database import init_db_ctt
from utils.data_loader import load_livraisons_ctt, load_kpis_ctt

# ── Config page ───────────────────────────────────────────────────
st.set_page_config(
    page_title="CTT → SOCOCIM · Livraisons",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS (reprend la charte SONAGED) ──────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap');

  html, body, [class*="css"] { font-family: 'Space Grotesk', sans-serif; }

  .main-header {
    background: linear-gradient(135deg, #1a2744 0%, #2c4a8c 50%, #162038 100%);
    padding: 2rem 2.5rem;
    border-radius: 16px;
    margin-bottom: 1.5rem;
    box-shadow: 0 8px 32px rgba(26,39,68,0.3);
  }
  .main-header h1 { color: #e8edf5; font-size: 2rem; font-weight: 700; margin: 0; letter-spacing: -0.5px; }
  .main-header p  { color: #a5b4d6; margin: 0.3rem 0 0; font-size: 0.95rem; }

  .kpi-card {
    background: #ffffff;
    border: 1px solid #e8eef5;
    border-radius: 12px;
    padding: 1.2rem 1.5rem;
    text-align: center;
    box-shadow: 0 2px 12px rgba(0,0,0,0.06);
    transition: transform 0.2s, box-shadow 0.2s;
  }
  .kpi-card:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(0,0,0,0.1); }
  .kpi-value { font-size: 2.4rem; font-weight: 700; color: #1a2744; font-family: 'JetBrains Mono', monospace; }
  .kpi-label { font-size: 0.78rem; font-weight: 500; color: #6b7fa0; text-transform: uppercase; letter-spacing: 1px; margin-top: 0.2rem; }
  .kpi-sub   { font-size: 0.8rem; color: #3b63c4; margin-top: 0.3rem; }

  .kpi-good    { border-color: #c8ddf5; background: #f5f9ff; }
  .kpi-good .kpi-value { color: #1a4d8a; }

  .section-title {
    font-size: 0.9rem; font-weight: 600; color: #2c4a8c;
    text-transform: uppercase; letter-spacing: 1.5px;
    margin: 1.8rem 0 0.8rem;
    padding-bottom: 0.4rem;
    border-bottom: 2px solid #dce8f5;
  }

  [data-testid="stSidebar"] { background: #1a2744 !important; }
  [data-testid="stSidebar"] * { color: #e8edf5 !important; }

  .stButton > button {
    background: #2c4a8c !important; color: white !important;
    border: none !important; border-radius: 8px !important;
    font-weight: 600 !important; padding: 0.5rem 1.2rem !important; width: 100%;
  }
  .stButton > button:hover { background: #1a2744 !important; }

  #MainMenu, footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Init table CTT ────────────────────────────────────────────────
try:
    init_db_ctt()
except Exception as e:
    st.error(f"❌ Connexion Neon impossible : {e}")
    st.stop()

# ── Sidebar ───────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🚛 CTT → SOCOCIM")
    st.markdown("**Suivi des livraisons de pneus**")
    st.markdown("---")

    st.markdown("#### 🔄 Synchronisation")
    if st.button("Synchroniser depuis KoboToolbox"):
        with st.spinner("Récupération des données..."):
            try:
                from utils.kobo_sync_ctt import sync_ctt_to_neon
                result = sync_ctt_to_neon()
                st.success(f"✅ {result['inserted']} livraisons synchronisées")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Erreur : {e}")

    st.markdown("---")

    try:
        df_full = load_livraisons_ctt()
    except Exception as e:
        st.error(f"Erreur chargement : {e}")
        df_full = pd.DataFrame()

    if df_full.empty:
        st.warning("Aucune donnée. Lancez une synchronisation.")
        st.stop()

    # ── Filtres ───────────────────────────────────────────────────
    st.markdown("#### 🔍 Filtres")

    min_date = df_full["date_livraison"].min().date()
    max_date = df_full["date_livraison"].max().date()

    st.markdown("**Période**")
    col_d1, col_d2 = st.columns(2)
    with col_d1:
        date_start = st.date_input("Du", value=min_date, min_value=min_date, max_value=max_date, key="ctt_d1")
    with col_d2:
        date_end   = st.date_input("Au", value=max_date, min_value=min_date, max_value=max_date, key="ctt_d2")
    if date_start > date_end:
        date_start, date_end = date_end, date_start

    provenances = ["Toutes"] + sorted(df_full["provenance"].dropna().unique().tolist())
    prov_sel    = st.selectbox("Provenance", provenances)

    vehicules = ["Tous"] + sorted(df_full["type_vehicule"].dropna().unique().tolist())
    veh_sel   = st.selectbox("Type de véhicule", vehicules)

    st.markdown("---")
    st.markdown(
        f"<small style='color:#a5b4d6'>Dernière sync : {datetime.now().strftime('%d/%m/%Y %H:%M')}</small>",
        unsafe_allow_html=True
    )

# ── Filtrage ──────────────────────────────────────────────────────
df = df_full.copy()
d0, d1 = pd.Timestamp(date_start), pd.Timestamp(date_end)
df = df[(df["date_livraison"] >= d0) & (df["date_livraison"] <= d1)]
if prov_sel != "Toutes":
    df = df[df["provenance"] == prov_sel]
if veh_sel != "Tous":
    df = df[df["type_vehicule"] == veh_sel]

# ── Header ────────────────────────────────────────────────────────
is_full = (date_start == min_date and date_end == max_date)
badge   = (
    "<span style='background:#2c4a8c;padding:2px 10px;border-radius:10px;font-size:0.8rem'>Toute la période</span>"
    if is_full else
    "<span style='background:#c85000;padding:2px 10px;border-radius:10px;font-size:0.8rem'>⚡ Filtre actif</span>"
)
st.markdown(f"""
<div class="main-header">
  <h1>🚛 Livraisons → SOCOCIM</h1>
  <p>Suivi des transferts de pneus usagés vers l'usine · SONAGED - SOCOCIM &nbsp;·&nbsp;
     📅 <strong style="color:#e8edf5">{date_start.strftime('%d/%m/%Y')}</strong>
     &nbsp;→&nbsp;
     <strong style="color:#e8edf5">{date_end.strftime('%d/%m/%Y')}</strong>
     &nbsp;&nbsp;{badge}
  </p>
</div>
""", unsafe_allow_html=True)
# --- CALCUL DES KPIS ---
# On appelle la fonction avec le dataframe filtré (df)
kpis = load_kpis_ctt(df)

# --- AFFICHAGE DES KPIS (SECTION MANQUANTE) ---
st.markdown('<p class="section-title">📊 Indicateurs Clés (Période Sélectionnée)</p>', unsafe_allow_html=True)

c1, c2, c3, c4, c5, c6 = st.columns(6)

kpi_data = [
    (c1, f"{kpis['total_livre']:,}",       "Pneus livrés",          ""),
    (c2, f"{kpis['tonnage_total']:,}",     "Tonnage total (t)",     ""),
    (c3, f"{kpis['nb_voyages']}",          "Livraisons",            ""),
    # (c4, f"{kpis['moyenne_par_voyage']}",  "Pneus / livraison",     ""),
    # (c5, f"{kpis['duree_moy']} min",       "Durée moy. sur site",   "kpi-good" if kpis['duree_moy'] < 60 else ""),
    # (c6, f"{kpis['taux_bien_charges']}%",  "Véhicules bien chargés", "kpi-good" if kpis['taux_bien_charges'] >= 75 else ""),
]

for col, val, label, card_class in kpi_data:
    with col:
        st.markdown(f"""
            <div class="kpi-card {card_class}">
                <div class="kpi-value">{val}</div>
                <div class="kpi-label">{label}</div>
            </div>
        """, unsafe_allow_html=True)

st.markdown("---") # Séparateur visuel

# ── KPIs ─────────────────────────────────────────────────────────
def load_kpis_ctt(df: pd.DataFrame) -> dict:
    """Calcule tous les KPIs nécessaires pour la page SOCOCIM."""
    if df.empty:
        return {
            "total_livre": 0,
            "nb_voyages": 0,
            "tonnage_total": 0,
            "moyenne_par_voyage": 0,
            "duree_moy": 0,
            "taux_bien_charges": 0
        }
    
    # Calculs de base
    total_pneus = int(df["nombre_pneus"].sum())
    nb_v = len(df)
    
    # Calcul de la durée moyenne (si la colonne existe)
    duree_moy = int(df["duree_minutes"].mean()) if "duree_minutes" in df.columns else 0
    
    # Calcul du taux de remplissage (ex: si contient '100%' ou 'Plein')
    taux_bc = 0
    if "taux_remplissage" in df.columns:
        # On compte les camions chargés à plus de 75% ou marqués comme "Plein"
        bons_chargements = df["taux_remplissage"].astype(str).str.contains("100|75|Plein", case=False).sum()
        taux_bc = int((bons_chargements / nb_v) * 100) if nb_v > 0 else 0

    return {
        "total_livre": total_pneus,
        "nb_voyages": nb_v,
        "tonnage_total": round(df["tonnage"].sum(), 2) if "tonnage" in df.columns else 0,
        "moyenne_par_voyage": int(total_pneus / nb_v) if nb_v > 0 else 0,
        "duree_moy": duree_moy,
        "taux_bien_charges": taux_bc
    }
# ── Ligne 1 : Évolution temporelle + Provenances ─────────────────
st.markdown('<p class="section-title">📈 Évolution & Répartition par Provenance</p>', unsafe_allow_html=True)
col_l, col_r = st.columns([2, 1])

with col_l:
    df_time = (
        df.groupby("date_livraison")
        .agg(pneus=("nombre_pneus", "sum"), tonnage=("tonnage", "sum"))
        .reset_index().sort_values("date_livraison")
    )
    df_time["cumul_pneus"] = df_time["pneus"].cumsum()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_time["date_livraison"], y=df_time["pneus"],
        name="Pneus / jour", marker_color="#a5b4d6", opacity=0.7
    ))
    fig.add_trace(go.Scatter(
        x=df_time["date_livraison"], y=df_time["cumul_pneus"],
        name="Cumul pneus", line=dict(color="#1a2744", width=2.5), yaxis="y2"
    ))
    fig.update_layout(
        title="Pneus livrés — Journalier & Cumulatif",
        yaxis=dict(title="Journalier", showgrid=False),
        yaxis2=dict(title="Cumul", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", y=-0.2),
        plot_bgcolor="white", paper_bgcolor="white",
        margin=dict(t=40, b=40), height=300,
        font=dict(family="Space Grotesk"),
    )
    st.plotly_chart(fig, use_container_width=True)

with col_r:
    df_prov = (
        df.groupby("provenance")["nombre_pneus"]
        .sum().reset_index()
        .sort_values("nombre_pneus", ascending=True)
    )
    fig_prov = px.bar(
        df_prov, x="nombre_pneus", y="provenance", orientation="h",
        title="Pneus livrés par provenance",
        color="nombre_pneus",
        color_continuous_scale=["#dce8f5", "#2c4a8c", "#1a2744"],
    )
    fig_prov.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        coloraxis_showscale=False,
        margin=dict(t=40, b=20, l=10, r=10), height=300,
        font=dict(family="Space Grotesk"),
    )
    st.plotly_chart(fig_prov, use_container_width=True)

# ── Ligne 2 : Véhicules + Taux remplissage + Capacité ────────────
st.markdown('<p class="section-title">🚛 Véhicules & Remplissage</p>', unsafe_allow_html=True)
col_a, col_b, col_c = st.columns(3)

with col_a:
    df_veh = df["type_vehicule"].value_counts().reset_index()
    df_veh.columns = ["Véhicule", "Nb"]
    fig_veh = px.pie(
        df_veh, names="Véhicule", values="Nb",
        title="Types de véhicules",
        color_discrete_sequence=["#2c4a8c", "#a5b4d6"],
        hole=0.45,
    )
    fig_veh.update_layout(
        margin=dict(t=40, b=10), height=280,
        font=dict(family="Space Grotesk"),
    )
    st.plotly_chart(fig_veh, use_container_width=True)

with col_b:
    # Ordre logique du taux de remplissage
    ordre_remplissage = ["Plein", "3/4 plein", "Moitié plein", "1/4 plein", "Vide"]
    df_rem = df["taux_remplissage"].value_counts().reindex(ordre_remplissage).dropna().reset_index()
    df_rem.columns = ["Taux", "Nb"]
    color_rem = {
        "Plein":        "#1a4d8a",
        "3/4 plein":    "#2c6fbf",
        "Moitié plein": "#6fa3e0",
        "1/4 plein":    "#b5cff0",
        "Vide":         "#e8eef5",
    }
    fig_rem = px.bar(
        df_rem, x="Taux", y="Nb",
        title="Taux de remplissage",
        color="Taux",
        color_discrete_map=color_rem,
    )
    fig_rem.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        showlegend=False,
        margin=dict(t=40, b=40), height=280,
        font=dict(family="Space Grotesk"),
    )
    st.plotly_chart(fig_rem, use_container_width=True)

with col_c:
    df_cap = df["capacite"].value_counts().reset_index()
    df_cap.columns = ["Capacité", "Nb"]
    fig_cap = px.pie(
        df_cap, names="Capacité", values="Nb",
        title="Capacité des véhicules",
        color_discrete_sequence=["#1a2744", "#2c4a8c", "#6fa3e0"],
        hole=0.45,
    )
    fig_cap.update_layout(
        margin=dict(t=40, b=10), height=280,
        font=dict(family="Space Grotesk"),
    )
    st.plotly_chart(fig_cap, use_container_width=True)

# ── Ligne 3 : Tonnage par provenance + Durée sur site ────────────
st.markdown('<p class="section-title">⏱️ Tonnage & Durée sur site</p>', unsafe_allow_html=True)
col_d, col_e = st.columns(2)

with col_d:
    df_ton = (
        df.groupby("provenance")["tonnage"]
        .sum().reset_index()
        .sort_values("tonnage", ascending=False)
    )
    fig_ton = px.bar(
        df_ton, x="provenance", y="tonnage",
        title="Tonnage total par provenance",
        text="tonnage",
        color="tonnage",
        color_continuous_scale=["#dce8f5", "#1a2744"],
    )
    fig_ton.update_traces(textposition="outside")
    fig_ton.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        coloraxis_showscale=False,
        margin=dict(t=40, b=50), height=300,
        font=dict(family="Space Grotesk"),
    )
    st.plotly_chart(fig_ton, use_container_width=True)

with col_e:
    df_dur = df[df["duree_minutes"].notna()].copy()
    if df_dur.empty:
        st.info("Pas de données de durée disponibles.")
    else:
        fig_dur = px.box(
            df_dur, x="provenance", y="duree_minutes",
            title="Durée sur site par provenance (min)",
            color="provenance",
            color_discrete_sequence=px.colors.sequential.Blues_r,
        )
        fig_dur.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            showlegend=False,
            margin=dict(t=40, b=50), height=300,
            font=dict(family="Space Grotesk"),
            xaxis_tickangle=-15,
        )
        st.plotly_chart(fig_dur, use_container_width=True)

# ── Superviseurs ──────────────────────────────────────────────────
st.markdown('<p class="section-title">👤 Performance par superviseur</p>', unsafe_allow_html=True)
df_sup = (
    df.groupby("superviseur")
    .agg(pneus=("nombre_pneus", "sum"), livraisons=("id", "count"), tonnage=("tonnage", "sum"))
    .reset_index()
    .sort_values("pneus", ascending=False)
)
fig_sup = px.bar(
    df_sup, x="superviseur", y="pneus",
    title="Pneus livrés par superviseur",
    text="pneus",
    color="pneus",
    color_continuous_scale=["#dce8f5", "#1a2744"],
)
fig_sup.update_traces(textposition="outside")
fig_sup.update_layout(
    plot_bgcolor="white", paper_bgcolor="white",
    coloraxis_showscale=False,
    margin=dict(t=40, b=60), height=280,
    font=dict(family="Space Grotesk"),
    xaxis_tickangle=-15,
)
st.plotly_chart(fig_sup, use_container_width=True)

# ── Tableau détaillé ──────────────────────────────────────────────
st.markdown('<p class="section-title">📋 Détail des livraisons</p>', unsafe_allow_html=True)

cols_show = [
    "date_livraison", "provenance", "superviseur",
    "type_vehicule", "capacite", "taux_remplissage",
    "nombre_pneus", "tonnage", "heure_arrivee", "heure_depart", "duree_minutes",
    "observation",
]
cols_available = [c for c in cols_show if c in df.columns]

st.dataframe(
    df[cols_available].rename(columns={
        "date_livraison":   "Date",
        "provenance":       "Provenance",
        "superviseur":      "Superviseur",
        "type_vehicule":    "Véhicule",
        "capacite":         "Capacité",
        "taux_remplissage": "Remplissage",
        "nombre_pneus":     "Pneus",
        "tonnage":          "Tonnage (t)",
        "heure_arrivee":    "Arrivée",
        "heure_depart":     "Départ",
        "duree_minutes":    "Durée (min)",
        "observation":      "Observation",
    }),
    use_container_width=True,
    height=380,
)

csv = df[cols_available].to_csv(index=False).encode("utf-8")
st.download_button(
    label="⬇️ Télécharger en CSV",
    data=csv,
    file_name=f"livraisons_ctt_{datetime.now().strftime('%Y%m%d')}.csv",
    mime="text/csv",
)
