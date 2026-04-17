"""
utils/data_loader.py
Chargement des données depuis Neon avec cache Streamlit.
"""

import pandas as pd
import streamlit as st
from utils.database import get_engine

# ── COLLECTES (Dashboard Principal) ────────────────────────────────

@st.cache_data(ttl=300)  # Cache 5 minutes
def load_collectes() -> pd.DataFrame:
    """Charge toutes les collectes depuis Neon."""
    engine = get_engine()
    df = pd.read_sql(
        "SELECT * FROM collectes ORDER BY date_collecte DESC",
        engine
    )
    
    # Nettoyage et conversion
    df["date_collecte"] = pd.to_datetime(df["date_collecte"])
    
    # Calcul du taux de traitement par ligne
    df["taux_traitement"] = df.apply(
        lambda r: round(r["points_traites"] / r["points_visites"] * 100, 1)
        if r["points_visites"] > 0 else 0,
        axis=1,
    )
    return df

def load_kpis(df: pd.DataFrame) -> dict:
    """
    Calcule les KPIs agrégés sur le DataFrame passé en paramètre.
    Note: On ne met pas de @st.cache_data ici pour que les KPIs 
    se recalculent immédiatement quand l'utilisateur filtre par date.
    """
    if df.empty:
        return {
            "total_pneus": 0, "total_points_visites": 0, "total_points_traites": 0,
            "taux_traitement_global": 0, "nb_collectes": 0, "sites_satures": 0,
            "sites_risque": 0, "besoins_appui": 0, "taux_problemes": 0,
        }

    sum_visites = df["points_visites"].sum()
    sum_traites = df["points_traites"].sum()
    
    return {
        "total_pneus":          int(df["pneus_collectes"].sum()),
        "total_points_visites": int(sum_visites),
        "total_points_traites": int(sum_traites),
        "taux_traitement_global": round(sum_traites / sum_visites * 100, 1) if sum_visites > 0 else 0,
        "nb_collectes":         len(df),
        "sites_satures":        int((df["etat_site"] == "Saturé").sum()),
        "sites_risque":         int((df["etat_site"] == "Risque").sum()),
        "besoins_appui":        int((df["besoin_appui"] == "oui").sum()),
        "taux_problemes":       round(
            (df["type_probleme"].notna() & (df["type_probleme"] != "RAS")).sum()
            / len(df) * 100, 1
        ) if len(df) > 0 else 0,
    }

# ── LIVRAISONS (Page CTT SOCOCIM) ──────────────────────────────────

@st.cache_data(ttl=300)
def load_livraisons_ctt() -> pd.DataFrame:
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM livraisons_ctt ORDER BY date_livraison DESC", engine)
    
    if not df.empty:
        df["date_livraison"] = pd.to_datetime(df["date_livraison"])
        # On garde la valeur réelle de Neon sans inventer de données
        df["tonnage"] = pd.to_numeric(df["tonnage"], errors='coerce').fillna(0.0)
        
    return df

def load_kpis_ctt(df: pd.DataFrame) -> dict:
    """Calcule les KPIs pour la page SOCOCIM sans erreurs de clés."""
    if df.empty:
        return {
            "total_livre": 0, 
            "nb_voyages": 0, 
            "tonnage_total": 0,
            "moyenne_par_voyage": 0,
            "duree_moy": 0,
            "taux_bien_charges": 0
        }
    
    total_pneus = int(df["nombre_pneus"].sum())
    nb_v = len(df)
    
    # Calcul de la durée moyenne (si colonne présente)
    d_moy = int(df["duree_minutes"].mean()) if "duree_minutes" in df.columns else 0
    
    # Calcul du taux de remplissage (véhicules à 75% ou 100%)
    taux_bc = 0
    if "taux_remplissage" in df.columns:
        bons = df["taux_remplissage"].astype(str).str.contains("100|75|Plein", case=False).sum()
        taux_bc = int((bons / nb_v) * 100) if nb_v > 0 else 0

    return {
        "total_livre": total_pneus,
        "nb_voyages": nb_v,
        "tonnage_total": round(df["tonnage"].sum(), 2),
        "moyenne_par_voyage": int(total_pneus / nb_v) if nb_v > 0 else 0,
        "duree_moy": d_moy,
        "taux_bien_charges": taux_bc
    }