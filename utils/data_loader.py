"""
utils/data_loader.py
Chargement des données depuis Neon avec cache Streamlit.
"""

import pandas as pd
import streamlit as st
from utils.database import get_engine


@st.cache_data(ttl=300)  # Cache 5 minutes
def load_collectes() -> pd.DataFrame:
    """Charge toutes les collectes depuis Neon."""
    engine = get_engine()
    df = pd.read_sql(
        "SELECT * FROM collectes ORDER BY date_collecte DESC",
        engine
    )
    # Nettoyage de base
    df["date_collecte"] = pd.to_datetime(df["date_collecte"])
    df["taux_traitement"] = df.apply(
        lambda r: round(r["points_traites"] / r["points_visites"] * 100, 1)
        if r["points_visites"] > 0 else 0,
        axis=1,
    )
    return df


@st.cache_data(ttl=300)
def load_kpis(df: pd.DataFrame) -> dict:
    """Calcule les KPIs agrégés."""
    return {
        "total_pneus":         int(df["pneus_collectes"].sum()),
        "total_points_visites": int(df["points_visites"].sum()),
        "total_points_traites": int(df["points_traites"].sum()),
        "taux_traitement_global": round(
            df["points_traites"].sum() / df["points_visites"].sum() * 100, 1
        ) if df["points_visites"].sum() > 0 else 0,
        "nb_collectes":        len(df),
        "sites_satures":       int((df["etat_site"] == "Saturé").sum()),
        "sites_risque":        int((df["etat_site"] == "Risque").sum()),
        "besoins_appui":       int((df["besoin_appui"] == "oui").sum()),
        "taux_problemes":      round(
            (df["type_probleme"].notna() & (df["type_probleme"] != "RAS")).sum()
            / len(df) * 100, 1
        ) if len(df) > 0 else 0,
    }
