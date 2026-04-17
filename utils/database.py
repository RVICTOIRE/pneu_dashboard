"""
utils/database.py
Gestion de la connexion Neon (PostgreSQL) et initialisation du schéma.
"""

import os
import logging
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


load_dotenv()
logger = logging.getLogger(__name__)

# --- COMPATIBILITÉ STREAMLIT CLOUD & LOCAL ---
# On cherche d'abord dans les secrets Streamlit, puis dans l'environnement système
DATABASE_URL = st.secrets.get("NEON_DATABASE_URL") or os.getenv("NEON_DATABASE_URL")

def get_engine():
    """Retourne un engine SQLAlchemy connecté à Neon."""
    if not DATABASE_URL:
        raise ValueError(
            "NEON_DATABASE_URL manquant. Vérifiez vos Secrets Streamlit ou votre fichier .env"
        )
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def init_db():
    """
    Initialise le schéma de la base de données (Collectes et Livraisons).
    À appeler au démarrage de l'application.
    """
    engine = get_engine()
    
    # ── Schéma global : Collectes + Livraisons ──
    ddl = """
    -- Table des collectes
    CREATE TABLE IF NOT EXISTS collectes (
        id                   TEXT PRIMARY KEY,
        date_collecte        DATE,
        start_time           TIMESTAMP,
        end_time             TIMESTAMP,
        departement          TEXT,
        commune              TEXT,
        quartier             TEXT,
        zone                 TEXT,
        lieu_de_collecte     TEXT,
        points_visites       INTEGER DEFAULT 0,
        points_traites       INTEGER DEFAULT 0,
        pneus_collectes      INTEGER DEFAULT 0,
        mode_collecte        TEXT,
        site_transit         TEXT,
        etat_site            TEXT,
        type_probleme        TEXT,
        description_probleme TEXT,
        action_corrective    TEXT,
        besoin_appui         TEXT,
        detail_appui         TEXT,
        superviseur          TEXT,
        latitude             DOUBLE PRECISION,
        longitude            DOUBLE PRECISION,
        created_at           TIMESTAMP DEFAULT NOW()
    );

    -- Table des livraisons CTT
    CREATE TABLE IF NOT EXISTS livraisons_ctt (
        id                   TEXT PRIMARY KEY,
        date_livraison       DATE,
        start_time           TIMESTAMP,
        end_time             TIMESTAMP,
        provenance           TEXT,
        superviseur          TEXT,
        type_vehicule        TEXT,
        capacite             TEXT,
        taux_remplissage     TEXT,
        nombre_pneus         INTEGER DEFAULT 0,
        tonnage              INTEGER DEFAULT 0,
        heure_arrivee        TIME,
        heure_depart         TIME,
        duree_minutes        INTEGER,
        observation          TEXT,
        created_at           TIMESTAMP DEFAULT NOW()
    );

    -- Indexation pour la performance des filtres
    CREATE INDEX IF NOT EXISTS idx_collectes_date ON collectes(date_collecte);
    CREATE INDEX IF NOT EXISTS idx_collectes_etat_site ON collectes(etat_site);
    CREATE INDEX IF NOT EXISTS idx_ctt_date ON livraisons_ctt(date_livraison);
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(ddl))
            conn.commit()
        logger.info("✅ Schéma de base de données initialisé avec succès.")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'initialisation de la DB : {e}")
        raise e


def upsert_collectes(records: list[dict]):
    """Insère ou met à jour les collectes dans Neon."""
    if not records: return 0
    engine = get_engine()
    sql = text("""
        INSERT INTO collectes (
            id, date_collecte, start_time, end_time, departement, commune, 
            quartier, zone, lieu_de_collecte, points_visites, points_traites,
            pneus_collectes, mode_collecte, site_transit, etat_site, 
            type_probleme, description_probleme, action_corrective, 
            besoin_appui, detail_appui, superviseur, latitude, longitude
        ) VALUES (
            :id, :date_collecte, :start_time, :end_time, :departement, :commune, 
            :quartier, :zone, :lieu_de_collecte, :points_visites, :points_traites,
            :pneus_collectes, :mode_collecte, :site_transit, :etat_site, 
            :type_probleme, :description_probleme, :action_corrective, 
            :besoin_appui, :detail_appui, :superviseur, :latitude, :longitude
        )
        ON CONFLICT (id) DO UPDATE SET
            date_collecte = EXCLUDED.date_collecte,
            points_visites = EXCLUDED.points_visites,
            points_traites = EXCLUDED.points_traites,
            pneus_collectes = EXCLUDED.pneus_collectes,
            etat_site = EXCLUDED.etat_site,
            type_probleme = EXCLUDED.type_probleme,
            besoin_appui = EXCLUDED.besoin_appui;
    """)
    with engine.connect() as conn:
        conn.execute(sql, records)
        conn.commit()
    return len(records)


def upsert_livraisons_ctt(records: list[dict]):
    """Insère ou met à jour les livraisons CTT dans Neon."""
    if not records: return 0
    engine = get_engine()
    sql = text("""
        INSERT INTO livraisons_ctt (
            id, date_livraison, start_time, end_time, provenance, superviseur, 
            type_vehicule, capacite, taux_remplissage, nombre_pneus, tonnage,
            heure_arrivee, heure_depart, duree_minutes, observation
        ) VALUES (
            :id, :date_livraison, :start_time, :end_time, :provenance, :superviseur, 
            :type_vehicule, :capacite, :taux_remplissage, :nombre_pneus, :tonnage,
            :heure_arrivee, :heure_depart, :duree_minutes, :observation
        )
        ON CONFLICT (id) DO UPDATE SET
            date_livraison = EXCLUDED.date_livraison,
            nombre_pneus = EXCLUDED.nombre_pneus,
            tonnage = EXCLUDED.tonnage,
            taux_remplissage = EXCLUDED.taux_remplissage,
            observation = EXCLUDED.observation;
    """)
    with engine.connect() as conn:
        conn.execute(sql, records)
        conn.commit()
    return len(records)
# Alias pour la compatibilité avec les pages existantes
init_db_ctt = init_db