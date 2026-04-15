"""
utils/database.py
Gestion de la connexion Neon (PostgreSQL) et initialisation du schéma.
"""

import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("NEON_DATABASE_URL")


def get_engine():
    """Retourne un engine SQLAlchemy connecté à Neon."""
    if not DATABASE_URL:
        raise ValueError(
            "NEON_DATABASE_URL manquant. Vérifiez votre fichier .env"
        )
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def init_db():
    """
    Crée la table `collectes` si elle n'existe pas encore.
    À appeler une seule fois au démarrage de l'app.
    """
    engine = get_engine()
    ddl = """
    CREATE TABLE IF NOT EXISTS collectes (
        id                  TEXT PRIMARY KEY,
        date_collecte       DATE,
        start_time          TIMESTAMP,
        end_time            TIMESTAMP,
        departement         TEXT,
        commune             TEXT,
        quartier            TEXT,
        zone                TEXT,
        lieu_de_collecte    TEXT,
        points_visites      INTEGER DEFAULT 0,
        points_traites      INTEGER DEFAULT 0,
        pneus_collectes     INTEGER DEFAULT 0,
        mode_collecte       TEXT,
        site_transit        TEXT,
        etat_site           TEXT,
        type_probleme       TEXT,
        description_probleme TEXT,
        action_corrective   TEXT,
        besoin_appui        TEXT,
        detail_appui        TEXT,
        superviseur         TEXT,
        latitude            DOUBLE PRECISION,
        longitude           DOUBLE PRECISION,
        created_at          TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_collectes_date        ON collectes(date_collecte);
    CREATE INDEX IF NOT EXISTS idx_collectes_departement ON collectes(departement);
    CREATE INDEX IF NOT EXISTS idx_collectes_etat_site   ON collectes(etat_site);
    """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
    logger.info("✅ Table `collectes` prête.")


def upsert_collectes(records: list[dict]):
    """
    Insère ou met à jour les enregistrements dans Neon.
    Utilise ON CONFLICT DO UPDATE pour la synchronisation idempotente.
    """
    if not records:
        return 0

    engine = get_engine()
    sql = text("""
        INSERT INTO collectes (
            id, date_collecte, start_time, end_time,
            departement, commune, quartier, zone,
            lieu_de_collecte, points_visites, points_traites,
            pneus_collectes, mode_collecte, site_transit,
            etat_site, type_probleme, description_probleme,
            action_corrective, besoin_appui, detail_appui,
            superviseur, latitude, longitude
        ) VALUES (
            :id, :date_collecte, :start_time, :end_time,
            :departement, :commune, :quartier, :zone,
            :lieu_de_collecte, :points_visites, :points_traites,
            :pneus_collectes, :mode_collecte, :site_transit,
            :etat_site, :type_probleme, :description_probleme,
            :action_corrective, :besoin_appui, :detail_appui,
            :superviseur, :latitude, :longitude
        )
        ON CONFLICT (id) DO UPDATE SET
            date_collecte       = EXCLUDED.date_collecte,
            points_visites      = EXCLUDED.points_visites,
            points_traites      = EXCLUDED.points_traites,
            pneus_collectes     = EXCLUDED.pneus_collectes,
            etat_site           = EXCLUDED.etat_site,
            type_probleme       = EXCLUDED.type_probleme,
            besoin_appui        = EXCLUDED.besoin_appui
    """)

    with engine.connect() as conn:
        conn.execute(sql, records)
        conn.commit()

    return len(records)
