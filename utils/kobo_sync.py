"""
utils/kobo_sync.py
Synchronisation KoboToolbox → Neon.
Récupère toutes les soumissions via l'API REST KoboToolbox
et les transforme en lignes prêtes à insérer dans PostgreSQL.
"""

import os
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from utils.database import upsert_collectes

load_dotenv()
logger = logging.getLogger(__name__)

KOBO_API_TOKEN = os.getenv("KOBO_API_TOKEN")
KOBO_ASSET_UID = os.getenv("KOBO_ASSET_UID")
KOBO_BASE_URL  = os.getenv("KOBO_BASE_URL", "https://kf.kobotoolbox.org")

# ── Mapping noms de champs KoboToolbox → colonnes Neon ────────────
DEPARTEMENT_LABELS = {
    "dakar":       "Dakar",
    "pikine":      "Pikine",
    "guediawaye":  "Guédiawaye",
    "rufisque":    "Rufisque",
    "keur_massar": "Keur Massar",
}

MODE_LABELS = {
    "manuel":              "Caisse polybenne 16 m³",
    "caisse_polybenne_m3": "Caisse polybenne 20 m³",
    "camion":              "Camion BTP 16 m³",
    "camion_btp_16_m3":   "Camion BTP 16 m³",
    "satellite":           "Satellite",
    "tri":                 "Tricycle",
    "autre":               "Autre",
}

ETAT_LABELS = {
    "normal": "Normal",
    "sature": "Saturé",
    "risque": "Risque",
}

PROBLEME_LABELS = {
    "aucun":      "RAS",
    "logistique": "Logistique",
    "securite":   "Sécurité",
    "acces":      "Accès difficile",
    "population": "Refus population",
    "epi":        "EPI",
    "autre":      "Autre",
}


def _safe_int(val, default=0):
    try:
        return int(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def _safe_date(val):
    if not val:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()
        except ValueError:
            pass
    return None


def _safe_dt(val):
    if not val:
        return None
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except Exception:
        return None


def _extract_commune(submission: dict, departement_key: str) -> str:
    """
    Les communes sont dans des champs select_multiple nommés
    selon le département (Communes_de_Dakar, Communes_de_Pikine, etc.).
    """
    field_map = {
        "dakar":       "Communes_de_Dakar",
        "pikine":      "Communes_de_Pikine",
        "guediawaye":  "Communes_de_Gu_diawaye",
        "rufisque":    "Communes_de_Rufisque",
        "keur_massar": "Communes_de_Keur_Massar",
    }
    field = field_map.get(departement_key, "")
    return submission.get(field, "") or ""


def _extract_gps(submission: dict):
    """Extrait latitude et longitude du champ geopoint KoboToolbox."""
    gps = submission.get("gps", "")
    if gps:
        parts = str(gps).split()
        if len(parts) >= 2:
            try:
                return float(parts[0]), float(parts[1])
            except ValueError:
                pass
    return None, None


def _transform(submission: dict) -> dict:
    """Transforme une soumission KoboToolbox en dict PostgreSQL."""
    dept_key = submission.get("departement", "") or ""
    lat, lon = _extract_gps(submission)

    # Lieu de collecte (select_multiple → liste jointe)
    lieu_raw = submission.get("Lieu_de_collecte", "") or ""
    lieu = " | ".join(
        l.strip().replace("_", " ").title()
        for l in lieu_raw.split()
        if l
    )

    return {
        "id":                  str(submission.get("_id", submission.get("_uuid", ""))),
        "date_collecte":       _safe_date(submission.get("Date")),
        "start_time":          _safe_dt(submission.get("start")),
        "end_time":            _safe_dt(submission.get("end")),
        "departement":         DEPARTEMENT_LABELS.get(dept_key, dept_key),
        "commune":             _extract_commune(submission, dept_key),
        "quartier":            submission.get("Quartiers", "") or "",
        "zone":                submission.get("zone", "") or "",
        "lieu_de_collecte":    lieu,
        "points_visites":      _safe_int(submission.get("points_visites")),
        "points_traites":      _safe_int(submission.get("points_traites")),
        "pneus_collectes":     _safe_int(submission.get("pneus_collectes")),
        "mode_collecte":       MODE_LABELS.get(submission.get("mode_collecte", ""), submission.get("mode_collecte", "")),
        "site_transit":        submission.get("site_transit", "") or "",
        "etat_site":           ETAT_LABELS.get(submission.get("etat_site", ""), submission.get("etat_site", "")),
        "type_probleme":       PROBLEME_LABELS.get(submission.get("type_probleme", ""), submission.get("type_probleme", "")),
        "description_probleme": submission.get("description_probleme", "") or "",
        "action_corrective":   submission.get("action_corrective", "") or "",
        "besoin_appui":        submission.get("besoin_appui", "") or "",
        "detail_appui":        submission.get("detail_appui", "") or "",
        "superviseur":         submission.get("superviseur_001", "") or "",
        "latitude":            lat,
        "longitude":           lon,
    }


def fetch_from_kobo(limit: int = 30000) -> list[dict]:
    """
    Récupère toutes les soumissions depuis l'API KoboToolbox.
    Gère la pagination automatiquement.
    """
    if not KOBO_API_TOKEN:
        raise ValueError("KOBO_API_TOKEN manquant dans le fichier .env")
    if not KOBO_ASSET_UID:
        raise ValueError("KOBO_ASSET_UID manquant dans le fichier .env")

    headers = {"Authorization": f"Token {KOBO_API_TOKEN}"}
    url = f"{KOBO_BASE_URL}/api/v2/assets/{KOBO_ASSET_UID}/data/?format=json&limit=1000"

    all_results = []
    while url and len(all_results) < limit:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        all_results.extend(data.get("results", []))
        url = data.get("next")  # pagination KoboToolbox

    logger.info(f"📥 {len(all_results)} soumissions récupérées depuis KoboToolbox")
    return all_results


def sync_kobo_to_neon() -> dict:
    """
    Point d'entrée principal : fetch KoboToolbox → transform → upsert Neon.
    Retourne un résumé de la synchronisation.
    """
    raw = fetch_from_kobo()
    records = [_transform(s) for s in raw]
    inserted = upsert_collectes(records)

    return {
        "fetched":  len(raw),
        "inserted": inserted,
        "status":   "success",
    }
