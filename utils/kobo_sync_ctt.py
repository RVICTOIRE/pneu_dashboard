"""
utils/kobo_sync_ctt.py
Synchronisation KoboToolbox (CTT → SOCOCIM) → Neon.
Formulaire : suivi des livraisons de pneus depuis les départements vers l'usine SOCOCIM.
"""

import os
import logging
import requests
from datetime import datetime, time
from dotenv import load_dotenv
from utils.database import upsert_livraisons_ctt

load_dotenv()
logger = logging.getLogger(__name__)

KOBO_API_TOKEN    = os.getenv("KOBO_API_TOKEN")
KOBO_ASSET_UID_CTT = os.getenv("KOBO_ASSET_UID_CTT")
KOBO_BASE_URL     = os.getenv("KOBO_BASE_URL", "https://kf.kobotoolbox.org")

# ── Mapping choix KoboToolbox → libellés lisibles ────────────────
PROVENANCE_LABELS = {
    "dakar":       "Dakar",
    "guediawaye":  "Guédiawaye",
    "pikine":      "Pikine",
    "rufisque":    "Rufisque",
    "keur_massar": "Keur Massar",
    "ctt":         "CTT",
}

VEHICULE_LABELS = {
    "camion_20_m3":          "Camion",
    "caisse_polybenne_20_m3": "Caisse polybenne",
}

CAPACITE_LABELS = {
    "16m3":   "16 m³",
    "20_m3":  "20 m³",
    "25_m3":  "25 m³",
}

REMPLISSAGE_LABELS = {
    "plein":         "Plein",
    "3_4_plein":     "3/4 plein",
    "moiti__plein":  "Moitié plein",
    "1_4_plein":     "1/4 plein",
    "vide":          "Vide",
}

# Coefficient de remplissage pour calcul du tonnage estimé
REMPLISSAGE_COEFF = {
    "Plein":        1.0,
    "3/4 plein":    0.75,
    "Moitié plein": 0.5,
    "1/4 plein":    0.25,
    "Vide":         0.0,
}


def _safe_int(val, default=0):
    try:
        return int(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def _safe_date(val):
    if not val:
        return None
    try:
        return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _safe_dt(val):
    if not val:
        return None
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except Exception:
        return None


def _parse_time(val):
    """Extrait un objet time depuis une chaîne HH:MM ou HH:MM:SS."""
    if not val:
        return None
    try:
        parts = str(val).split(":")
        h, m = int(parts[0]), int(parts[1])
        return time(h, m)
    except Exception:
        return None


def _calc_duree(heure_arrivee, heure_depart):
    """Calcule la durée en minutes entre l'arrivée et le départ."""
    if not heure_arrivee or not heure_depart:
        return None
    try:
        arr = datetime.combine(datetime.today(), heure_arrivee)
        dep = datetime.combine(datetime.today(), heure_depart)
        delta = dep - arr
        minutes = int(delta.total_seconds() / 60)
        return minutes if minutes >= 0 else None
    except Exception:
        return None


def _transform(submission: dict) -> dict:
    """Transforme une soumission KoboToolbox CTT en dict PostgreSQL."""

    # Véhicules multiples
    veh_raw = submission.get("Type_de_v_hicule", "") or ""
    vehicule = " | ".join(
        VEHICULE_LABELS.get(v.strip(), v.strip().replace("_", " ").title())
        for v in veh_raw.split()
        if v
    )

    provenance_key = submission.get("Provenance", "") or ""
    capacite_key   = submission.get("Capacit_m3_ou_tonne", "") or ""
    remplissage_key = submission.get("Taux_de_remplissage", "") or ""
    remplissage_label = REMPLISSAGE_LABELS.get(remplissage_key, remplissage_key)

    heure_arrivee = _parse_time(submission.get("Heure_d_arriv_e"))
    heure_depart  = _parse_time(submission.get("Heure_de_d_part"))
    duree         = _calc_duree(heure_arrivee, heure_depart)



    # --- GESTION DU TONNAGE (Nouveau nom de colonne Kobo) ---
    # On récupère 'Poids_total_en_tonne_t'
    tonnage_raw = (
        submission.get("Tonnage") or 
        submission.get("poids_total_en_tonne_t") or 
        submission.get("tonnage")
    )
    def _safe_float(val):
        if val is None or str(val).strip() == "": return 0.0
        try:
            return float(str(val).replace(',', '.').strip())
        except:
            return 0.0

    tonnage_val = _safe_float(tonnage_raw)
    try:
        # On remplace la virgule par un point si nécessaire et on convertit en float
        tonnage_val = float(str(tonnage_raw).replace(',', '.')) if tonnage_raw else 0.0
    except (ValueError, TypeError):
        tonnage_val = 0.0

    return {
        "id":               str(submission.get("_id", submission.get("_uuid", ""))),
        "date_livraison":   _safe_date(submission.get("Date")),
        "start_time":       _safe_dt(submission.get("start")),
        "end_time":         _safe_dt(submission.get("end")),
        "provenance":       PROVENANCE_LABELS.get(provenance_key, provenance_key),
        "superviseur":      submission.get("Superviseur", "") or "",
        "type_vehicule":    vehicule,
        "capacite":         CAPACITE_LABELS.get(capacite_key, capacite_key),
        "taux_remplissage": remplissage_label,
        "nombre_pneus":     _safe_int(submission.get("Nombre_de_pneus")),
        "tonnage":          tonnage_val, # Enregistré dans la colonne 'tonnage' de Neon
        "heure_arrivee":    heure_arrivee,
        "heure_depart":     heure_depart,
        "duree_minutes":    duree,
        "observation":      submission.get("Observation", "") or "",
    }


def fetch_from_kobo_ctt(limit: int = 30000) -> list[dict]:
    """Récupère toutes les soumissions CTT depuis KoboToolbox (avec pagination)."""
    if not KOBO_API_TOKEN:
        raise ValueError("KOBO_API_TOKEN manquant dans le fichier .env")
    if not KOBO_ASSET_UID_CTT:
        raise ValueError("KOBO_ASSET_UID_CTT manquant dans le fichier .env")

    headers = {"Authorization": f"Token {KOBO_API_TOKEN}"}
    url = f"{KOBO_BASE_URL}/api/v2/assets/{KOBO_ASSET_UID_CTT}/data/?format=json&limit=1000"

    all_results = []
    while url and len(all_results) < limit:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        all_results.extend(data.get("results", []))
        url = data.get("next")

    logger.info(f"📥 {len(all_results)} livraisons CTT récupérées depuis KoboToolbox")
    return all_results


def sync_ctt_to_neon() -> dict:
    """Point d'entrée : fetch KoboToolbox CTT → transform → upsert Neon."""
    raw     = fetch_from_kobo_ctt()
    records = [_transform(s) for s in raw]
    inserted = upsert_livraisons_ctt(records)
    return {"fetched": len(raw), "inserted": inserted, "status": "success"}
