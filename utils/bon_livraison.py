"""
utils/bon_livraison.py
Génération des bons de livraison au format PDF pour les livraisons CTT → SOCOCIM.
"""

import io
from fpdf import FPDF
from datetime import datetime


# Palette de couleurs (cohérente avec le dashboard)
BLEU_FONCE  = (26, 39, 68)    # #1a2744
BLEU_MOYEN  = (44, 74, 140)   # #2c4a8c
BLEU_CLAIR  = (220, 232, 245) # #dce8f5
GRIS_TEXTE  = (107, 127, 160) # #6b7fa0
BLANC       = (255, 255, 255)
NOIR        = (0, 0, 0)


def _val(row: dict, key: str, default: str = "—") -> str:
    """Retourne la valeur d'une clé sous forme de chaîne propre."""
    v = row.get(key)
    if v is None or str(v).strip() in ("", "nan", "None", "NaT"):
        return default
    if key == "date_livraison":
        try:
            return datetime.strptime(str(v)[:10], "%Y-%m-%d").strftime("%d/%m/%Y")
        except Exception:
            return str(v)[:10]
    if key in ("heure_arrivee", "heure_depart"):
        return str(v)[:5] if len(str(v)) >= 5 else str(v)
    if key == "tonnage":
        try:
            return f"{float(v):.2f} t"
        except Exception:
            return str(v)
    return str(v).strip()


def _numero_bon(row: dict) -> str:
    """Génère un numéro de bon lisible depuis l'id de la livraison."""
    raw = str(row.get("id", "")).strip()
    if raw and raw not in ("", "nan", "None"):
        return raw[:12].upper()
    date = _val(row, "date_livraison", "00000000").replace("/", "")
    prov = str(row.get("provenance", "CTT"))[:3].upper()
    return f"BL-{prov}-{date}"


def generer_bon(row: dict) -> bytes:
    """
    Génère un bon de livraison PDF pour une livraison donnée.

    Parameters
    ----------
    row : dict
        Dictionnaire représentant une ligne du dataframe livraisons_ctt.

    Returns
    -------
    bytes
        Contenu binaire du fichier PDF.
    """
    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.add_page()
    pdf.set_auto_page_break(auto=False)

    W = pdf.w - pdf.l_margin - pdf.r_margin  # largeur utile

    # ── En-tête ──────────────────────────────────────────────────────
    pdf.set_fill_color(*BLEU_FONCE)
    pdf.rect(x=10, y=10, w=190, h=28, style="F")

    pdf.set_xy(12, 13)
    pdf.set_font("Helvetica", "B", 15)
    pdf.set_text_color(*BLANC)
    pdf.cell(130, 8, "SONAGED · CTT → SOCOCIM", ln=0)

    pdf.set_xy(12, 22)
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(165, 180, 214)
    pdf.cell(130, 6, "Bon de livraison de pneus usagés", ln=0)

    # Numéro de bon (haut droite)
    pdf.set_xy(142, 13)
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_text_color(*BLANC)
    pdf.cell(56, 6, f"N° {_numero_bon(row)}", align="R", ln=0)
    pdf.set_xy(142, 20)
    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(165, 180, 214)
    pdf.cell(56, 6, f"Généré le {datetime.now().strftime('%d/%m/%Y à %H:%M')}", align="R")

    y = 44

    # ── Sous-titre de section ─────────────────────────────────────────
    def section_title(title: str, y_pos: float) -> float:
        pdf.set_fill_color(*BLEU_CLAIR)
        pdf.rect(x=10, y=y_pos, w=190, h=7, style="F")
        pdf.set_xy(12, y_pos + 0.5)
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_text_color(*BLEU_MOYEN)
        pdf.cell(186, 6, title.upper(), ln=1)
        return y_pos + 9

    # ── Ligne de champ ────────────────────────────────────────────────
    def champ(label: str, valeur: str, x: float, y_pos: float, w_lbl: float = 42, w_val: float = 50):
        pdf.set_xy(x, y_pos)
        pdf.set_font("Helvetica", "", 7.5)
        pdf.set_text_color(*GRIS_TEXTE)
        pdf.cell(w_lbl, 6, label, ln=0)
        pdf.set_font("Helvetica", "B", 8.5)
        pdf.set_text_color(*NOIR)
        pdf.cell(w_val, 6, valeur, ln=0)

    # ── Informations de livraison ─────────────────────────────────────
    y = section_title("Informations de livraison", y)
    champ("Date de livraison :", _val(row, "date_livraison"), x=12, y_pos=y)
    champ("Heure d'arrivée :", _val(row, "heure_arrivee"), x=107, y_pos=y)
    y += 8
    champ("Provenance :", _val(row, "provenance"), x=12, y_pos=y)
    champ("Heure de départ :", _val(row, "heure_depart"), x=107, y_pos=y)
    y += 8
    champ("Superviseur :", _val(row, "superviseur"), x=12, y_pos=y)
    duree = _val(row, "duree_minutes")
    champ("Durée sur site :", f"{duree} min" if duree != "—" else "—", x=107, y_pos=y)
    y += 14

    # ── Informations véhicule ─────────────────────────────────────────
    y = section_title("Informations véhicule", y)
    champ("Type de véhicule :", _val(row, "type_vehicule"), x=12, y_pos=y)
    champ("Capacité :", _val(row, "capacite"), x=107, y_pos=y)
    y += 8
    champ("Taux de remplissage :", _val(row, "taux_remplissage"), x=12, y_pos=y)
    y += 14

    # ── Détails de la cargaison ───────────────────────────────────────
    y = section_title("Détails de la cargaison", y)

    # Cases encadrées pour les métriques clés
    def metric_box(label: str, valeur: str, x: float, y_pos: float, w: float = 55, h: float = 20):
        pdf.set_fill_color(*BLEU_CLAIR)
        pdf.set_draw_color(*BLEU_MOYEN)
        pdf.rect(x=x, y=y_pos, w=w, h=h, style="FD")
        pdf.set_xy(x + 2, y_pos + 2)
        pdf.set_font("Helvetica", "", 7)
        pdf.set_text_color(*GRIS_TEXTE)
        pdf.cell(w - 4, 5, label, ln=1, align="C")
        pdf.set_xy(x + 2, y_pos + 8)
        pdf.set_font("Helvetica", "B", 14)
        pdf.set_text_color(*BLEU_FONCE)
        pdf.cell(w - 4, 10, valeur, ln=0, align="C")

    metric_box("Nombre de pneus", _val(row, "nombre_pneus"), x=12,  y_pos=y)
    metric_box("Tonnage total",   _val(row, "tonnage"),       x=72,  y_pos=y)
    y += 26

    # ── Observation ───────────────────────────────────────────────────
    y = section_title("Observation / Remarques", y)
    obs = _val(row, "observation", "Aucune observation")
    pdf.set_xy(12, y)
    pdf.set_font("Helvetica", "", 8.5)
    pdf.set_text_color(*NOIR)
    pdf.multi_cell(186, 6, obs)
    y = pdf.get_y() + 8
    if y > 200:
        y = 200

    # ── Signatures ────────────────────────────────────────────────────
    y = section_title("Signatures", y)
    sig_y = y + 2

    # Boîte signature CTT
    pdf.set_draw_color(*BLEU_MOYEN)
    pdf.set_fill_color(*BLANC)
    pdf.rect(x=12, y=sig_y, w=80, h=28, style="D")
    pdf.set_xy(12, sig_y + 1)
    pdf.set_font("Helvetica", "B", 7.5)
    pdf.set_text_color(*BLEU_MOYEN)
    pdf.cell(80, 5, "Responsable CTT", align="C")

    # Boîte signature SOCOCIM
    pdf.rect(x=108, y=sig_y, w=80, h=28, style="D")
    pdf.set_xy(108, sig_y + 1)
    pdf.set_font("Helvetica", "B", 7.5)
    pdf.set_text_color(*BLEU_MOYEN)
    pdf.cell(80, 5, "Responsable SOCOCIM", align="C")

    # Lignes de signature
    line_y = sig_y + 22
    pdf.set_draw_color(180, 180, 180)
    pdf.line(22, line_y, 82, line_y)
    pdf.line(118, line_y, 178, line_y)

    pdf.set_xy(12, line_y + 2)
    pdf.set_font("Helvetica", "", 6.5)
    pdf.set_text_color(*GRIS_TEXTE)
    pdf.cell(80, 4, "Nom, prénom et cachet", align="C")
    pdf.set_xy(108, line_y + 2)
    pdf.cell(80, 4, "Nom, prénom et cachet", align="C")

    # ── Pied de page ──────────────────────────────────────────────────
    pdf.set_fill_color(*BLEU_FONCE)
    pdf.rect(x=10, y=281, w=190, h=8, style="F")
    pdf.set_xy(12, 282)
    pdf.set_font("Helvetica", "", 6.5)
    pdf.set_text_color(165, 180, 214)
    pdf.cell(186, 6,
             "SONAGED · Société Nationale de Gestion des Déchets — Projet Pneus Usagés · Dakar, Sénégal",
             align="C")

    buffer = io.BytesIO()
    pdf.output(buffer)
    return buffer.getvalue()


def generer_tous_les_bons(df) -> bytes:
    """
    Génère un ZIP contenant un PDF par livraison.

    Parameters
    ----------
    df : pd.DataFrame
        Toutes les lignes à exporter.

    Returns
    -------
    bytes
        Archive ZIP en mémoire.
    """
    import zipfile

    buf_zip = io.BytesIO()
    with zipfile.ZipFile(buf_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            pdf_bytes = generer_bon(row_dict)
            num = _numero_bon(row_dict)
            date = _val(row_dict, "date_livraison", "00000000").replace("/", "-")
            filename = f"bon_livraison_{date}_{num}.pdf"
            zf.writestr(filename, pdf_bytes)
    return buf_zip.getvalue()
