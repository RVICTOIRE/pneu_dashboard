# ♻️ SONAGED · Dashboard Ramassage de Pneus

Tableau de bord Streamlit pour le suivi opérationnel du dispositif de ramassage de pneus.

**Architecture :** `KoboToolbox API → Neon (PostgreSQL) → Streamlit`

---

## 📁 Structure du projet

```
pneu_dashboard/
├── app.py                        # Application Streamlit principale
├── requirements.txt
├── .env.example                  # Variables d'environnement (à copier en .env)
├── .streamlit/
│   ├── config.toml               # Thème et configuration Streamlit
│   └── secrets.toml.example      # Template secrets (pour Streamlit Cloud)
└── utils/
    ├── database.py               # Connexion Neon + initialisation schéma
    ├── kobo_sync.py              # Fetch KoboToolbox + transformation
    └── data_loader.py            # Chargement données + cache Streamlit
```

---

## ⚙️ Installation locale

### 1. Cloner et installer les dépendances

```bash
git clone <votre-repo>
cd pneu_dashboard
pip install -r requirements.txt
```

### 2. Configurer les variables d'environnement

```bash
cp .env.example .env
```

Éditez `.env` avec vos vraies valeurs :

```env
NEON_DATABASE_URL=postgresql://user:password@ep-xxxx.region.aws.neon.tech/neondb?sslmode=require
KOBO_API_TOKEN=votre_token_kobo
KOBO_ASSET_UID=votre_asset_uid
KOBO_BASE_URL=https://kf.kobotoolbox.org
```

### 3. Obtenir vos credentials

#### 🟢 Neon (PostgreSQL gratuit)
1. Créez un compte sur [neon.tech](https://neon.tech)
2. Créez un projet → Copiez la **Connection String**
3. Format : `postgresql://user:password@ep-xxxx.region.aws.neon.tech/neondb?sslmode=require`

#### 🔵 KoboToolbox
1. Connectez-vous sur [kf.kobotoolbox.org](https://kf.kobotoolbox.org)
2. **Token API** : `Compte (avatar) → Paramètres → API token`
3. **Asset UID** : Ouvrez votre formulaire → regardez l'URL :
   `https://kf.kobotoolbox.org/#/forms/**aXXXXXXXXXXXX**/summary`

### 4. Lancer l'application

```bash
streamlit run app.py
```

---

## 🚀 Déploiement sur Streamlit Cloud (gratuit)

### Étape 1 — Pousser sur GitHub
```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/votre-user/pneu-dashboard.git
git push -u origin main
```

> ⚠️ Ajoutez `.env` et `.streamlit/secrets.toml` à votre `.gitignore` !

### Étape 2 — Déployer sur Streamlit Cloud
1. Allez sur [share.streamlit.io](https://share.streamlit.io)
2. Connectez votre compte GitHub
3. Choisissez votre repo → branch `main` → fichier `app.py`
4. Cliquez **Advanced settings** → **Secrets** et collez :

```toml
NEON_DATABASE_URL = "postgresql://..."
KOBO_API_TOKEN = "..."
KOBO_ASSET_UID = "..."
KOBO_BASE_URL = "https://kf.kobotoolbox.org"
```

### Étape 3 — Première synchronisation
Une fois déployé, cliquez le bouton **"Synchroniser depuis KoboToolbox"** dans la sidebar.

---

## 📊 KPIs disponibles

| KPI | Description |
|-----|-------------|
| **Total pneus collectés** | Somme de tous les pneus ramassés |
| **Taux de traitement** | Points traités / Points visités × 100 |
| **Sites saturés** | Nombre de sites en état "Saturé" (alerte rouge) |
| **Besoins d'appui** | Collectes nécessitant un renfort |
| **Pneus par département** | Répartition géographique |
| **Performance par superviseur** | Suivi par responsable de zone |
| **Lieux de collecte** | Garages, vulcanisateurs, bords de route... |
| **Modes de collecte** | Polybenne, camion BTP, tricycle... |
| **Problèmes terrain** | Logistique, sécurité, accès, population... |
| **Évolution temporelle** | Courbe journalière + cumulative |
| **Carte GPS** | Localisation des points (si GPS renseigné) |

---

## 🔄 Synchronisation des données

La synchronisation est **manuelle** (bouton sidebar) ou peut être automatisée :

### Synchronisation automatique (optionnel)
Créez un script `sync_job.py` :

```python
from utils.kobo_sync import sync_kobo_to_neon
result = sync_kobo_to_neon()
print(f"Sync : {result['inserted']} enregistrements")
```

Puis planifiez avec cron (Linux) :
```bash
# Sync toutes les heures
0 * * * * cd /path/to/pneu_dashboard && python sync_job.py
```

Ou utilisez un service comme **GitHub Actions** pour déclencher la sync automatiquement.

---

## 🛠️ Dépannage

| Erreur | Solution |
|--------|----------|
| `NEON_DATABASE_URL manquant` | Vérifiez votre `.env` ou Secrets Streamlit Cloud |
| `KOBO_API_TOKEN manquant` | Récupérez votre token sur kf.kobotoolbox.org |
| `401 Unauthorized` (Kobo) | Token expiré ou invalide — régénérez-le |
| `SSL error` (Neon) | Vérifiez `?sslmode=require` dans l'URL |
| `Table not found` | La table se crée automatiquement au premier lancement |
