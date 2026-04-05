from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import pandas as pd
import sqlite3
import threading
import requests as req
from datetime import datetime
from pathlib import Path

app = Flask(__name__, static_folder=".")
CORS(app)

# ── Configuration ────────────────────────────────────────────────────────────
CACHE_DIR = Path("cache_sirene")
CACHE_DIR.mkdir(exist_ok=True)
NAF_FILE  = Path("int_courts_naf_rev_2.csv")
API_KEY   = "101af1cc-e294-46b0-8ebc-9e9488b302f3"

# ── Chargement NAF ───────────────────────────────────────────────────────────
df_naf = pd.read_csv(NAF_FILE, header=None, usecols=[1, 2],
                     names=["code_naf", "libelle_activite"], dtype=str).dropna(subset=["code_naf"])
df_naf["code_naf_norm"] = df_naf["code_naf"].str.strip().str.replace(".", "", regex=False).str.upper()

def _merge_naf(df):
    df["ape_norm"] = df["ape"].str.replace(".", "", regex=False).str.upper()
    df = df.merge(df_naf[["code_naf_norm", "libelle_activite"]],
                  left_on="ape_norm", right_on="code_naf_norm", how="left"
                  ).drop(columns=["ape_norm", "code_naf_norm"])
    df["annee_creation"] = pd.to_numeric(df["annee_creation"], errors="coerce")
    return df

# ── Chargement de tous les CSV en cache ──────────────────────────────────────
# dfs : { cp: DataFrame }
dfs_lock = threading.Lock()
dfs: dict[str, pd.DataFrame] = {}

def load_csv(cp: str) -> pd.DataFrame:
    path = CACHE_DIR / f"cp_{cp}_actifs.csv"
    df   = pd.read_csv(path, dtype={"siret": str, "siren": str, "code_postal": str})
    return _merge_naf(df)

def load_all_cached():
    for csv in CACHE_DIR.glob("cp_*_actifs.csv"):
        cp = csv.stem.split("_")[1]
        with dfs_lock:
            dfs[cp] = load_csv(cp)
        print(f"  Chargé : {cp} ({len(dfs[cp])} établissements)")

load_all_cached()

# ── Suivi des téléchargements en cours ───────────────────────────────────────
# { cp: {status: 'running'|'done'|'error', count: int, message: str} }
download_jobs: dict[str, dict] = {}

def _extraire_secteur(cp: str):
    """Télécharge les établissements actifs d'un code postal depuis l'API SIRENE."""
    download_jobs[cp] = {"status": "running", "count": 0, "message": "Connexion à l'API SIRENE…"}

    url     = "https://api.insee.fr/api-sirene/3.11/siret"
    headers = {"X-INSEE-Api-Key-Integration": API_KEY, "Accept": "application/json"}

    resultats = []
    curseur   = "*"

    try:
        while True:
            params   = {"q": f"codePostalEtablissement:{cp}", "nombre": 1000, "curseur": curseur}
            response = req.get(url, headers=headers, params=params, timeout=30)

            if response.status_code != 200:
                download_jobs[cp] = {
                    "status":  "error",
                    "count":   len(resultats),
                    "message": f"Erreur API {response.status_code} : {response.text[:200]}"
                }
                return

            data  = response.json()
            etabs = data.get("etablissements", [])
            if not etabs:
                break

            for e in etabs:
                periodes = e.get("periodesEtablissement", [{}])
                periode  = periodes[0] if periodes else {}
                if periode.get("etatAdministratifEtablissement") != "A":
                    continue

                ape   = periode.get("activitePrincipaleEtablissement") or ""
                unite = e.get("uniteLegale", {})
                nom   = (
                    unite.get("denominationUniteLegale")
                    or f"{unite.get('prenomUsuelUniteLegale', '')} {unite.get('nomUniteLegale', '')}"
                )
                adresse        = e.get("adresseEtablissement", {})
                date_raw       = e.get("dateCreationEtablissement")
                annee_creation = None
                if date_raw:
                    try:
                        annee_creation = datetime.strptime(date_raw, "%Y-%m-%d").year
                    except ValueError:
                        pass

                resultats.append({
                    "nom":              nom.strip(),
                    "siret":            e.get("siret", ""),
                    "siren":            e.get("siren", ""),
                    "ape":              ape,
                    "code_postal":      adresse.get("codePostalEtablissement", ""),
                    "ville":            adresse.get("libelleCommuneEtablissement", ""),
                    "date_creation":    date_raw or "",
                    "annee_creation":   annee_creation,
                    "tranche_effectif": e.get("trancheEffectifsEtablissement") or "",
                })

            download_jobs[cp]["count"]   = len(resultats)
            download_jobs[cp]["message"] = f"{len(resultats)} établissements actifs récupérés…"

            curseur_suivant = data.get("header", {}).get("curseurSuivant")
            if not curseur_suivant or curseur_suivant == curseur:
                break
            curseur = curseur_suivant

        df = pd.DataFrame(resultats)
        cache_path = CACHE_DIR / f"cp_{cp}_actifs.csv"
        df.to_csv(cache_path, index=False)

        with dfs_lock:
            dfs[cp] = load_csv(cp)

        download_jobs[cp] = {
            "status":  "done",
            "count":   len(resultats),
            "message": f"{len(resultats)} établissements actifs chargés."
        }

    except Exception as exc:
        download_jobs[cp] = {
            "status":  "error",
            "count":   len(resultats) if resultats else 0,
            "message": str(exc)
        }

# ── Base SQLite de prospection ───────────────────────────────────────────────
DB_FILE = Path("prospection.db")

def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS prospection (
                siret            TEXT PRIMARY KEY,
                nom              TEXT,
                ape              TEXT,
                ville            TEXT,
                annee_creation   TEXT,
                libelle_activite TEXT,
                statut           TEXT NOT NULL DEFAULT 'À contacter',
                notes            TEXT NOT NULL DEFAULT '',
                date_ajout       TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)

init_db()

# ── Routes statiques ─────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(".", "index.html")

# ── Routes cache ─────────────────────────────────────────────────────────────
@app.route("/api/cache/list")
def cache_list():
    """Liste les codes postaux disponibles en cache."""
    with dfs_lock:
        result = [
            {"cp": cp, "count": len(df)}
            for cp, df in sorted(dfs.items())
        ]
    return jsonify(result)


@app.route("/api/cache/<cp>", methods=["DELETE"])
def cache_delete(cp):
    """Supprime le CSV en cache et décharge les données pour un code postal."""
    csv_path = CACHE_DIR / f"cp_{cp}_actifs.csv"
    if not csv_path.exists():
        return jsonify({"error": "cache introuvable"}), 404
    csv_path.unlink()
    with dfs_lock:
        dfs.pop(cp, None)
    download_jobs.pop(cp, None)
    return jsonify({"ok": True})


@app.route("/api/cache/download", methods=["POST"])
def cache_download():
    """Lance le téléchargement d'un code postal en arrière-plan."""
    data = request.get_json(silent=True) or {}
    cp   = str(data.get("cp", "")).strip()
    if not cp:
        return jsonify({"error": "cp requis"}), 400
    if cp in download_jobs and download_jobs[cp]["status"] == "running":
        return jsonify({"error": "téléchargement déjà en cours"}), 409
    thread = threading.Thread(target=_extraire_secteur, args=(cp,), daemon=True)
    thread.start()
    return jsonify({"ok": True, "cp": cp}), 202


@app.route("/api/cache/status/<cp>")
def cache_status(cp):
    """Retourne l'état du téléchargement pour un code postal."""
    if cp not in download_jobs:
        # Déjà en cache ?
        with dfs_lock:
            if cp in dfs:
                return jsonify({"status": "done", "count": len(dfs[cp]), "message": "Déjà en cache."})
        return jsonify({"status": "idle", "count": 0, "message": ""})
    return jsonify(download_jobs[cp])

# ── Routes SIRENE ────────────────────────────────────────────────────────────
@app.route("/api/activites")
def activites():
    """Retourne tous les libellés d'activité pour un CP, triés alphabétiquement."""
    cp = request.args.get("cp", "").strip()
    with dfs_lock:
        if cp and cp in dfs:
            df = dfs[cp]
        elif dfs:
            df = pd.concat(dfs.values(), ignore_index=True)
        else:
            return jsonify([])
    libelles = sorted(df["libelle_activite"].dropna().unique().tolist())
    return jsonify(libelles)


@app.route("/api/entreprises")
def entreprises():
    """Retourne les entreprises dont le libellé correspond exactement."""
    libelle = request.args.get("libelle", "").strip()
    annee   = request.args.get("annee_min", type=int, default=0)
    cp      = request.args.get("cp", "").strip()
    if not libelle:
        return jsonify([])
    with dfs_lock:
        if cp and cp in dfs:
            df = dfs[cp]
        elif dfs:
            df = pd.concat(dfs.values(), ignore_index=True)
        else:
            return jsonify([])
    mask = df["libelle_activite"] == libelle
    if annee:
        mask &= df["annee_creation"] >= annee
    cols   = ["nom", "siret", "ape", "ville", "annee_creation", "libelle_activite"]
    result = (df[mask][cols]
              .sort_values("annee_creation", ascending=False)
              .fillna("")
              .to_dict(orient="records"))
    return jsonify(result)

# ── Routes prospection ───────────────────────────────────────────────────────
@app.route("/api/prospection", methods=["GET"])
def get_prospection():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM prospection ORDER BY date_ajout DESC").fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/prospection", methods=["POST"])
def add_prospection():
    data  = request.get_json(silent=True) or {}
    siret = data.get("siret", "").strip()
    if not siret:
        return jsonify({"error": "siret requis"}), 400
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO prospection (siret, nom, ape, ville, annee_creation, libelle_activite)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (siret, data.get("nom", ""), data.get("ape", ""), data.get("ville", ""),
                  str(data.get("annee_creation", "")), data.get("libelle_activite", "")))
        return jsonify({"ok": True}), 201
    except sqlite3.IntegrityError:
        return jsonify({"error": "déjà dans la liste"}), 409


@app.route("/api/prospection/<siret>", methods=["PATCH"])
def update_prospection(siret):
    data   = request.get_json(silent=True) or {}
    fields = {k: v for k, v in data.items() if k in ("statut", "notes")}
    if not fields:
        return jsonify({"error": "rien à mettre à jour"}), 400
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    with get_db() as conn:
        conn.execute(f"UPDATE prospection SET {set_clause} WHERE siret = ?",
                     list(fields.values()) + [siret])
    return jsonify({"ok": True})


@app.route("/api/prospection/<siret>", methods=["DELETE"])
def delete_prospection(siret):
    with get_db() as conn:
        conn.execute("DELETE FROM prospection WHERE siret = ?", (siret,))
    return jsonify({"ok": True})


if __name__ == "__main__":
    print("Serveur démarré sur http://localhost:5000")
    app.run(debug=True, port=5000)
