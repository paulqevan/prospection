import os
from flask import Flask, jsonify, request, send_from_directory, session, redirect, url_for
from flask_cors import CORS
import threading
import csv as csvlib
import requests as req
from datetime import datetime
from pathlib import Path

app = Flask(__name__, static_folder=".")
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")
CORS(app)

APP_PASSWORD = os.environ.get("APP_PASSWORD", "")

# ── Authentification ──────────────────────────────────────────────────────────
LOGIN_PAGE = """<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Connexion</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Segoe UI', system-ui, sans-serif;
      background: #f0f2f5; min-height: 100vh;
      display: flex; align-items: center; justify-content: center;
    }
    .card {
      background: #fff; border-radius: 16px; padding: 2.5rem 2rem;
      box-shadow: 0 4px 24px rgba(0,0,0,.1); width: 340px; max-width: 95vw;
    }
    .logo { display: flex; align-items: center; gap: 0.6rem; margin-bottom: 1.75rem; }
    .logo svg { color: #4f6ef7; }
    .logo h1 { font-size: 1.1rem; font-weight: 700; color: #1a1a2e; }
    label { font-size: 0.8rem; color: #6b7280; display: block; margin-bottom: 0.35rem; }
    input[type=password] {
      width: 100%; padding: 0.7rem 1rem; border: 1.5px solid #d1d5db;
      border-radius: 8px; font-size: 0.95rem; outline: none; transition: border-color .2s;
      margin-bottom: 1rem;
    }
    input[type=password]:focus { border-color: #4f6ef7; }
    button {
      width: 100%; padding: 0.75rem; border-radius: 8px; border: none;
      background: #4f6ef7; color: #fff; font-size: 0.95rem; font-weight: 600;
      cursor: pointer; transition: background .15s;
    }
    button:hover { background: #3b5de7; }
    .error {
      background: #fef2f2; color: #dc2626; border-radius: 8px;
      padding: 0.6rem 0.9rem; font-size: 0.82rem; margin-bottom: 1rem;
    }
  </style>
</head>
<body>
  <div class="card">
    <div class="logo">
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <rect x="2" y="7" width="20" height="14" rx="2"/>
        <path d="M16 7V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v2"/>
      </svg>
      <h1>Prospection SIRENE</h1>
    </div>
    {error}
    <form method="POST" action="/login">
      <label for="pwd">Mot de passe</label>
      <input id="pwd" type="password" name="password" autofocus placeholder="••••••••" />
      <button type="submit">Accéder</button>
    </form>
  </div>
</body>
</html>"""

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("password") == APP_PASSWORD:
            session["auth"] = True
            return redirect("/")
        return LOGIN_PAGE.replace("{error}", '<div class="error">Mot de passe incorrect.</div>')
    return LOGIN_PAGE.replace("{error}", "")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")

@app.before_request
def require_auth():
    if APP_PASSWORD and request.endpoint not in ("login", "static"):
        if not session.get("auth"):
            return redirect("/login")

# ── Configuration ────────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL")   # Supabase en prod, absent en local
USE_PG       = bool(DATABASE_URL)
CACHE_DIR    = Path("cache_sirene")
NAF_FILE     = Path("int_courts_naf_rev_2.csv")
API_KEY      = os.environ.get("SIRENE_API_KEY", "101af1cc-e294-46b0-8ebc-9e9488b302f3")
DB_FILE      = Path("prospection.db")           # SQLite local uniquement

# ── Table NAF en mémoire (≈700 lignes, statique) ────────────────────────────
naf_map: dict[str, str] = {}
with open(NAF_FILE, newline="", encoding="utf-8") as f:
    for row in csvlib.reader(f):
        if len(row) >= 3:
            code = row[1].strip().replace(".", "").upper()
            if code:
                naf_map[code] = row[2].strip()

def _naf_libelle(ape: str) -> str:
    return naf_map.get(ape.replace(".", "").upper(), "")

# ── Couche base de données (SQLite local / PostgreSQL prod) ──────────────────
class _Conn:
    """Wrapper fin qui unifie sqlite3 et psycopg2."""

    def __init__(self):
        if USE_PG:
            import psycopg2
            import psycopg2.extras
            self._conn = psycopg2.connect(DATABASE_URL,
                                          cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            import sqlite3
            self._conn = sqlite3.connect(str(DB_FILE))
            self._conn.row_factory = sqlite3.Row

    def _sql(self, sql: str) -> str:
        """Remplace les ? par %s pour psycopg2."""
        return sql.replace("?", "%s") if USE_PG else sql

    def execute(self, sql: str, params=()):
        if USE_PG:
            cur = self._conn.cursor()
            cur.execute(self._sql(sql), list(params))
            return cur
        return self._conn.execute(sql, params)

    def executemany(self, sql: str, rows):
        if USE_PG:
            import psycopg2.extras
            cur = self._conn.cursor()
            psycopg2.extras.execute_batch(cur, self._sql(sql), rows, page_size=500)
            return cur
        return self._conn.executemany(sql, rows)

    def __enter__(self):
        return self

    def __exit__(self, exc, *_):
        try:
            if exc:
                self._conn.rollback()
            else:
                self._conn.commit()
        finally:
            self._conn.close()

def get_db() -> _Conn:
    return _Conn()

# ── Initialisation des tables ────────────────────────────────────────────────
NOW_DEFAULT = "NOW()" if USE_PG else "datetime('now','localtime')"

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS etablissements (
                siret            TEXT,
                siren            TEXT,
                nom              TEXT,
                ape              TEXT,
                code_postal      TEXT NOT NULL,
                ville            TEXT,
                date_creation    TEXT,
                annee_creation   INTEGER,
                tranche_effectif TEXT,
                libelle_activite TEXT,
                PRIMARY KEY (siret, code_postal)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_etab_cp       ON etablissements(code_postal)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_etab_activite  ON etablissements(libelle_activite)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_etab_annee     ON etablissements(annee_creation)")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS prospection (
                siret            TEXT PRIMARY KEY,
                nom              TEXT,
                ape              TEXT,
                ville            TEXT,
                annee_creation   TEXT,
                libelle_activite TEXT,
                statut           TEXT NOT NULL DEFAULT 'À contacter',
                notes            TEXT NOT NULL DEFAULT '',
                date_contact     TEXT NOT NULL DEFAULT '',
                date_ajout       TEXT NOT NULL DEFAULT ({NOW_DEFAULT})
            )
        """)
        # Migration : ajout de date_contact sur les bases existantes
        try:
            conn.execute("ALTER TABLE prospection ADD COLUMN date_contact TEXT NOT NULL DEFAULT ''")
        except Exception:
            pass  # colonne déjà présente
    if not USE_PG:
        _migrate_csv_to_db()

def _migrate_csv_to_db():
    """Import des CSV existants dans SQLite au premier démarrage (local uniquement)."""
    if not CACHE_DIR.exists():
        return
    for csv_path in CACHE_DIR.glob("cp_*_actifs.csv"):
        cp = csv_path.stem.split("_")[1]
        with get_db() as conn:
            already = conn.execute(
                "SELECT COUNT(*) FROM etablissements WHERE code_postal = ?", (cp,)
            ).fetchone()[0]
        if already:
            continue
        print(f"  Migration CSV → DB pour {cp}…")
        rows = []
        with open(csv_path, newline="", encoding="utf-8") as f:
            for r in csvlib.DictReader(f):
                annee_raw = r.get("annee_creation", "")
                try:
                    annee = int(float(annee_raw)) if annee_raw else None
                except ValueError:
                    annee = None
                rows.append((
                    r.get("siret", ""), r.get("siren", ""), r.get("nom", ""),
                    r.get("ape", ""), r.get("code_postal", cp), r.get("ville", ""),
                    r.get("date_creation", ""), annee, r.get("tranche_effectif", ""),
                    _naf_libelle(r.get("ape", "")),
                ))
        with get_db() as conn:
            conn.executemany("""
                INSERT INTO etablissements
                (siret, siren, nom, ape, code_postal, ville, date_creation,
                 annee_creation, tranche_effectif, libelle_activite)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
            """, rows)
        print(f"  {len(rows)} établissements importés pour {cp}.")

init_db()

# ── Suivi des téléchargements en cours ───────────────────────────────────────
download_jobs: dict[str, dict] = {}

def _extraire_secteur(cp: str):
    """Télécharge les établissements actifs d'un CP et les insère en DB."""
    download_jobs[cp] = {"status": "running", "count": 0, "message": "Connexion à l'API SIRENE…"}

    url     = "https://api.insee.fr/api-sirene/3.11/siret"
    headers = {"X-INSEE-Api-Key-Integration": API_KEY, "Accept": "application/json"}
    rows    = []
    curseur = "*"

    try:
        while True:
            params   = {"q": f"codePostalEtablissement:{cp}", "nombre": 1000, "curseur": curseur}
            response = req.get(url, headers=headers, params=params, timeout=30)

            if response.status_code != 200:
                download_jobs[cp] = {
                    "status":  "error",
                    "count":   len(rows),
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

                ape     = periode.get("activitePrincipaleEtablissement") or ""
                unite   = e.get("uniteLegale", {})
                nom     = (
                    unite.get("denominationUniteLegale")
                    or f"{unite.get('prenomUsuelUniteLegale', '')} {unite.get('nomUniteLegale', '')}".strip()
                )
                adresse  = e.get("adresseEtablissement", {})
                date_raw = e.get("dateCreationEtablissement")
                annee    = None
                if date_raw:
                    try:
                        annee = datetime.strptime(date_raw, "%Y-%m-%d").year
                    except ValueError:
                        pass

                rows.append((
                    e.get("siret", ""), e.get("siren", ""), nom.strip(), ape,
                    adresse.get("codePostalEtablissement", cp),
                    adresse.get("libelleCommuneEtablissement", ""),
                    date_raw or "", annee,
                    e.get("trancheEffectifsEtablissement") or "",
                    _naf_libelle(ape),
                ))

            download_jobs[cp]["count"]   = len(rows)
            download_jobs[cp]["message"] = f"{len(rows)} établissements actifs récupérés…"

            curseur_suivant = data.get("header", {}).get("curseurSuivant")
            if not curseur_suivant or curseur_suivant == curseur:
                break
            curseur = curseur_suivant

        with get_db() as conn:
            conn.execute("DELETE FROM etablissements WHERE code_postal = ?", (cp,))
            conn.executemany("""
                INSERT INTO etablissements
                (siret, siren, nom, ape, code_postal, ville, date_creation,
                 annee_creation, tranche_effectif, libelle_activite)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
            """, rows)

        download_jobs[cp] = {
            "status":  "done",
            "count":   len(rows),
            "message": f"{len(rows)} établissements actifs chargés."
        }

    except Exception as exc:
        download_jobs[cp] = {
            "status":  "error",
            "count":   len(rows),
            "message": str(exc)
        }

# ── Routes statiques ─────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(".", "index.html")

# ── Routes cache ─────────────────────────────────────────────────────────────
@app.route("/api/cache/list")
def cache_list():
    with get_db() as conn:
        rows = conn.execute("""
            SELECT code_postal AS cp, COUNT(*) AS count
            FROM etablissements
            GROUP BY code_postal
            ORDER BY code_postal
        """).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/cache/<cp>", methods=["DELETE"])
def cache_delete(cp):
    with get_db() as conn:
        deleted = conn.execute(
            "DELETE FROM etablissements WHERE code_postal = ?", (cp,)
        ).rowcount
    if deleted == 0:
        return jsonify({"error": "cache introuvable"}), 404
    download_jobs.pop(cp, None)
    return jsonify({"ok": True})


@app.route("/api/cache/download", methods=["POST"])
def cache_download():
    data = request.get_json(silent=True) or {}
    cp   = str(data.get("cp", "")).strip()
    if not cp:
        return jsonify({"error": "cp requis"}), 400
    if cp in download_jobs and download_jobs[cp]["status"] == "running":
        return jsonify({"error": "téléchargement déjà en cours"}), 409
    threading.Thread(target=_extraire_secteur, args=(cp,), daemon=True).start()
    return jsonify({"ok": True, "cp": cp}), 202


@app.route("/api/cache/status/<cp>")
def cache_status(cp):
    if cp not in download_jobs:
        with get_db() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM etablissements WHERE code_postal = ?", (cp,)
            ).fetchone()[0]
        if count:
            return jsonify({"status": "done", "count": count, "message": "Déjà en cache."})
        return jsonify({"status": "idle", "count": 0, "message": ""})
    return jsonify(download_jobs[cp])

# ── Routes SIRENE ────────────────────────────────────────────────────────────
@app.route("/api/activites")
def activites():
    cp = request.args.get("cp", "").strip()
    with get_db() as conn:
        if cp:
            rows = conn.execute("""
                SELECT DISTINCT libelle_activite FROM etablissements
                WHERE code_postal = ? AND libelle_activite != ''
                ORDER BY libelle_activite
            """, (cp,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT DISTINCT libelle_activite FROM etablissements
                WHERE libelle_activite != ''
                ORDER BY libelle_activite
            """).fetchall()
    return jsonify([r["libelle_activite"] for r in rows])


@app.route("/api/entreprises")
def entreprises():
    libelle = request.args.get("libelle", "").strip()
    annee   = request.args.get("annee_min", type=int, default=0)
    cp      = request.args.get("cp", "").strip()
    if not libelle:
        return jsonify([])

    conditions = ["libelle_activite = ?"]
    params     = [libelle]
    if cp:
        conditions.append("code_postal = ?")
        params.append(cp)
    if annee:
        conditions.append("annee_creation >= ?")
        params.append(annee)

    sql = f"""
        SELECT nom, siret, ape, code_postal, ville, annee_creation, libelle_activite
        FROM etablissements
        WHERE {' AND '.join(conditions)}
        ORDER BY annee_creation DESC
    """
    with get_db() as conn:
        rows = conn.execute(sql, params).fetchall()
    return jsonify([dict(r) for r in rows])

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
    except Exception:
        return jsonify({"error": "déjà dans la liste"}), 409


@app.route("/api/prospection/<siret>", methods=["PATCH"])
def update_prospection(siret):
    data   = request.get_json(silent=True) or {}
    fields = {k: v for k, v in data.items() if k in ("statut", "notes", "date_contact")}
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
    port = int(os.environ.get("PORT", 5000))
    debug = not USE_PG
    print(f"Serveur démarré sur http://localhost:{port} ({'PostgreSQL' if USE_PG else 'SQLite'})")
    app.run(debug=debug, port=port, host="0.0.0.0")
