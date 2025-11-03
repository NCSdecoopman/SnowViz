#!/usr/bin/env python3
# fetch_observations.py
# Objectif:
#   Pour une date UTC donnée, récupérer pour chaque station la dernière mesure de la journée,
#   en ne requêtant QUE les pas listés dans `_scales` (quoti/horaire/6m) du fichier stations combiné.
#   - Aligne strictement les fenêtres 6 minutes (00:00:00Z → 23:54:00Z, secs=00, minutes multiple de 6).
#   - Pré-filtre via /information-station pour éviter les commandes sur périodes inactives (supprime 404).
#   - Log détaillé sur stderr (verbose) : auth/ratelimit/infos/colonnes.
#   - stdout = CSV minimal: id,nom,pas,last_datetime_utc,status[,columns]
#     (la colonne `columns` JSON compact est ajoutée seulement si --emit-cols est passé).
#
# Usage typique (GitHub Actions) :
#   DATE=$(date -u +%F)
#   python fetch_observations.py --date="$DATE" \
#     --stations data/metadonnees/stations.json \
#   | aws s3 cp - "s3://bucket/mf/last_records_${DATE}.csv"
#
# Dépendances: requests, python-dateutil
# Auth: src/api/token_provider.py -> get_api_key(use_cache=True), clear_token_cache()

import os
import io
import sys
import csv
import json
import time
import argparse
import datetime as dt
from collections import deque
from functools import lru_cache
from typing import Dict, Any, Tuple, List

import requests
from dateutil import tz, parser as dtparser

# --- Auth MF OAuth2 (portail) --------------------------------------------
# NOTE: on s'appuie sur ton provider existant (cache local + refresh).
from src.api.token_provider import get_api_key, clear_token_cache  # type: ignore

# --- Configuration --------------------------------------------------------
DEFAULT_STATIONS = "data/metadonnees/stations.json"  # fichier combiné avec _scales
BASE_URL = os.getenv("METEO_BASE_URL", "https://public-api.meteofrance.fr/public/DPClim/v1")

# Priorité globale si plusieurs pas existent pour une station
PASSES = ["quotidienne", "horaire", "infrahoraire-6m"]

# Limiteur de débit: 50 req/min par défaut
MAX_RPM   = int(os.getenv("METEO_MAX_RPM", "50"))
RATE_SECS = 60.0

# STRICT_SCALES=true -> si _scales est vide/absent on NE tente rien
STRICT_SCALES = os.getenv("DPCLIM_STRICT_SCALES", "true").lower() in ("1", "true", "yes")

# Heuristiques de mapping "nom paramètre" -> pas (utilisées dans /information-station)
_PAS_KEYWORDS = {
    "quotidienne": ["quotidien"],
    "horaire":     ["horaire"],
    "infrahoraire-6m": ["6 mn", "6min", "6 min", "6 minutes"],
}

# Colonnes à conserver
COL_KEEP = {
    "quotidienne": ["HNEIGEF","NEIGETOTX","NEIGETOT06"],
    "horaire":     ["HNEIGEF","NEIGETOT"],
    "infrahoraire-6m": [],
}

# --- Rate limiter fenêtre glissante --------------------------------------
class RateLimiter:
    """Limiteur simple à fenêtre glissante (max_calls par period_sec)."""
    def __init__(self, max_calls: int, period_sec: float):
        self.max_calls = max_calls
        self.period = period_sec
        self.calls = deque()

    def wait(self) -> None:
        now = time.time()
        while self.calls and (now - self.calls[0]) > self.period:
            self.calls.popleft()
        if len(self.calls) >= self.max_calls:
            sleep_for = self.period - (now - self.calls[0]) + 0.01
            if sleep_for > 0:
                time.sleep(sleep_for)
        self.calls.append(time.time())

_rl = RateLimiter(MAX_RPM, RATE_SECS)

def _headers_json() -> Dict[str, str]:
    token = get_api_key(use_cache=True)
    return {"accept": "application/json", "authorization": f"Bearer {token}"}

def _req(method: str, url: str, *, params=None, timeout=60):
    """
    Requête avec 1 retry en cas de 401/403 (refresh token), gestion 429 Retry-After.
    Ne lève pas; c'est l'appelant qui décide. Logs sur stderr pour transparence.
    """
    for attempt in (1, 2):
        _rl.wait()
        resp = requests.request(method, url, headers=_headers_json(), params=params, timeout=timeout)
        if resp.status_code in (401, 403) and attempt == 1:
            sys.stderr.write(f"[auth] 401/403 -> refresh token\n")
            clear_token_cache()
            continue
        if resp.status_code == 429 and attempt == 1:
            ra = resp.headers.get("Retry-After")
            try:
                wait = float(ra) if ra is not None else 60.0
            except ValueError:
                wait = 60.0
            sys.stderr.write(f"[rate] 429 -> sleep {wait}s\n")
            time.sleep(wait)
            continue
        return resp
    return resp  # dernier essai

# --- Fenêtres et parsing temps ------------------------------------------
def _floor_to_6min(d: dt.datetime) -> dt.datetime:
    """Aligne vers le bas sur un multiple de 6 minutes; secondes/microsecondes = 0."""
    m = (d.minute // 6) * 6
    return d.replace(minute=m, second=0, microsecond=0)

def _day_window_utc(day_str: str, pas: str) -> Tuple[str, str]:
    """
    Construit la fenêtre UTC pour la date donnée.
    - quotidien/horaire : [00:00:00Z ; 23:59:59Z] (l'API ignore min/sec pour ces pas)
    - 6m : [00:00:00Z ; 23:54:00Z] (bornes multiples de 6, secs=00)
      Si la date est aujourd'hui UTC, fin = min(23:54:00Z, floor_6min(now_utc)).
    """
    d = dt.datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=tz.UTC)

    if pas != "infrahoraire-6m":
        start = d.replace(hour=0, minute=0, second=0, microsecond=0)
        end   = d.replace(hour=23, minute=59, second=59, microsecond=0)
        return start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ")

    day_start = d.replace(hour=0, minute=0, second=0, microsecond=0)   # 00:00:00Z
    day_end_6 = d.replace(hour=23, minute=54, second=0, microsecond=0) # 23:54:00Z (dernier multiple de 6)

    now_utc = dt.datetime.now(tz.UTC)
    if d.date() == now_utc.date():
        now_floor = _floor_to_6min(now_utc)
        end_6 = min(day_end_6, now_floor)
    else:
        end_6 = day_end_6

    return day_start.strftime("%Y-%m-%dT%H:%M:%SZ"), end_6.strftime("%Y-%m-%dT%H:%M:%SZ")

def _parse_dt_naive_utc(s: str):
    """Parse 'YYYY-mm-dd HH:MM:SS' (catalogue MF) -> datetime UTC, ou None si vide/erreur."""
    if not s:
        return None
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz.UTC)
    except Exception:
        return None

def _day_bounds_utc(day_str: str) -> Tuple[dt.datetime, dt.datetime]:
    d = dt.datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=tz.UTC)
    return d.replace(hour=0, minute=0, second=0, microsecond=0), d.replace(hour=23, minute=59, second=59, microsecond=0)

# --- API DPClim: information-station (cache) -----------------------------
@lru_cache(maxsize=4096)
def _info_station_cached(station_id: int) -> dict:
    """Récupère info-station + log verbose."""
    url = f"{BASE_URL}/information-station"
    resp = _req("GET", url, params={"id-station": station_id}, timeout=30)
    if resp.status_code >= 400:
        sys.stderr.write(f"[info] id={station_id} HTTP{resp.status_code} {resp.text[:160]}\n")
        return {}
    try:
        js = resp.json()
        info = js[0] if isinstance(js, list) and js else js
        sys.stderr.write(f"[info] id={station_id} ok\n")
        return info
    except Exception as ex:
        sys.stderr.write(f"[info] id={station_id} parse_error {repr(ex)}\n")
        return {}

def _pas_active_this_day(info: dict, pas: str, day_str: str) -> bool:
    """
    True si au moins un capteur du 'pas' est actif ce jour UTC (intersection d'intervalles).
    Évite les commandes sur périodes inactives (réduit les 404 backend).
    """
    params = info.get("parametres") or []
    if not isinstance(params, list):
        return False
    start_day, end_day = _day_bounds_utc(day_str)
    keys = _PAS_KEYWORDS[pas]
    for p in params:
        nom = str(p.get("nom", "")).lower()
        if not any(k in nom for k in keys):
            continue
        d0 = _parse_dt_naive_utc(p.get("dateDebut", ""))
        d1 = _parse_dt_naive_utc(p.get("dateFin", ""))  # vide = ouvert
        if d0 is None:
            continue
        if d1 is None:
            d1 = dt.datetime.max.replace(tzinfo=tz.UTC)
        # intersection [start_day,end_day] ∩ [d0,d1] ?
        if not (end_day < d0 or start_day > d1):
            return True
    return False

# --- API DPClim: commande + téléchargement -------------------------------
def commande_station(station_id: int, pas: str, start_utc: str, end_utc: str) -> Tuple[bool, str]:
    """
    Lance la commande pour un pas. GET /commande-station/{pas} avec query params.
    Retourne (ok, id_commande).
    """
    url = f"{BASE_URL}/commande-station/{pas}"
    params = {"id-station": int(station_id), "date-deb-periode": start_utc, "date-fin-periode": end_utc}
    resp = _req("GET", url, params=params, timeout=60)
    if resp.status_code not in (200, 201, 202):
        sys.stderr.write(f"[cmd]  id={station_id} pas={pas} HTTP{resp.status_code} {resp.text[:200]}\n")
        return False, ""
    try:
        cmd_id = resp.json()["elaboreProduitAvecDemandeResponse"]["return"]
        sys.stderr.write(f"[cmd]  id={station_id} pas={pas} -> cmde={cmd_id}\n")
        return True, cmd_id
    except Exception as ex:
        sys.stderr.write(f"[cmd]  id={station_id} pas={pas} parse_error {repr(ex)} {resp.text[:200]}\n")
        return False, ""

def telecharger_commande(commande_id: str, max_wait_s=90, step_s=5) -> Tuple[int, bytes]:
    """
    Télécharge le résultat d'une commande.
    GET /commande/fichier?id-cmde=... ; 204 si pas prêt, 200/201 si prêt.
    """
    url = f"{BASE_URL}/commande/fichier"
    waited = 0
    while waited <= max_wait_s:
        resp = _req("GET", url, params={"id-cmde": commande_id}, timeout=60)  # param correct
        if resp.status_code in (200, 201):
            sys.stderr.write(f"[dl ] cmde={commande_id} ready HTTP{resp.status_code}\n")
            return resp.status_code, resp.content
        if resp.status_code == 204:
            time.sleep(step_s)
            waited += step_s
            sys.stderr.write(f"[dl ] cmde={commande_id} 204 wait {waited}/{max_wait_s}s\n")
            continue
        sys.stderr.write(f"[dl ] cmde={commande_id} HTTP{resp.status_code} {resp.text[:200]}\n")
        return resp.status_code, b""
    return 408, b""

# --- Parsing CSV: dernière mesure temporelle + colonnes ------------------
def parse_latest_row(csv_bytes: bytes, *, debug=False):
    """
    Cherche colonne DATE|datetime|time|heure.
    Retourne (dt_utc, row, colonnes) ou (None, None, colonnes).
    """
    if not csv_bytes:
        return None, None, []
    text = csv_bytes.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    cols = reader.fieldnames or []
    if debug:
        sys.stderr.write(f"[cols] {cols}\n")

    best_dt = None
    best_row = None

    for row in reader:
        for k, v in list(row.items()):
            if isinstance(v, str):
                row[k] = v.strip()

        keys = {k.lower(): k for k in row.keys()}
        dkey = next((keys[k] for k in ("date", "datetime", "time", "heure") if k in keys), None)
        if not dkey:
            continue
        try:
            cur = dtparser.parse(row[dkey])
            cur = cur.replace(tzinfo=tz.UTC) if not cur.tzinfo else cur.astimezone(tz.UTC)
        except Exception:
            continue
        if (best_dt is None) or (cur > best_dt):
            best_dt = cur
            best_row = row

    return best_dt, best_row, cols

# --- Sélecteur de pas à partir de _scales --------------------------------
def _scales_for_station(st: dict) -> List[str]:
    """
    Retourne les pas à interroger pour cette station selon _scales + priorité globale.
    Si STRICT_SCALES est true et _scales est vide/absent -> liste vide.
    """
    avail = st.get("_scales") or []
    if not isinstance(avail, list):
        avail = []
    ordered = [p for p in PASSES if p in avail]
    if ordered:
        return ordered
    return ordered if STRICT_SCALES else PASSES

# --- CLI principal -------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Fetch dernier enregistrement MF DPClim pour une date UTC")
    ap.add_argument("--date", required=True, help="Date UTC au format YYYY-MM-DD")
    ap.add_argument("--stations", default=DEFAULT_STATIONS, help="Chemin du JSON stations combiné (_scales)")
    ap.add_argument("--emit-cols", action="store_true",
                    help="Ajoute une colonne 'columns' (JSON par pas) à la sortie CSV")
    args = ap.parse_args()

    # Valide la date
    try:
        dt.datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        sys.stderr.write("[arg] --date doit être YYYY-MM-DD\n")
        sys.exit(2)

    # Charge les stations
    try:
        stations = json.load(open(args.stations, "r", encoding="utf-8"))
    except Exception as ex:
        sys.stderr.write(f"[io ] lecture stations échouée: {repr(ex)}\n")
        sys.exit(2)

    # Header CSV
    w = csv.writer(sys.stdout, lineterminator="\n")
    header = ["id", "nom", "pas", "last_datetime_utc", "status"]
    if args.emit_cols:
        header.append("columns")  # JSON: {"quotidienne":[...], "horaire":[...], "infrahoraire-6m":[...]}
    w.writerow(header)

    # Parcours stations
    for st in stations:
        sid_raw = st.get("id", "")
        try:
            sid = int(str(sid_raw).strip())
        except Exception:
            sys.stderr.write(f"[skip] id invalide: {sid_raw}\n")
            row = [sid_raw, st.get("nom", ""), "", "", "invalid_id"]
            if args.emit_cols:
                row.append("{}")
            w.writerow(row)
            continue

        nom = st.get("nom", "")
        scales = _scales_for_station(st)
        if not scales:
            sys.stderr.write(f"[skip] id={sid} no _scales (STRICT)\n")
            row = [sid, nom, "", "", "no_scale"]
            if args.emit_cols:
                row.append("{}")
            w.writerow(row)
            continue

        # Préfiltre activité capteurs pour la date (réduit fortement les 404 backend)
        info = _info_station_cached(sid)
        if not info:
            sys.stderr.write(f"[warn] id={sid} information-station indisponible\n")

        status = "no_data"
        best_pas = ""
        best_dt = None
        seen_cols: Dict[str, List[str]] = {}

        for pas in scales:
            if info and not _pas_active_this_day(info, pas, args.date):
                sys.stderr.write(f"[skip] id={sid} pas={pas} inactif le {args.date}\n")
                continue

            s, e = _day_window_utc(args.date, pas)
            ok, cmd_id = commande_station(sid, pas, s, e)
            if not ok:
                continue

            sc, content = telecharger_commande(cmd_id, max_wait_s=90, step_s=5)
            if sc not in (200, 201):
                continue

            # récupère colonnes + log
            last_dt, _, cols = parse_latest_row(content, debug=False)
            sys.stderr.write(f"[cols] id={sid} pas={pas} -> {','.join(cols or [])}\n")

            if pas in COL_KEEP and cols:
                # garde seulement les colonnes neige pertinentes par pas
                want = {x.upper() for x in COL_KEEP[pas]}
                cols = [c for c in cols if c and c.upper() in want]

            # sélection du meilleur timestamp du jour
            if last_dt and last_dt.strftime("%Y-%m-%d") == args.date:
                if (best_dt is None) or (last_dt > best_dt):
                    best_dt, best_pas, status = last_dt, pas, "ok"

        row = [
            sid,
            nom,
            best_pas,
            best_dt.strftime("%Y-%m-%dT%H:%M:%SZ") if best_dt else "",
            status
        ]
        if args.emit_cols:
            row.append(json.dumps(seen_cols, ensure_ascii=False, separators=(",", ":")))
        w.writerow(row)

if __name__ == "__main__":
    main()
