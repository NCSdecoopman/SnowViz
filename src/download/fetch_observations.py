#!/usr/bin/env python3
# fetch_observations.py
# But:
#   - Log fichier horodaté AAAAMMJJHHMMSS (UTC) dans ./logs/observations
#   - Logs au format: "[id] : Etat de la connection | Données récupérées ou non | Si non la raison"
#   - stdout CSV: id,date,<colonnes sélectionnées> puis valeurs de la dernière mesure du jour
#
# Points clés de cette version:
#   - Parsing des dates "information-station" robuste (ISO-8601, avec/sans TZ).
#   - _req() tolérant aux erreurs réseau + retry 401/403/429.
#   - telecharger_commande() ne renvoie plus HTTP0 immédiatement: attend et réessaie.
#   - Logs plus informatifs (pas, fenêtre, id commande).
#   - Respect du rate limiting.
#
# Dépendances: requests, python-dateutil

import os
import io
import sys
import csv
import json
import time
import argparse
import datetime as dt
from datetime import timezone
from collections import deque
from functools import lru_cache
from typing import Dict, Tuple, List, Optional

import requests
from dateutil import tz, parser as dtparser

# --- Auth MF OAuth2 (portail) --------------------------------------------
from src.api.token_provider import get_api_key, clear_token_cache  # type: ignore
# --- Registre des données manquantes -------------------------------------
from src.utils.missing_registry import append_missing  # type: ignore


# --- Configuration --------------------------------------------------------
DEFAULT_STATIONS = "data/metadonnees/stations.json"
BASE_URL = os.getenv(
    "METEO_BASE_URL",
    "https://public-api.meteofrance.fr/public/DPClim/v1",
)

PASSES = ["quotidienne", "horaire", "infrahoraire-6m"]

# Limiteur de débit (RPM = requêtes par minute)
MAX_RPM = int(os.getenv("METEO_MAX_RPM", "50"))
RATE_SECS = 60.0

STRICT_SCALES = os.getenv("DPCLIM_STRICT_SCALES", "true").lower() in ("1", "true", "yes")

# Heuristiques de mapping
_PAS_KEYWORDS = {
    "quotidienne": ["quotidienne"],
    "horaire": ["horaire"],
    "infrahoraire-6m": ["6 mn", "6min", "6 min", "6 minutes"],
}

# Colonnes à conserver par pas
COL_KEEP = {
    "quotidienne": ["HNEIGEF", "NEIGETOT", "NEIGETOT06"],
    "horaire": ["HNEIGEF", "NEIGETOT"],
    "infrahoraire-6m": [],
}

# Union dédupliquée des colonnes à exporter
def _build_union_cols() -> List[str]:
    seen = set()
    out: List[str] = []
    for pas in PASSES:
        for c in COL_KEEP.get(pas, []):
            if c not in seen:
                seen.add(c)
                out.append(c)
    return out


UNION_COLS = _build_union_cols()  # ex: ["HNEIGEF","NEIGETOT","NEIGETOT06"]

# --- Rate limiter ---------------------------------------------------------
class RateLimiter:
    """Glisse une fenêtre de RATE_SECS secondes et borne à MAX_RPM appels."""

    def __init__(self, max_calls: int, period_sec: float):
        self.max_calls = max_calls
        self.period = period_sec
        self.calls = deque()

    def wait(self) -> None:
        now = time.time()
        # évacue les appels en dehors de la fenêtre
        while self.calls and (now - self.calls[0]) > self.period:
            self.calls.popleft()
        # si plein: dort juste ce qu'il faut
        if len(self.calls) >= self.max_calls:
            sleep_for = self.period - (now - self.calls[0]) + 0.01
            if sleep_for > 0:
                time.sleep(sleep_for)
        self.calls.append(time.time())


_rl = RateLimiter(MAX_RPM, RATE_SECS)

# --- Logging fichier ------------------------------------------------------
def _init_log_file(logdir: str) -> str:
    # UTC pour éviter l'ambiguïté; format AAAAMMJJHHMMSS
    ts = dt.datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    os.makedirs(logdir, exist_ok=True)
    return os.path.join(logdir, f"{ts}.log")


_LOG_PATH: Optional[str] = None


def _log_line(station_id: int, etat: str, ok_data: bool, reason: str = "") -> None:
    """
    Ecrit une ligne de log normalisée:
    [id] : Etat de la connection | Données récupérées ou non | Si non la raison
    """
    global _LOG_PATH
    if not _LOG_PATH:
        return
    data_str = "Oui" if ok_data else "Non"
    reason_str = reason.strip() if (reason and not ok_data) else ""
    line = f"[{station_id}] : {etat} | Données récupérées : {data_str} | {reason_str}".rstrip()
    with open(_LOG_PATH, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


# --- HTTP helpers ---------------------------------------------------------
def _headers_json() -> Dict[str, str]:
    token = get_api_key(use_cache=True)
    return {"accept": "application/json", "authorization": f"Bearer {token}"}


def _req(method: str, url: str, *, params=None, timeout=60):
    """
    Requête robuste:
      - respect du rate limit local
      - 2 tentatives
      - refresh token sur 401/403
      - attente sur 429 Retry-After
      - tolère les exceptions réseau transitoires
    Retourne:
      - requests.Response ou None si échec total.
    """
    last_resp = None
    for attempt in (1, 2):
        try:
            _rl.wait()
            resp = requests.request(
                method, url, headers=_headers_json(), params=params, timeout=timeout
            )
            last_resp = resp
        except requests.RequestException:
            if attempt == 1:
                time.sleep(2)
                continue
            return None

        if resp.status_code in (401, 403) and attempt == 1:
            clear_token_cache()
            time.sleep(0.5)
            continue

        if resp.status_code == 429 and attempt == 1:
            ra = resp.headers.get("Retry-After")
            try:
                wait = float(ra) if ra is not None else 60.0
            except ValueError:
                wait = 60.0
            time.sleep(wait)
            continue

        return resp
    return last_resp


# --- Fenêtres temps -------------------------------------------------------
def _floor_to_6min(d: dt.datetime) -> dt.datetime:
    m = (d.minute // 6) * 6
    return d.replace(minute=m, second=0, microsecond=0)


def _day_window_utc(day_str: str, pas: str) -> Tuple[str, str]:
    """Fenêtre UTC pour la journée cible. 6 min: borne sup à 23:54:00Z, bornée à 'now' si jour courant."""
    d = dt.datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=tz.UTC)
    if pas != "infrahoraire-6m":
        start = d.replace(hour=0, minute=0, second=0, microsecond=0)
        end = d.replace(hour=23, minute=59, second=59, microsecond=0)
        return start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ")

    day_start = d.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_6 = d.replace(hour=23, minute=54, second=0, microsecond=0)
    now_utc = dt.datetime.now(tz.UTC)
    if d.date() == now_utc.date():
        now_floor = _floor_to_6min(now_utc)
        end_6 = min(day_end_6, now_floor)
    else:
        end_6 = day_end_6
    return day_start.strftime("%Y-%m-%dT%H:%M:%SZ"), end_6.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_any_to_utc(s: str) -> Optional[dt.datetime]:
    """Accepte ISO-8601 ou 'YYYY-MM-DD HH:MM:SS'. Retourne un datetime UTC."""
    if not s:
        return None
    try:
        d = dtparser.parse(s)
        return d.astimezone(tz.UTC) if d.tzinfo else d.replace(tzinfo=tz.UTC)
    except Exception:
        return None


def _day_bounds_utc(day_str: str) -> Tuple[dt.datetime, dt.datetime]:
    d = dt.datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=tz.UTC)
    return (
        d.replace(hour=0, minute=0, second=0, microsecond=0),
        d.replace(hour=23, minute=59, second=59, microsecond=0),
    )


# --- API info-station -----------------------------------------------------
@lru_cache(maxsize=4096)
def _info_station_cached(station_id: int) -> dict:
    url = f"{BASE_URL}/information-station"
    resp = _req("GET", url, params={"id-station": station_id}, timeout=30)
    if not resp or resp.status_code >= 400:
        return {}
    try:
        js = resp.json()
        return js[0] if isinstance(js, list) and js else js
    except Exception:
        return {}


def _pas_active_this_day(info: dict, pas: str, day_str: str) -> bool:
    """Vérifie si un paramètre correspondant au 'pas' est actif le jour 'day_str'."""
    params = info.get("parametres") or []
    if not isinstance(params, list):
        return False
    start_day, end_day = _day_bounds_utc(day_str)
    keys = _PAS_KEYWORDS[pas]
    matched_any = False
    for p in params:
        nom_raw = str(p.get("nom", ""))
        nom = nom_raw.lower()
        if not any(k in nom for k in keys):
            continue
        matched_any = True
        d0 = _parse_any_to_utc(p.get("dateDebut", ""))
        d1 = _parse_any_to_utc(p.get("dateFin", "")) or dt.datetime.max.replace(tzinfo=tz.UTC)
        if d0 is None:
            continue
        if not (end_day < d0 or start_day > d1):
            return True
    if matched_any:
        # station id n’est pas dans info → passer via log appelant
        pass
    return False



# --- API commande + téléchargement ---------------------------------------
def commande_station(station_id: int, pas: str, start_utc: str, end_utc: str) -> Tuple[bool, str, str]:
    """
    Retourne (ok, id_commande, etat_connection) où etat_connection est un court libellé.
    """
    url = f"{BASE_URL}/commande-station/{pas}"
    params = {"id-station": int(station_id), "date-deb-periode": start_utc, "date-fin-periode": end_utc}
    resp = _req("GET", url, params=params, timeout=60)
    if not resp:
        return False, "", "HTTP? (pas de réponse)"
    if resp.status_code not in (200, 201, 202):
        return False, "", f"HTTP{resp.status_code}"
    try:
        cmd_id = resp.json()["elaboreProduitAvecDemandeResponse"]["return"]
        return True, cmd_id, f"HTTP{resp.status_code}"
    except Exception:
        return False, "", f"HTTP{resp.status_code} parse_error"


def telecharger_commande(commande_id: str, max_wait_s=300, step_s=5) -> Tuple[int, bytes]:
    """
    Polling de /commande/fichier.
    - 204 => attendre (Retry-After si fourni).
    - None => échec transitoire: attendre et réessayer.
    - 200/201 => OK avec contenu.
    - 4xx/5xx => échec définitif pour cette commande.
    """
    url = f"{BASE_URL}/commande/fichier"
    waited = 0.0
    while waited <= max_wait_s:
        resp = _req("GET", url, params={"id-cmde": commande_id}, timeout=60)
        if resp and resp.status_code in (200, 201):
            return resp.status_code, resp.content
        if resp and resp.status_code == 204:
            ra = resp.headers.get("Retry-After")
            try:
                step = float(ra) if ra is not None else float(step_s)
            except ValueError:
                step = float(step_s)
            time.sleep(step)
            waited += step
            continue
        if resp is None:
            time.sleep(step_s)
            waited += step_s
            continue
        # autre code HTTP (4xx/5xx) -> stop propre
        return resp.status_code, b""
    return 408, b""


# --- Parsing CSV ----------------------------------------------------------
def parse_latest_row(csv_bytes: bytes):
    """
    Cherche colonne temporelle (date/datetime/time/heure) et prend la dernière.
    Retourne (dt_utc, row_dict, colonnes_header)
    """
    if not csv_bytes:
        return None, None, []
    text = csv_bytes.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    cols = reader.fieldnames or []
    best_dt = None
    best_row = None
    for row in reader:
        # trim
        for k, v in list(row.items()):
            if isinstance(v, str):
                row[k] = v.strip()
        keys = {k.lower(): k for k in row.keys()}
        dkey = next((keys[k] for k in ("date", "datetime", "time", "heure") if k in keys), None)
        if not dkey:
            continue
        try:
            cur = dtparser.parse(row[dkey])
            cur = cur.astimezone(tz.UTC) if cur.tzinfo else cur.replace(tzinfo=tz.UTC)
        except Exception:
            continue
        if (best_dt is None) or (cur > best_dt):
            best_dt = cur
            best_row = row
    return best_dt, best_row, cols


# --- Sélection pas --------------------------------------------------------
def _scales_for_station(st: dict) -> List[str]:
    """Retourne les pas autorisés pour la station, en respectant l'ordre global PASSES."""
    avail = st.get("_scales") or []
    if not isinstance(avail, list):
        avail = []
    ordered = [p for p in PASSES if p in avail]
    return ordered if ordered or STRICT_SCALES else PASSES


# --- Utilitaires valeurs --------------------------------------------------
def _pick_values_case_insensitive(row: dict, wanted: List[str]) -> Dict[str, str]:
    """Retourne un dict {COL: valeur_str} respectant la casse de wanted, recherche insensible."""
    lowmap = {k.lower(): k for k in row.keys()}
    out = {}
    for col in wanted:
        key = lowmap.get(col.lower())
        out[col] = row.get(key, "") if key else ""
    return out


# --- CLI ------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Fetch dernier enregistrement MF DPClim pour une date UTC")
    ap.add_argument("--date", required=True, help="Date UTC au format YYYY-MM-DD")
    ap.add_argument("--stations", default=DEFAULT_STATIONS, help="Chemin du JSON stations combiné (_scales)")
    ap.add_argument("--id", type=int, help="Force une seule station ID précise")
    ap.add_argument("--logdir", default="logs/observations", help="Répertoire des logs horodatés")
    args = ap.parse_args()

    # Init fichier log
    global _LOG_PATH
    _LOG_PATH = _init_log_file(args.logdir)

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

    # Filtrage par id si demandé
    if args.id:
        try:
            target_id = int(args.id)
        except ValueError:
            sys.stderr.write("[arg] --id doit être un entier\n")
            sys.exit(2)
        stations = [st for st in stations if str(st.get("id","")).strip() == str(target_id)]
        if not stations:
            sys.stderr.write(f"[id] {target_id} introuvable dans stations.json\n")
            sys.exit(1)

    # Prépare CSV stdout: id,date,<UNION_COLS>
    writer = csv.writer(sys.stdout, lineterminator="\n")
    header = ["id", "date"] + UNION_COLS
    writer.writerow(header)

    # Boucle stations
    for st in stations:
        sid_raw = st.get("id", "")
        try:
            sid = int(str(sid_raw).strip())
        except Exception:
            _log_line(station_id=sid_raw, etat="invalid_id", ok_data=False, reason="Identifiant station invalide")
            writer.writerow([sid_raw, ""] + [""] * len(UNION_COLS))
            continue

        info = _info_station_cached(sid)
        scales = _scales_for_station(st)
        if not scales:
            _log_line(sid, "no_scale", False, "Aucun pas actif pour cette station (STRICT)")
            writer.writerow([sid, ""] + [""] * len(UNION_COLS))
            continue

        best_dt = None
        best_row = None
        best_pas = None

        # Tente chaque pas autorisé
        for pas in scales:
            # Vérifie activité pour le jour cible
            if info and not _pas_active_this_day(info, pas, args.date):
                _log_line(sid, f"{pas}:inactif", False, f"inactif le {args.date}")
                continue

            # Fenêtre temporelle demandée
            s_utc, e_utc = _day_window_utc(args.date, pas)

            # Création de commande
            ok, cmd_id, etat = commande_station(sid, pas, s_utc, e_utc)
            if not ok:
                _log_line(sid, f"{etat}", False, f"commande pas={pas} {s_utc}→{e_utc} échouée")
                continue

            # Téléchargement avec polling
            sc, content = telecharger_commande(cmd_id, max_wait_s=300, step_s=5)
            if sc not in (200, 201):
                _log_line(sid, f"HTTP{sc}", False, f"pas={pas} cmd={cmd_id} fichier non prêt/erreur")
                continue

            # Parse et sélection de la dernière ligne datée
            last_dt, row, _cols = parse_latest_row(content)
            if not last_dt or not row:
                _log_line(sid, "OK", False, f"pas={pas} cmd={cmd_id} aucune ligne datée valide")
                continue

            if last_dt.strftime("%Y-%m-%d") != args.date:
                _log_line(sid, "OK", False, f"pas={pas} cmd={cmd_id} dernière mesure hors jour cible")
                continue

            # Mieux que l'actuel ?
            if (best_dt is None) or (last_dt > best_dt):
                best_dt, best_row, best_pas = last_dt, row, pas
                _log_line(sid, "OK", True, f"pas={pas} cmd={cmd_id}")

        # Harmonisation NEIGETOTX -> NEIGETOT pour la quotidienne
        if best_pas == "quotidienne" and best_row:
            if "NEIGETOT" not in best_row and "NEIGETOTX" in best_row:
                best_row["NEIGETOT"] = best_row.pop("NEIGETOTX")
            elif "NEIGETOTX" in best_row:
                # si les deux existent on supprime l'alias
                del best_row["NEIGETOTX"]

        # Sortie CSV
        if best_dt and best_row:
            vals_map = _pick_values_case_insensitive(best_row, UNION_COLS)

            # check si aucune valeur neige n’est renseignée
            has_data = any(v not in ("", None) for v in vals_map.values())
            if not has_data:
                append_missing(sid, args.date)
                continue

            row_out = [sid, best_dt.strftime("%Y-%m-%dT%H:%M:%SZ")] + [vals_map[c] for c in UNION_COLS]
            writer.writerow(row_out)
        else:
            # aucune mesure du tout
            append_missing(sid, args.date)
            continue


if __name__ == "__main__":
    main()
