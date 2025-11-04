#!/usr/bin/env python3
# Relance ciblée des observations manquantes.
# - Lit un JSON de la forme [{"id": 38002401, "date": "YYYY-MM-DD"}, ...]
# - Pour chaque (id, date), appelle fetch_observations.py avec --id et --date
# - Agrège toutes les lignes CSV sur stdout avec un seul header
# - Si au moins une ligne utile est retournée (date non vide), retire l'entrée du JSON
# - Ecriture atomique du JSON nettoyé
#
# Dépendances: Python 3.9+, le module/cli fetch_observations.py existant

import os
import sys
import json
import csv
import argparse
import tempfile
import subprocess
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional

DEFAULT_MISSING = Path(os.getenv("MISSING_OBS_JSON", "data/metadonnees/missing_observations.json"))
DEFAULT_STATIONS = Path(os.getenv("STATIONS_JSON", "data/metadonnees/stations.json"))
DEFAULT_LOGDIR = Path(os.getenv("OBS_LOGDIR", "logs/observations"))

# --- I/O helpers -----------------------------------------------------------

def _read_missing(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return []
        # déduplication sur (id,date)
        seen = set()
        out = []
        for e in data:
            try:
                k = (int(e.get("id")), str(e.get("date")))
            except Exception:
                continue
            if not k[1]:
                continue
            if k in seen:
                continue
            seen.add(k)
            out.append({"id": k[0], "date": k[1], **({} if "reason" not in e else {"reason": e["reason"]})})
        return out
    except Exception:
        return []

def _atomic_write_json(path: Path, data: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=str(path.parent)) as tmp:
        json.dump(data, tmp, ensure_ascii=False, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_name = tmp.name
    os.replace(tmp_name, path)

# --- Core ------------------------------------------------------------------

def _run_fetch_observations(date_str: str, station_id: int, stations_path: Path, logdir: Path) -> Tuple[bool, List[List[str]]]:
    """
    Lance `python -m src.download.fetch_observations --date ... --id ...`
    Retourne (success, rows) où rows inclut header+rows CSV renvoyés par le sous-process.
    success=True si au moins une ligne data avec colonne date non vide est présente.
    """
    # Important: on invoque via -m pour utiliser l'import package.
    cmd = [
        sys.executable,
        "-m", "src.download.fetch_observations",
        "--date", date_str,
        "--id", str(station_id),
        "--stations", str(stations_path),
        "--logdir", str(logdir),
    ]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except Exception as ex:
        sys.stderr.write(f"[spawn] échec lancement fetch_observations: {repr(ex)}\n")
        return False, []

    if proc.returncode not in (0,):
        # Même en cas d'erreur, tenter de lire ce qui a été produit sur stdout
        if proc.stderr:
            sys.stderr.write(proc.stderr)
    out = proc.stdout.strip().splitlines()
    if not out:
        return False, []

    # Parse CSV en mémoire
    reader = csv.reader(out)
    rows = [row for row in reader]
    if not rows:
        return False, []

    # rows[0] = header attendu: id,date,...
    # Si une ligne a une date non vide en col 1, on considère success
    success = any((len(r) >= 2 and r[1].strip() != "" ) for r in rows[1:])
    return success, rows

def fetch_all(missing_path: Path, stations_path: Path, logdir: Path, dry_run: bool=False) -> int:
    items = _read_missing(missing_path)
    if not items:
        # Rien à faire, mais on émet quand même un header CSV standard minimal
        # Laisser fetch_observations gérer le header? Ici on sort rien.
        return 0

    # Un seul header sur stdout
    header_written = False
    writer = csv.writer(sys.stdout, lineterminator="\n")
    remaining: List[Dict[str, Any]] = []

    for e in items:
        try:
            sid = int(e["id"])
            date_str = str(e["date"])
        except Exception:
            # entrée corrompue -> on la supprime silencieusement
            continue

        ok, rows = _run_fetch_observations(date_str, sid, stations_path, logdir)

        if rows:
            # Écrit le header une seule fois
            if not header_written:
                writer.writerow(rows[0])
                header_written = True
            # Écrit toutes les data rows (sans réécrire le header)
            for r in rows[1:]:
                writer.writerow(r)

        if not ok:
            # On conserve l'entrée si échec
            remaining.append(e)

    # Écrit le JSON mis à jour si pas dry-run
    if not dry_run:
        _atomic_write_json(missing_path, remaining)

    # Retourne code: 0 si tout résolu, 1 si il reste des manquants
    return 0 if len(remaining) == 0 else 1

# --- CLI -------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Récupère les observations listées dans missing_observations.json")
    ap.add_argument("--missing", type=Path, default=DEFAULT_MISSING, help="Chemin du JSON des observations manquantes")
    ap.add_argument("--stations", type=Path, default=DEFAULT_STATIONS, help="Chemin du stations.json combiné")
    ap.add_argument("--logdir", type=Path, default=DEFAULT_LOGDIR, help="Répertoire des logs pour fetch_observations")
    ap.add_argument("--dry-run", action="store_true", help="N'écrit pas le JSON, affiche seulement le CSV agrégé")
    args = ap.parse_args()

    rc = fetch_all(args.missing, args.stations, args.logdir, dry_run=args.dry_run)
    sys.exit(rc)

if __name__ == "__main__":
    main()