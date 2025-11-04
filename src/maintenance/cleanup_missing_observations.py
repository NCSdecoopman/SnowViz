#!/usr/bin/env python3
# Supprime les entrées (id, date[, reason]) de missing_observations.json plus vieilles que N jours.
# - Par défaut N=11.
# - Dates attendues au format 'YYYY-MM-DD' (UTC ou local, peu importe, c'est une date civile).
# - Écriture atomique pour éviter toute corruption en cas d'arrêt brutal.
# - Options CLI: --days, --path, --dry-run.

import os
import json
import argparse
import tempfile
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Tuple

DEFAULT_PATH = Path(os.getenv("MISSING_OBS_JSON", "data/metadonnees/missing_observations.json"))

# --- utils E/S ------------------------------------------------------------

def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

def _read_list(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        # JSON invalide: on repart d'une liste vide pour ne pas bloquer le cron.
        return []

def _atomic_write(path: Path, data: List[Dict[str, Any]]) -> None:
    _ensure_parent(path)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=str(path.parent)) as tmp:
        json.dump(data, tmp, ensure_ascii=False, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_name = tmp.name
    os.replace(tmp_name, path)

# --- logique métier -------------------------------------------------------

def _parse_date_ymd(s: Any) -> Tuple[bool, Any]:
    """Retourne (ok, date). ok=False si parsing échoue."""
    if not isinstance(s, str):
        return False, None
    try:
        d = datetime.strptime(s, "%Y-%m-%d").date()
        return True, d
    except Exception:
        return False, None

def cleanup(path: Path, keep_days: int, dry_run: bool = False) -> Dict[str, Any]:
    """
    Garde les entrées dont date >= today_utc - keep_days.
    Retourne un petit rapport.
    """
    today_utc = datetime.now(timezone.utc).date()
    cutoff = today_utc - timedelta(days=keep_days)

    data = _read_list(path)
    before = len(data)

    kept: List[Dict[str, Any]] = []
    removed_invalid_date = 0
    removed_old = 0

    for e in data:
        ok, d = _parse_date_ymd(e.get("date"))
        if not ok:
            removed_invalid_date += 1
            continue
        if d >= cutoff:
            kept.append(e)
        else:
            removed_old += 1

    after = len(kept)

    if not dry_run:
        _atomic_write(path, kept)

    return {
        "path": str(path),
        "keep_days": keep_days,
        "today_utc": str(today_utc),
        "cutoff_inclusive": str(cutoff),
        "before": before,
        "after": after,
        "removed_old": removed_old,
        "removed_invalid_date": removed_invalid_date,
        "dry_run": dry_run,
    }

# --- CLI ------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Nettoie missing_observations.json des entrées > N jours.")
    ap.add_argument("--path", type=Path, default=DEFAULT_PATH, help="Chemin du JSON des observations manquantes")
    ap.add_argument("--days", type=int, default=11, help="Nombre de jours à conserver (par défaut 11)")
    ap.add_argument("--dry-run", action="store_true", help="N'écrit pas le fichier, affiche seulement le rapport")
    args = ap.parse_args()

    report = cleanup(args.path, args.days, dry_run=args.dry_run)

    # Sortie concise pour logs/cron
    print(
        f"[cleanup_missing] path={report['path']} keep_days={report['keep_days']} "
        f"cutoff>={report['cutoff_inclusive']} before={report['before']} after={report['after']} "
        f"removed_old={report['removed_old']} removed_bad_date={report['removed_invalid_date']} "
        f"dry_run={report['dry_run']}"
    )

if __name__ == "__main__":
    main()
