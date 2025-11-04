#!/usr/bin/env python3
# Enregistre les (id, date) non récupérés dans un JSON, sans doublons.
# Écriture atomique pour éviter la corruption.

import os
import json
import tempfile
from pathlib import Path
from typing import Optional, List, Dict, Any

DEFAULT_PATH = Path(os.getenv("MISSING_OBS_JSON", "data/metadonnees/missing_observations.json"))

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
        # En cas de JSON cassé, on repart proprement.
        return []

def _atomic_write(path: Path, data: List[Dict[str, Any]]) -> None:
    _ensure_parent(path)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=str(path.parent)) as tmp:
        json.dump(data, tmp, ensure_ascii=False, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_name = tmp.name
    os.replace(tmp_name, path)

def _key(entry: Dict[str, Any]) -> tuple:
    # Unicité sur (id, date). Le reste est informatif.
    return (str(entry.get("id", "")), str(entry.get("date", "")))

def append_missing(station_id: int, date_str: str, *, reason: Optional[str] = None,
                   path: Path = DEFAULT_PATH) -> None:
    """
    Ajoute un enregistrement manquant si absent.
    - station_id: identifiant numérique de la station.
    - date_str: 'YYYY-MM-DD'.
    - reason: optionnel, courte explication.
    - path: fichier JSON cible.
    """
    entry = {"id": int(station_id), "date": date_str}
    if reason:
        entry["reason"] = reason

    data = _read_list(path)
    seen = { _key(e) for e in data }
    k = _key(entry)
    if k in seen:
        return
    data.append(entry)
    _atomic_write(path, data)
