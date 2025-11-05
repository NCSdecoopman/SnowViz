#!/usr/bin/env python3
# Enregistre les (id, date) non récupérés dans un JSON, sans doublons.
# Écriture atomique pour éviter la corruption.

import os
import json
import tempfile
from pathlib import Path
from typing import List, Dict, Any, Optional

DEFAULT_PATH = Path(os.getenv("MISSING_OBS_JSON", "data/metadonnees/missing_observations.json"))

def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

def _read_any(path: Path) -> Any:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _atomic_write(path: Path, payload: Any) -> None:
    _ensure_parent(path)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=str(path.parent)) as tmp:
        json.dump(payload, tmp, ensure_ascii=False, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_name = tmp.name
    os.replace(tmp_name, path)

def _to_grouped(data: Any) -> Dict[str, Dict[str, Any]]:
    grouped = {}

    def _ins(_id: int, _date: Optional[str]) -> None:
        key = str(int(_id))
        slot = grouped.get(key)
        if slot is None:
            slot = {"id": int(_id), "dates": []}
            grouped[key] = slot
        if _date and _date not in slot["dates"]:
            slot["dates"].append(_date)

    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict): continue
            if "dates" in item and isinstance(item["dates"], list):
                for d in item["dates"]:
                    _ins(item["id"], str(d))
            elif "id" in item and "date" in item:
                _ins(item["id"], str(item["date"]))

    elif isinstance(data, dict):
        for k,item in data.items():
            if not isinstance(item, dict): continue
            if "dates" in item and isinstance(item["dates"], list):
                for d in item["dates"]:
                    _ins(item.get("id", k), str(d))
            elif "date" in item:
                _ins(item.get("id", k), str(item["date"]))

    for slot in grouped.values():
        slot["dates"].sort()

    return grouped

def _grouped_to_list(grouped: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = list(grouped.values())
    out.sort(key=lambda x: int(x["id"]))
    return out

def append_missing(station_id: int, date_str: str, *, path: Path = DEFAULT_PATH) -> None:
    raw = _read_any(path)
    grouped = _to_grouped(raw)

    key = str(int(station_id))
    slot = grouped.get(key)
    if slot is None:
        slot = {"id": int(station_id), "dates": []}
        grouped[key] = slot

    if date_str not in slot["dates"]:
        slot["dates"].append(date_str)
        slot["dates"].sort()

    payload = _grouped_to_list(grouped)
    _atomic_write(path, payload)