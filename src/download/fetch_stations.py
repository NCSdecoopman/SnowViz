# src/api/fetch_stations_all_scales.py
# Récupère les listes de stations par département pour 3 échelles (DPClim).
# Flux: secrets → mf_auth → cache → token_provider → scripts => API.

import os, json, time
from pathlib import Path
from typing import Dict, List, Union
from collections import deque
import requests

from ..api.token_provider import get_api_key, clear_token_cache  # récup token
from ..utils.combine_stations import main as combine_stations

BASE_URL = os.getenv("METEO_BASE_URL", "https://public-api.meteofrance.fr/public/DPClim/v1")
SAVE_DIR = Path(os.getenv("METEO_SAVE_DIR", "data/metadonnees/download/stations"))

DEPARTMENTS: List[int] = [38, 73, 74]
SCALES: Dict[str, str] = {
    "infrahoraire-6m": "/liste-stations/infrahoraire-6m",
    "horaire":         "/liste-stations/horaire",
    "quotidienne":     "/liste-stations/quotidienne",
}

MAX_RPM = int(os.getenv("METEO_MAX_RPM", "50"))
RATE_PERIOD = 60.0

class RateLimiter:
    # Fenêtre glissante simple
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

_rl = RateLimiter(MAX_RPM, RATE_PERIOD)

def _headers_json() -> Dict[str, str]:
    token = get_api_key(use_cache=True)
    return {
        "accept": "application/json",
        "authorization": f"Bearer {token}",
    }

def _annotate_with_scale(data, scale: str):
    """
    Ajoute au(x) dictionnaire(s) retourné(s) par l'API:
      - _scale: le pas de temps de CE fichier (provenance) : jetable ensuite
      - _scales: liste des pas connus pour cet objet (utile si fusion ultérieure)
    Ne touche qu'aux dict; laisse le reste intact.
    """
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                item["_scale"] = scale
                prev = item.get("_scales", [])
                if scale not in prev:
                    item["_scales"] = [*prev, scale]
    elif isinstance(data, dict):
        data["_scale"] = scale
        prev = data.get("_scales", [])
        if scale not in prev:
            data["_scales"] = [*prev, scale]
    return data

def fetch_stations_for_scale(
    department: int,
    scale: str,
    save_dir: Union[Path, str] = SAVE_DIR,
) -> list:
    if scale not in SCALES:
        raise ValueError(f"Échelle inconnue: {scale}. Attendu: {list(SCALES.keys())}")
    url = f"{BASE_URL}{SCALES[scale]}"
    params = {"id-departement": department}

    _rl.wait()
    resp = requests.get(url, headers=_headers_json(), params=params, timeout=30)

    if resp.status_code == 204:
        raise RuntimeError(f"{scale} dept {department}: Pas de contenu (204).")

    if resp.status_code in (401, 403):
        # token perimé → purge + retry unique
        preview = resp.text[:400]
        clear_token_cache()
        _rl.wait()
        resp = requests.get(url, headers=_headers_json(), params=params, timeout=30)
        try:
            resp.raise_for_status()
        except Exception:
            raise RuntimeError(f"{scale} dept {department}: HTTP {resp.status_code} {preview}")
    elif resp.status_code == 429:
        retry_after = resp.headers.get("Retry-After")
        try:
            wait = float(retry_after) if retry_after is not None else 60.0
        except ValueError:
            wait = 60.0
        time.sleep(wait)
        _rl.wait()
        resp = requests.get(url, headers=_headers_json(), params=params, timeout=30)
        resp.raise_for_status()
    else:
        resp.raise_for_status()

    data = resp.json()
    data = _annotate_with_scale(data, scale)

    out_dir = Path(save_dir) / scale
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"stations_{department}.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return data


def fetch_all_scales_all_departments(
    departments: List[int] = DEPARTMENTS,
    scales: List[str] = list(SCALES.keys()),
    save_dir: Union[Path, str] = SAVE_DIR,
):
    results: Dict[str, Dict[int, Union[list, dict]]] = {s: {} for s in scales}
    for s in scales:
        for d in departments:
            try:
                results[s][d] = fetch_stations_for_scale(d, s, save_dir=save_dir)
            except Exception as e:
                results[s][d] = {"error": str(e)}
    return results

if __name__ == "__main__":
    # Exécution directe simple
    res = fetch_all_scales_all_departments()
    for scale, per_dept in res.items():
        for dept, data in per_dept.items():
            if isinstance(data, dict) and "error" in data:
                print(f"[{scale}] Dept {dept}: error: {data['error']}")
            else:
                n = len(data) if isinstance(data, list) else "unknown"
                print(f"[{scale}] Dept {dept}: saved -> {SAVE_DIR}/{scale}/stations_{dept}.json (items: {n})")

    # Lancement du combineur après les téléchargements
    try:
        combine_stations()  # agrège et filtre alt > 500 m
    except Exception as e:
        # log court et non bloquant
        print(f"[combine] error: {e}")