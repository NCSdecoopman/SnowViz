#!/usr/bin/env python3
# Récupère les listes de stations par département et échelle (DPClim).
# Logs dans logs/stations/AAAAMMJJHHMMSS.log
# Stdout: CSV "id,nom,lon,lat,alt,_scales" issu du fichier fusionné.

import argparse
import os
import json
import time
import csv
from pathlib import Path
from typing import Dict, List, Union, Tuple
from collections import deque
from datetime import datetime, timezone
import requests
from ..api.token_provider import get_api_key, clear_token_cache
from ..utils.combine_stations import main as combine_stations

# Configuration
BASE_URL = os.getenv("METEO_BASE_URL", "https://public-api.meteofrance.fr/public/DPClim/v1")
SAVE_DIR = Path(os.getenv("METEO_SAVE_DIR", "data/metadonnees/download/stations"))
COMBINED_PATH = Path("data/metadonnees/stations.json")
ALT_SELECT = int(os.getenv("ALT_SELECT", "1000"))
DEPARTMENTS = [38, 73, 74]
SCALES = {
    "infrahoraire-6m": "/liste-stations/infrahoraire-6m",
    "horaire": "/liste-stations/horaire",
    "quotidienne": "/liste-stations/quotidienne",
}
MAX_RPM = int(os.getenv("METEO_MAX_RPM", "50"))
RATE_PERIOD = 60.0

# Rate limiter
class RateLimiter:
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

# Logging
def _init_log_file() -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    logdir = Path("logs/stations")
    logdir.mkdir(parents=True, exist_ok=True)
    return logdir / f"{ts}.log"

_LOG_PATH = _init_log_file()

def _log(msg: str) -> None:
    with _LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(msg.rstrip() + "\n")

# HTTP helpers
def _headers_json() -> Dict[str, str]:
    token = get_api_key(use_cache=True)
    return {"accept": "application/json", "authorization": f"Bearer {token}"}

# Annotate with scale
def _annotate_with_scale(data, scale: str):
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

# Fetch stations for a scale and department
def fetch_stations_for_scale(department: int, scale: str) -> list:
    if scale not in SCALES:
        raise ValueError(f"Échelle inconnue: {scale}")
    url = f"{BASE_URL}{SCALES[scale]}"
    params = {"id-departement": department}
    _rl.wait()
    resp = requests.get(url, headers=_headers_json(), params=params, timeout=30)

    # Handle errors
    if resp.status_code == 204:
        raise RuntimeError(f"{scale} dept {department}: 204 No Content")
    if resp.status_code in (401, 403):
        clear_token_cache()
        _rl.wait()
        resp = requests.get(url, headers=_headers_json(), params=params, timeout=30)
        resp.raise_for_status()
    elif resp.status_code == 429:
        retry_after = resp.headers.get("Retry-After", "60")
        time.sleep(float(retry_after))
        _rl.wait()
        resp = requests.get(url, headers=_headers_json(), params=params, timeout=30)
        resp.raise_for_status()
    else:
        resp.raise_for_status()

    data = resp.json()
    data = _annotate_with_scale(data, scale)
    out_dir = Path(SAVE_DIR) / scale
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"stations_{department}.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return data

# Main orchestrator
def fetch_all_scales_all_departments(departments: List[int], scales: List[str]):
    results = {s: {} for s in scales}
    counts = {s: {} for s in scales}
    conn_errors = 0

    for s in scales:
        for d in departments:
            try:
                data = fetch_stations_for_scale(d, s)
                n = len(data) if isinstance(data, list) else 0
                results[s][d] = data
                counts[s][d] = n
            except Exception as e:
                results[s][d] = {"error": str(e)}
                counts[s][d] = 0
                conn_errors += 1
                _log(f"[error] scale={s} dept={d} -> {e}")

    return results, counts, conn_errors

# Print merged data as CSV
def _print_merged_as_csv(path: Path) -> None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        data = []

    sysout = os.sys.stdout
    sysout.write("id,nom,lon,lat,alt,_scales\n")

    for st in (data or []):
        sid = st.get("id", "")
        nom = st.get("nom", "")
        lon = st.get("lon", "")
        lat = st.get("lat", "")
        alt = st.get("alt", "")
        scales = st.get("_scales", [])

        scales_json = json.dumps(scales, ensure_ascii=False, separators=(",", ":"))
        nom_safe = (str(nom) or "").replace(",", " ")
        line = f"{sid},{nom_safe},{lon},{lat},{alt},{scales_json}\n"
        sysout.write(line)

# Main
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scales", type=lambda s: [x.strip() for x in s.split(",")], default=["quotidienne"])
    parser.add_argument("--departments", type=lambda s: [int(x) for x in s.split(",")], default=[38, 73, 74])
    args = parser.parse_args()

    _log(f"[run] start altitude={ALT_SELECT} scales={args.scales} departments={args.departments}")
    res, counts, conn_errors = fetch_all_scales_all_departments(args.departments, args.scales)

    for s in args.scales:
        total_scale = sum(counts[s].get(d, 0) for d in args.departments)
        for d in args.departments:
            _log(f"[count] scale={s} dept={d} items={counts[s].get(d, 0)}")
        _log(f"[count] scale={s} total_items={total_scale}")

    fused_ok = True
    try:
        combine_stations(alt_select=ALT_SELECT)
    except Exception as e:
        fused_ok = False
        _log(f"[combine] error: {e}")

    fused_n = 0
    if fused_ok and COMBINED_PATH.exists():
        try:
            fused = json.loads(COMBINED_PATH.read_text(encoding="utf-8"))
            fused_n = len(fused) if isinstance(fused, list) else 0
        except Exception as e:
            _log(f"[combine] read_error: {e}")
            fused_ok = False

    _log(f"[errors] connection_errors={conn_errors}")
    _log(f"[fusion] ok={fused_ok} count={fused_n} path={COMBINED_PATH}")

    if fused_ok and fused_n > 0:
        _print_merged_as_csv(COMBINED_PATH)
    else:
        writer = csv.writer(os.sys.stdout, lineterminator="\n")
        writer.writerow(["id", "nom", "lon", "lat", "alt", "_scales"])
