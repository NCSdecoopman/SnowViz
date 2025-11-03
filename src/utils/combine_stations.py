# src/api/combine_stations.py
# Script: combine all stations JSON (different scales/depts) into one deduplicated file.

import json
import re
from pathlib import Path

SRC_DIR = Path("data/metadonnees/download/stations")
OUT_DIR = Path("data/metadonnees")
OUT_FILE = OUT_DIR / "stations.json"

ID_KEY = "id"
NAME_KEY = "nom"
KEEP_KEYS = ("lon", "lat", "alt")
SCALE_KEY = "_scales"
_VALID_SCALES = {"infrahoraire-6m", "horaire", "quotidienne"}

def _extract_scales(item: dict) -> set:
    """Retourne un set des pas valides à partir de _scales ou _scale."""
    out = set()
    v = item.get("_scales")
    if isinstance(v, list):
        out.update(s for s in v if s in _VALID_SCALES)
    v = item.get("_scale")
    if isinstance(v, str) and v in _VALID_SCALES:
        out.add(v)
    return out

# regex to turn " d Allevard" -> "d'Allevard" (and same for l/L)
_RE_D_APOST = re.compile(r"\b([dDlL])\s+([A-Za-zÀ-ÖØ-öø-ÿ])")
# remove occurrences like "-NIVO", "_NIVO", "NIVOSE" etc. case-insensitive
_RE_REMOVE_NIVO = re.compile(r"[-_]?\bNIVO(?:SE)?\b", flags=re.I)
# collapse multiple spaces
_RE_SPACES = re.compile(r"\s+")


def normalize_name(raw: str) -> str:
    if raw is None:
        return ""
    s = raw.strip()
    s = _RE_D_APOST.sub(r"\1'\2", s)
    s = _RE_REMOVE_NIVO.sub("", s)
    s = _RE_SPACES.sub(" ", s).strip()
    return s.lower()


_PARTICLES = {
    "de", "du", "des", "la", "le", "les", "et", "à", "au", "aux", "sur",
    "sous", "par", "en", "chez", "l", "d"
}

def _cap_first(s: str) -> str:
    if not s:
        return s
    return s[0].upper() + s[1:].lower()

def capitalize_name(normalized: str) -> str:
    if not normalized:
        return normalized
    parts = normalized.split(" ")
    out_parts = []
    for i, part in enumerate(parts):
        is_first = (i == 0)
        hy_parts = part.split("-")
        out_hy = []
        for j, h in enumerate(hy_parts):
            if "'" in h:
                pre, post = h.split("'", 1)
                pre_fmt = _cap_first(pre) if is_first or pre not in _PARTICLES else pre
                post_fmt = _cap_first(post)
                out_hy.append(f"{pre_fmt}'{post_fmt}")
            else:
                if is_first or h not in _PARTICLES:
                    out_hy.append(_cap_first(h))
                else:
                    out_hy.append(h)
            is_first = False
        out_parts.append("-".join(out_hy))
    return " ".join(out_parts)

def pick_better(existing: dict, candidate: dict) -> dict:
    # fusion lon/lat/alt
    for k in KEEP_KEYS:
        ex = existing.get(k)
        ca = candidate.get(k)
        if (ex is None or ex == "") and (ca is not None and ca != ""):
            existing[k] = ca
    # fusion des pas (_scales comme set)
    existing.setdefault(SCALE_KEY, set())
    candidate.setdefault(SCALE_KEY, set())
    existing[SCALE_KEY].update(candidate[SCALE_KEY])
    return existing

def main() -> None:
    files = list(SRC_DIR.glob("**/stations_*.json"))
    if not files:
        print(f"No input files found under {SRC_DIR}")
        return

    by_id: dict[str, dict] = {}
    for fp in files:
        try:
            arr = json.loads(fp.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"Skipping {fp} (read error): {e}")
            continue
        if not isinstance(arr, list):
            print(f"Skipping {fp} (not a list)")
            continue

        for item in arr:
            sid = str(item.get(ID_KEY, "")).strip()
            if not sid:
                continue
            raw_name = item.get(NAME_KEY) or ""
            name = normalize_name(raw_name)

            entry = {"id": sid, "nom": name}
            for k in KEEP_KEYS:
                if k in item:
                    entry[k] = item[k]

            # initialise _scales comme set
            entry[SCALE_KEY] = _extract_scales(item)

            if sid in by_id:
                by_id[sid] = pick_better(by_id[sid], entry)
            else:
                by_id[sid] = entry


    out_list = [by_id[k] for k in sorted(by_id.keys())]
    for e in out_list:
        e["nom"] = capitalize_name(e.get("nom", ""))
        # convertit set -> liste ordonnée
        sc = e.get(SCALE_KEY, set())
        if isinstance(sc, set):
            e[SCALE_KEY] = sorted(sc)
        # nettoie toute trace de _scale unitaire si présent
        if "_scale" in e:
            del e["_scale"]

    filtered = []
    for e in out_list:
        alt = e.get("alt")
        try:
            alt_val = float(alt) if alt not in (None, "") else None
        except (ValueError, TypeError):
            alt_val = None
        if alt_val is not None and alt_val > 500:
            filtered.append(e)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(json.dumps(filtered, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Combined {len(files)} files -> {len(filtered)} unique stations (alt > 500 m) saved to {OUT_FILE}")

if __name__ == "__main__":
    main()
