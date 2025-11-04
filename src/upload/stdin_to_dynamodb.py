#!/usr/bin/env python3
# Lit un CSV depuis STDIN et écrit en batch dans DynamoDB.
# Ajout: --ttl-days pour expires_at. Ajout: --allow-empty pour accepter vide.

import sys, csv, json, argparse
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, timedelta
import boto3

def _to_decimal_or_str(v: str):
    s = v.strip()
    if s == "" or s.lower() == "nan":
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return s

def _parse_scales(v: str):
    s = v.strip()
    if not s:
        return []
    try:
        arr = json.loads(s)
        return [str(x) for x in arr] if isinstance(arr, list) else []
    except Exception:
        return []

def _parse_date_utc(date_str: str) -> datetime | None:
    ds = date_str.strip()
    if not ds:
        return None
    try:
        return datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    try:
        if ds.endswith("Z"):
            ds = ds.replace("Z", "+00:00")
        return datetime.fromisoformat(ds).astimezone(timezone.utc)
    except Exception:
        return None

def _compute_expires_at(date_str: str, days: int) -> int | None:
    d0 = _parse_date_utc(date_str)
    if d0 is None:
        return None
    d_exp = d0 + timedelta(days=days, hours=23, minutes=59, seconds=59)
    return int(d_exp.timestamp())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    ap.add_argument("--pk", required=True)           # ex: id
    ap.add_argument("--sk")                          # ex: date
    ap.add_argument("--ttl-days", type=int, default=0,
                    help="si >0, calcule expires_at= date + N jours")
    ap.add_argument("--ttl-field", default="expires_at",
                    help="nom d'attribut TTL (def=expires_at)")
    ap.add_argument("--allow-empty", action="store_true",
                    help="ne pas échouer si l'entrée est vide ou sans données")
    args = ap.parse_args()

    # Refuse absence de stdin sauf si --allow-empty
    if sys.stdin.isatty():
        if args.allow_empty:
            print("NOOP: no stdin", file=sys.stderr)
            return 0
        print("ERROR: no stdin", file=sys.stderr)
        return 2

    buf = sys.stdin.read()

    # Vide total
    if not buf.strip():
        if args.allow_empty:
            print("NOOP: empty input", file=sys.stderr)
            return 0
        print("ERROR: 0 input lines", file=sys.stderr)
        return 3

    data_lines = buf.splitlines()
    reader = csv.DictReader(data_lines)
    header = reader.fieldnames or []

    # Pas d'en-tête
    if not header:
        if args.allow_empty:
            print("NOOP: no header", file=sys.stderr)
            return 0
        print("ERROR: missing header", file=sys.stderr)
        return 4

    # Clés manquantes
    if args.pk not in header or (args.sk and args.sk not in header):
        if args.allow_empty:
            print(f"NOOP: header missing keys {args.pk}/{args.sk}", file=sys.stderr)
            return 0
        print(f"ERROR: missing header or keys. header={header}", file=sys.stderr)
        return 4

    ddb = boto3.resource("dynamodb")
    table = ddb.Table(args.table)

    pkeys = [args.pk] + ([args.sk] if args.sk else [])
    wrote = 0
    skipped = 0

    # Écrire. Si aucune ligne de données, NOOP si autorisé.
    with table.batch_writer(overwrite_by_pkeys=pkeys) as bw:
        for row in reader:
            item = {}
            for k, v in row.items():
                if k == "" or v is None:
                    continue
                if k == "_scales":
                    lst = _parse_scales(v)
                    if lst:
                        item[k] = lst
                    continue
                if k == args.pk:
                    try:
                        item[k] = int(v.strip())  # PK en Number
                    except Exception:
                        item = None
                        break
                    continue
                if args.sk and k == args.sk:
                    item[k] = v.strip()          # SK en String
                    continue
                if k == args.ttl_field:
                    try:
                        item[k] = int(str(v).strip())
                    except Exception:
                        pass
                    continue
                val = _to_decimal_or_str(v)
                if val is None:
                    continue
                item[k] = val

            if not item or args.pk not in item or (args.sk and args.sk not in item):
                skipped += 1
                continue

            if args.ttl_days > 0 and args.ttl_field not in item:
                date_col = item.get(args.sk) if args.sk else item.get("date")
                if isinstance(date_col, str):
                    exp = _compute_expires_at(date_col, args.ttl_days)
                    if exp is not None:
                        item[args.ttl_field] = exp  # int → Number

            bw.put_item(Item=item)
            wrote += 1

    if wrote == 0 and skipped == 0 and args.allow_empty:
        print("NOOP: header only", file=sys.stderr)
        return 0

    print(f"WROTE={wrote} SKIPPED={skipped}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
