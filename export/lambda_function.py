# lambda_function.py
import os, json, base64, time
from decimal import Decimal
from datetime import datetime, timezone
import boto3
import urllib.request

DDB = boto3.client("dynamodb")
SSM = boto3.client("ssm") # Secrets Manager est payant alors que SSM Parameter Store est gratuit

def _decimal_to_native(o):
    # Convertit Decimal → int/float pour JSON
    if isinstance(o, list):
        return [_decimal_to_native(x) for x in o]
    if isinstance(o, dict):
        return {k: _decimal_to_native(v) for k, v in o.items()}
    if isinstance(o, Decimal):
        if o % 1 == 0:
            return int(o)
        return float(o)
    return o

def _ddb_item_to_plain(item):
    from boto3.dynamodb.types import TypeDeserializer
    return TypeDeserializer().deserialize({"M": item})

def _scan_all(table, projection=None, filter_ttl=True):
    kwargs = {"TableName": table}
    if projection:
        tokens = [t.strip() for t in projection.split(",") if t.strip()]
        ean = {}
        if "#d" in tokens:
            ean["#d"] = "date"  # alias du mot réservé
        kwargs["ProjectionExpression"] = ",".join(tokens)
        if ean:
            kwargs["ExpressionAttributeNames"] = ean

    items = []
    now_epoch = int(time.time())
    while True:
        resp = DDB.scan(**kwargs)
        for it in resp.get("Items", []):
            obj = _decimal_to_native(_ddb_item_to_plain(it))
            if filter_ttl and "expires_at" in obj:
                try:
                    if int(obj["expires_at"]) <= now_epoch:
                        continue
                except Exception:
                    pass
            items.append(obj)
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items

def _get_github_token(param_name: str) -> str:
    # SecureString nécessite WithDecryption=True
    r = SSM.get_parameter(Name=param_name, WithDecryption=True)
    return r["Parameter"]["Value"]

def _github_headers(token):
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "lambda-ddb-exporter"
    }    

def _github_get_sha(owner, repo, path, headers, branch):
    # Récupère le SHA courant du fichier pour update. None si absent.
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={branch}"
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.load(r)
            return data.get("sha")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise

def _github_put_file(owner, repo, path, headers, message, content_bytes, branch, sha=None):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    payload = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode(),
        "branch": branch,
        "committer": {
            "name": "ncsdecoopman-bot",
            "email": "242443272+ncsdecoopman-bot@users.noreply.github.com"
        },
        "author": {
            "name": "ncsdecoopman-bot",
            "email": "242443272+ncsdecoopman-bot@users.noreply.github.com"
        }
    }
    if sha:
        payload["sha"] = sha
    body = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=body, headers=headers, method="PUT")
    with urllib.request.urlopen(req) as r:
        return json.load(r)

def lambda_handler(event, context):
    table = os.environ["TABLE_NAME"]
    proj = os.getenv("DDB_PROJECTION")
    gh_owner = os.environ["GH_OWNER"]
    gh_repo = os.environ["GH_REPO"]
    gh_branch = os.environ.get("GH_BRANCH", "main")
    gh_path = os.environ["GH_PATH"]
    gh_token_param = os.environ["GH_TOKEN_PARAM_NAME"]
    max_mb = int(os.getenv("MAX_JSON_MB", "95"))
    gz_path = os.getenv("FALLBACK_GZ_PATH")

    # 1) Scan DDB
    items = _scan_all(table, projection=proj, filter_ttl=True)

    # 2) Tri stable pour des diffs git lisibles
    items.sort(key=lambda x: (x.get("id", 0), x.get("date", "")))

    # 3) JSON bytes
    data_bytes = json.dumps(items, ensure_ascii=False, separators=(",", ":"), sort_keys=False).encode("utf-8")
    size_mb = len(data_bytes) / (1024 * 1024)

    # 4) Token GitHub
    token = _get_github_token(gh_token_param)
    headers = _github_headers(token)

    # 5) Commits
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    commit_msg = f"chore(observations): export daily {today}" # pas de [skip ci] pour lancer le workflown d'app

    if size_mb <= max_mb:
        sha = _github_get_sha(gh_owner, gh_repo, gh_path, headers, gh_branch)
        _github_put_file(gh_owner, gh_repo, gh_path, headers, commit_msg, data_bytes, gh_branch, sha)
        return {"status": "ok", "path": gh_path, "count": len(items), "size_mb": round(size_mb, 2)}

    # 6) Fallback gzip si trop gros
    if not gz_path:
        raise RuntimeError(f"JSON {size_mb:.2f} MB dépasse {max_mb} MB et aucun FALLBACK_GZ_PATH n’est défini.")
    import gzip, io
    buf = io.BytesIO()
    with gzip.GzipFile(filename="observations.json", mode="wb", fileobj=buf, compresslevel=6) as gz:
        gz.write(data_bytes)
    gz_bytes = buf.getvalue()
    sha = _github_get_sha(gh_owner, gh_repo, gz_path, headers, gh_branch)
    _github_put_file(gh_owner, gh_repo, gz_path, headers, commit_msg + " (gzip)", gz_bytes, gh_branch, sha)
    return {"status": "ok", "path": gz_path, "count": len(items), "size_mb_gz": round(len(gz_bytes)/(1024*1024), 2)}
