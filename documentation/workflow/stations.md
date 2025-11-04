# Récupération des données stations (`fetch_stations.py`)

Le script `src/download/fetch_stations.py` récupère les métadonnées des stations Météo-France et déclenche la fusion.

* Logs: `logs/stations/AAAAMMJJHHMMSS.log`
* Sortie standard: CSV `id,nom,lon,lat,alt,_scales` issu du fichier fusionné final
* Sauvegarde brute par pas et département: `data/metadonnees/download/stations/{echelle}/stations_{departement}.json`

## Stockage de l’identifiant portail Météo-France (client_id)

Identifiants client OAuth2 (format `client_id:client_secret`) dans `.secrets/mf_api_id`.
Ce fichier n’est **jamais commit**.

## Génération d’un token OAuth2 (validité ~1h)

`src/utils/auth_mf.py` gère la génération en **client_credentials** et écrit `.secrets/mf_token.json`.

## Récupération du token avec cache

`src/api/token_provider.py` fournit l’accès au token:

* token valide → renvoie le cache
* sinon → regénère via `auth_mf`

Les scripts n’appellent **jamais** la génération brute. Uniquement:

```python
from src.api.token_provider import get_api_key, clear_token_cache
```

## Paramètres, en-têtes et limitation de débit

* Base API: `METEO_BASE_URL` (défaut: `https://public-api.meteofrance.fr/public/DPClim/v1`)
* Dossier de sortie: `METEO_SAVE_DIR` (défaut: `data/metadonnees/download/stations`)
* Limite requêtes: `METEO_MAX_RPM` (défaut: `50` req/min)
* Seuil altitude pour la fusion finale: `ALT_SELECT` (défaut: `1000`)

En-tête HTTP utilisé par `fetch_stations.py`:

```
accept: application/json
authorization: Bearer <token>
```

Stratégie d’erreurs:

* `401/403` → vidage cache (`clear_token_cache()`), nouveau token, retry
* `429` → respect `Retry-After`, puis retry
* `204` → log explicite “No Content”

## Téléchargement des stations par pas et par département

`fetch_stations.py` appelle les endpoints:

```
/liste-stations/infrahoraire-6m
/liste-stations/horaire
/liste-stations/quotidienne
```

Chaque réponse est annotée avec le pas (`_scale` et `_scales`) puis sauvegardée sous:

```
data/metadonnees/download/stations/{echelle}/stations_{departement}.json
```

### Exemples d’usage

Plusieurs pas, plusieurs départements:

```bash
python -m src.download.fetch_stations \
  --scales "quotidienne,horaire" \
  --departments "38,73,74"
```

Pas par défaut (= `quotidienne`) et départements par défaut (= `38,73,74`):

```bash
python -m src.download.fetch_stations
```

## Combinaison finale des stations

En fin d’exécution, `fetch_stations.py` lance automatiquement:

```python
from src.utils.combine_stations import main as combine_stations
combine_stations(alt_select=int(os.getenv("ALT_SELECT","1000")))
```

### Rôle de `src/utils/combine_stations.py`

* Agrège tous les JSON téléchargés (`data/metadonnees/download/stations/**/stations_*.json`)
* Déduplication par `id`
* Normalisation des noms:

  * `d Allevard` → `d'Allevard`
  * suppression des suffixes `-NIVO`, `_NIVO`, `NIVOSE`
  * capitalisation cohérente
* Fusion des champs `lon/lat/alt` quand incomplets
* Union des pas `_scales` et tri
* Coercition `alt` → entier (gère int/float/str, “m”, virgule)
* Filtre final:

  * `alt >= alt_select`
  * `posteOuvert == True` si présent
* Suppression de la clé `posteOuvert` dans la sortie
* Écrit le fichier unique:

  ```
  data/metadonnees/stations.json
  ```

Ce fichier devient la **source de vérité** pour les autres pipelines.

## Sortie CSV sur stdout

Si la fusion finale réussit, `fetch_stations.py` imprime vers stdout:

```
id,nom,lon,lat,alt,_scales
...
```

Sinon, il émet seulement l’en-tête.

## Journalisation

Le script écrit:

* le contexte de run (seuil altitude, pas, départements)
* le nombre d’items par pas et par département
* les erreurs de connexion
* l’état de la fusion et le nombre de stations finales

## Workflow 3 couches

| couche                                | rôle                                            |
| ------------------------------------- | ----------------------------------------------- |
| `auth_mf`                             | génération brute du token                       |
| `token_provider`                      | cache vs régénération + helpers                 |
| `fetch_stations` + `combine_stations` | métier: téléchargement + structuration + filtre |

# Workflow GitHub Actions : **Stations Weekly**

But : exécuter `fetch_stations.py` chaque vendredi vers 23:59 Europe/Paris, ingérer le CSV en flux dans DynamoDB, versionner `data/metadonnees/stations.json` si modifié, et archiver les logs du run.

## Déclencheurs

* **CRON**

  * `59 21 * * 5` : ~23:59 en été (UTC+2)
  * `59 22 * * 5` : ~23:59 en hiver (UTC+1)
* **Manuel** : `workflow_dispatch` depuis l’UI GitHub.

La garde `.github/scripts/should_run.sh fri-23:59` bloque les exécutions CRON hors de 23:59 Europe/Paris (sécurité fuseau).

## Permissions

```yaml
permissions:
  id-token: write   # OIDC vers AWS
  contents: write   # push du stations.json
```

## Secrets requis

* `AWS_ROLE_ARN` : rôle AWS à assumer par OIDC.
* `AWS_REGION` : région AWS (ex. eu-west-3).
* `MF_BASIC_AUTH_B64` : `base64(client_id:client_secret)` portail Météo-France.

## Variables d’environnement

* `METEO_TOKEN_CACHE` : chemin du cache token OAuth2 local au runner.
* `METEO_MAX_RPM` : limite soft de requêtes/minute (par défaut 50).
* `ALT_SELECT` : seuil d’altitude pour la fusion finale (par défaut 1000 m).

## Chaîne d’exécution

1. **Checkout**
2. **Gate fuseau horaire**
   `.github/scripts/should_run.sh fri-23:59` court-circuite si l’heure locale Europe/Paris ne correspond pas.
3. **Python 3.11**
4. **Installation**
   `uv` puis `requests`, `python-dateutil`, `boto3` en site-packages.
5. **AWS OIDC**
   `aws-actions/configure-aws-credentials@v4` assume le rôle `AWS_ROLE_ARN`.
6. **Téléchargement + ingestion DynamoDB**

   ```bash
   python -u -m src.download.fetch_stations \
   | python -m src.upload.stdin_to_dynamodb --table Stations --pk id
   ```

   * `fetch_stations.py` :

     * écrit des JSON bruts par pas/département sous `data/metadonnees/download/stations/**`
     * fusionne en `data/metadonnees/stations.json`
     * émet un **CSV sur stdout** (`id,nom,lon,lat,alt,_scales`)
   * `stdin_to_dynamodb` lit ce CSV et fait des **PutItem** par `id`.
     Idempotent : même `id` ⇒ remplacement de l’item, pas de doublon.
7. **Commit sélectif de `stations.json`**

   * Configure l’identité bot.
   * Vérifie un changement sur `data/metadonnees/stations.json`.
   * Commit + push avec `[skip ci]` si modifié.
     Évite de relancer d’autres workflows.
8. **Archive des logs**

   * Upload `logs/stations/*.log` comme artefact `stations-logs-${{ github.run_id }}`.
   * Rétention 14 jours.

## Flux de données

* **Entrées** : API DPClim Météo-France (OAuth2), secrets MF.
* **Sorties** :

  * Table **DynamoDB `Stations`** : upsert par `id`.
  * Fichier **versionné** : `data/metadonnees/stations.json` (source de vérité pour les workflows quotidiens).
  * **Artefact** GitHub : logs horodatés.
