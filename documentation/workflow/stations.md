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