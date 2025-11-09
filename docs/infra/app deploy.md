# Workflow de déploiement niveo sur GitHub Pages

Ce workflow GitHub Actions déploie automatiquement l’application niveo (Astro) sur GitHub Pages.

## Déclencheurs

* Sur `push` sur la branche `main`
* Si un fichier sous `app/**` ou `data/**` change : un redéploiement est effectué même si seuls les JSON changent (mise à jour des données)
* Déclenchable manuellement via `workflow_dispatch`

## Permissions nécessaires

* `pages: write`
* `id-token: write`
* `contents: read`

## Concurrence

Un seul déploiement est autorisé en parallèle (`group: pages`).

## Jobs

### build

* checkout du repo
* setup Node 20
* installation des dépendances (`npm ci`)
* build Astro (`npm run build`)
* upload du dossier `app/dist` en tant qu’artefact Pages

### deploy

* dépend du job **build**
* effectue le déploiement effectif sur GitHub Pages via `actions/deploy-pages@v4`
* met à jour l’URL de l’environnement `github-pages`