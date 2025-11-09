# Documentation niveo — Application de visualisation

## Introduction

niveo est une application de visualisation de données neige.
Elle utilise **Astro** pour le front-end et affiche les observations via tableaux et cartes interactives.

## Structure du projet

```
app/
  public/data/
      observations.json
      metadonnees/
        missing_observations.json
        stations.json
  src/
    components/
      DataTable.astro
      SnowMap.astro
    pages/
      index.astro
    styles/
      global.css
  scripts/
    copy-data.mjs
```

## Composants principaux

### DataTable.astro

Affiche les observations sous forme de tableau.
Charge un JSON et génère les colonnes et lignes à la volée.

### SnowMap.astro

Affiche une carte interactive des stations.
S’appuie sur `stations.json` pour positionner les marqueurs.

### index.astro

Page principale.
Assemble DataTable + SnowMap et organise la mise en page de l’interface.

## Styles

Styles globaux dans `src/styles/global.css`.
Personnaliser ici la charte visuelle globale.

## Installation

```bash
npm install
```

## Démarrer l’application

```bash
npm run dev
```

URL locale par défaut :
[http://localhost:3000](http://localhost:3000)

## Contribution

Pull requests acceptées.

## Licence

MIT.
Voir fichier `LICENSE`.