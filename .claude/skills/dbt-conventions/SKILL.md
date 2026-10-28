---
name: dbt-conventions
description: "Conventions dbt du projet (nommage, couches, matérialisation, accès, contracts, tests, unit tests, snapshots, exposures, CI, particularités Databricks). À utiliser avant d'écrire ou de modifier un modèle, un test ou un schema.yml dbt."
---

# Conventions dbt du projet

## Nommage et couches

- `stg_<source>__<table>` : staging, en vue, accès `private`.
- `int_<verbe>` : intermédiaire.
- `dim_<entité>`, `fct_<événement>`, `mart_<usage>` : accès `public`.
- Schémas Databricks : `carburant_bronze`, `carburant_silver`, `carburant_gold`.

## Matérialisation

- Staging en vue.
- `fct_prix_changement` en incrémental.
- Marts en table.

## Contracts et versions

- `contract: enforced` sur tout modèle public.
- Colonnes et types déclarés, `primary_key` via Unity Catalog.
- `mart_exposition_territoire` en v1 puis v2 (un indicateur ajouté) pour exercer `latest_version`.

## Tests

- Génériques : `unique`, `not_null`, `relationships`, `accepted_values`, `unique_combination_of_columns` sur chaque grain.
- Singuliers : prix plausible (plage par carburant), trous de collecte supérieurs au seuil.
- Unit tests : delta de prix, prix republié sans nouvel événement, durée de rupture, absence de chevauchement SCD2.

## Sources, snapshots, exposures

- `freshness` déclarée sur les sources bronze.
- `dim_station` en snapshot SCD2, stratégie `check` sur services et localisation.
- Exposures : dashboard Qlik et agent IA.

## CI

`dbt build --select state:modified+` avec `--defer`, puis `dbt clone` et `dbt retry` en cas d'échec.

## Databricks

- Les contraintes `not_null` et `check` sont vérifiées après la construction du modèle : garder les tests `not_null` en complément des contracts.
- Un seul SQL warehouse 2X-Small, 5 tâches concurrentes au plus, `threads` entre 2 et 4.
- Free Edition : serverless uniquement, sortie internet limitée à des domaines de confiance.

## Sources de collecte (état J2)

- `prix_flux(station_id, carburant, maj, prix, collecte_ts)` : `maj` est en UTC, corrigée à l'ingestion (la source étiquette en UTC une heure de Paris).
- `station_etat_flux(station_id, collecte_ts, etat_hash, etat)` : `etat` est un JSONB, les clés utiles sont extraites en staging. Une ligne par version d'état.

## Avant de livrer

- Nommage conforme, accès et matérialisation corrects.
- Tests minimum présents sur chaque clé et chaque jointure.
- Contract déclaré sur tout modèle public, unit test sur toute logique métier.
