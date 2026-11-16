# Projet dbt observatoire_carburant

Lit `carburant_bronze` (Databricks, catalogue `workspace`) et écrit dans `carburant_silver` (staging et intermediate), puis plus tard dans `carburant_gold` (marts).

## Installation (sur l'hôte)

    py -3.12 -m venv .venv-dbt
    .venv-dbt/Scripts/pip install -r requirements-dbt.txt

## Commandes (depuis la racine du dépôt)

    make dbt ARGS="debug"
    make dbt ARGS="deps"
    make dbt ARGS="build --select staging"

`make dbt` inclut `.env` pour cette seule cible, exporte les quatre `DATABRICKS_*` et pose `DBT_PROFILES_DIR` et `DBT_PROJECT_DIR` sur `dbt/`. Aucune valeur n'est écrite dans le dépôt.

Règles : jamais de `dbt run` ni de `dbt build` sans `--select` ; aucune écriture hors `carburant_silver` et `carburant_gold`.
