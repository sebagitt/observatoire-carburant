"""DAG transformation_dbt : dbt build de la chaîne gold, déclenché par l'Asset carburant_bronze (émis par controler).

La logique vit dans ingestion/taches_dbt.py (testée avec pytest) ; le DAG ne fait que l'orchestrer.
Les imports de ingestion.* restent dans la fonction : l'analyse du fichier par Airflow n'en dépend pas.
Créé en pause : à activer après la recréation du conteneur sur l'image avec le venv dbt.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.sdk import Asset, dag, task


@dag(
    dag_id="transformation_dbt",
    schedule=[Asset("carburant_bronze")],
    start_date=pendulum.datetime(2026, 11, 26, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["transformation", "dbt", "databricks"],
)
def transformation_dbt():
    @task
    def construire() -> None:
        import os

        from ingestion.taches_dbt import commande_dbt, environnement_dbt, lancer_dbt

        lancer_dbt(commande_dbt(), environnement_dbt(os.environ))

    construire()


transformation_dbt()
