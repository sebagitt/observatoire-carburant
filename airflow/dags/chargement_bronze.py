"""DAG chargement_bronze : Postgres collecte -> carburant_bronze (Databricks), une fois par jour à 12:20 UTC.

La logique vit dans ingestion/taches_bronze.py (testée avec pytest) ; le DAG ne fait que l'orchestrer.
Les imports du SDK et de ingestion.* restent dans les fonctions : l'analyse du fichier par Airflow n'en dépend pas.
Créé en pause : à activer seulement après la phase B (SDK dans l'image, variables DATABRICKS_* dans le compose).
"""
from __future__ import annotations

import logging
from datetime import timedelta

import pendulum

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

LOG = logging.getLogger(__name__)


@dag(
    dag_id="chargement_bronze",
    schedule="20 12 * * *",
    start_date=pendulum.datetime(2026, 11, 16, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=20),
    },
    tags=["chargement", "bronze", "databricks"],
)
def chargement_bronze():
    @task
    def charger() -> None:
        from ingestion.taches_bronze import tache_charger

        tache_charger(sortie=LOG.info)

    @task
    def controler() -> None:
        from ingestion.taches_bronze import DoublonsBronze, tache_controler

        try:
            tache_controler(sortie=LOG.info)
        except DoublonsBronze as erreur:
            # Pas de retry : relire ne fait pas disparaître des doublons.
            raise AirflowFailException(str(erreur)) from erreur

    charger() >> controler()


chargement_bronze()
