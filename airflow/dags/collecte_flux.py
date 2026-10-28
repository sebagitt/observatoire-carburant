"""DAG collecte_flux : flux carburants (toutes les 15 min) -> GX -> Postgres collecte.

Un seul téléchargement alimente deux tables tampon :
  - prix_flux         : un enregistrement par (station, carburant, maj), dédoublonné ;
  - station_etat_flux : une version d'état par station, écrite seulement quand l'état change.

La logique vit dans ingestion/ et quality/gx/ (testée avec pytest) ; le DAG ne fait que l'orchestrer.
Les lots transitent par des fichiers parquet dans un dossier partagé (pas par XCom : ~40 000 lignes).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pendulum

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

LOG = logging.getLogger(__name__)
DOSSIER_LOTS = Path(os.environ.get("LOTS_DIR", "/opt/airflow/data/lots"))


@dag(
    dag_id="collecte_flux",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 10, 22, tz="UTC"),
    catchup=False,  # le flux est un instantané : rien à rattraper
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["collecte", "carburants"],
)
def collecte_flux():
    @task
    def extraire() -> dict:
        from ingestion.carburants import aplatir_prix, construire_etats, telecharger_flux

        collecte_ts = datetime.now(timezone.utc)
        stations = telecharger_flux()
        prix, etats = aplatir_prix(stations), construire_etats(stations)
        DOSSIER_LOTS.mkdir(parents=True, exist_ok=True)
        horodatage = f"{collecte_ts:%Y%m%dT%H%M%S}Z"
        chemin_prix = DOSSIER_LOTS / f"prix_{horodatage}.parquet"
        chemin_etat = DOSSIER_LOTS / f"etat_{horodatage}.parquet"
        prix.to_parquet(chemin_prix, index=False)
        etats.to_parquet(chemin_etat, index=False)
        LOG.info("%d stations, %d lignes de prix, %d états -> %s", len(stations), len(prix), len(etats), horodatage)
        return {"chemin": str(chemin_prix), "chemin_etat": str(chemin_etat), "collecte_ts": collecte_ts.isoformat()}

    @task
    def valider(lot: dict) -> dict:
        import pandas as pd

        from quality.gx.prix_flux import valider_lot
        from quality.gx.station_etat_flux import valider_etats

        resultats = {
            "prix": valider_lot(pd.read_parquet(lot["chemin"])),
            "etats": valider_etats(pd.read_parquet(lot["chemin_etat"])),
        }
        echecs = {nom: r["echecs"] for nom, r in resultats.items() if not r["success"]}
        if echecs:
            # Pas de retry : les lots sont conservés sur disque pour analyse, le prochain run repart d'un flux frais.
            raise AirflowFailException(f"Lot invalide, expectations en échec : {echecs}")
        LOG.info("Lots valides (%s)", lot["chemin"])
        return lot

    @task
    def charger(lot: dict) -> dict:
        import pandas as pd

        from ingestion.db import EtatsInstablesError, charger_etats, charger_prix, connecter, ensure_schema

        collecte_ts = datetime.fromisoformat(lot["collecte_ts"])
        conn = connecter()
        try:
            ensure_schema(conn)
            compteurs = {"prix": charger_prix(conn, pd.read_parquet(lot["chemin"]), collecte_ts)}
            LOG.info("Prix : %s", compteurs["prix"])
            if compteurs["prix"]["conflits_prix"]:
                LOG.warning("%d prix corrigés à la source sans nouvelle maj (ignorés)", compteurs["prix"]["conflits_prix"])
            try:
                compteurs["etats"] = charger_etats(conn, pd.read_parquet(lot["chemin_etat"]), collecte_ts)
            except EtatsInstablesError as erreur:
                # Les prix sont déjà chargés ; on ne réessaie pas, le problème est dans la source.
                raise AirflowFailException(str(erreur)) from erreur
            LOG.info("États : %s", compteurs["etats"])
        finally:
            conn.close()
        Path(lot["chemin"]).unlink(missing_ok=True)
        Path(lot["chemin_etat"]).unlink(missing_ok=True)
        return compteurs

    charger(valider(extraire()))


collecte_flux()
