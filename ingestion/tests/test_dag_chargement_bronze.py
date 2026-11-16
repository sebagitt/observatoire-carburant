"""DAG chargement_bronze : structure et import sans le SDK Databricks. Exige Airflow (conteneur), ignoré sur l'hôte.

Le fichier du DAG est cherché dans <racine>/airflow/dags s'il existe (copie de mutation), sinon dans dags_folder
d'Airflow (conteneur : /opt/airflow/dags, monté depuis airflow/dags).
"""
import subprocess
import sys
from pathlib import Path

import pytest

# airflow.models et pas airflow : sur l'hôte, le dossier airflow/ du dépôt s'importe comme paquet d'espace de noms vide.
pytest.importorskip("airflow.models")
RACINE = Path(__file__).resolve().parents[2]


def _dossier_dags() -> Path:
    local = RACINE / "airflow" / "dags"
    if local.is_dir():
        return local
    from airflow.configuration import conf

    return Path(conf.get("core", "dags_folder"))


FICHIER = _dossier_dags() / "chargement_bronze.py"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(FICHIER), include_examples=False)


def test_dag_importe_sans_erreur(dagbag):
    assert dagbag.import_errors == {}
    assert "chargement_bronze" in dagbag.dags


def test_planification_et_garde_fous(dagbag):
    dag = dagbag.dags["chargement_bronze"]
    assert dag.timetable.summary == "20 12 * * *"
    assert type(dag.timetable).__name__ == "CronTriggerTimetable"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.is_paused_upon_creation is True
    assert set(dag.tags) >= {"chargement", "bronze"}


def test_ordre_charger_puis_controler(dagbag):
    dag = dagbag.dags["chargement_bronze"]
    assert set(dag.task_ids) == {"charger", "controler"}
    assert dag.get_task("controler").upstream_task_ids == {"charger"}
    assert dag.get_task("charger").upstream_task_ids == set()


def test_reprises_et_delai(dagbag):
    from datetime import timedelta

    tache = dagbag.dags["chargement_bronze"].get_task("charger")
    assert tache.retries == 1
    assert tache.retry_delay == timedelta(minutes=5)
    assert tache.execution_timeout == timedelta(minutes=20)


def test_importer_le_dag_ne_charge_pas_le_sdk_ni_ingestion():
    code = (
        "import importlib.util, sys\n"
        f"spec = importlib.util.spec_from_file_location('dag_chargement_bronze', {str(FICHIER)!r})\n"
        "module = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(module)\n"
        "charges = sorted(m for m in sys.modules if m.startswith(('databricks', 'ingestion')))\n"
        "print(charges)\n"
    )
    fini = subprocess.run([sys.executable, "-c", code], cwd=RACINE, capture_output=True, text=True, timeout=120)
    assert fini.returncode == 0, fini.stderr[-500:]
    assert fini.stdout.strip().splitlines()[-1] == "[]"
