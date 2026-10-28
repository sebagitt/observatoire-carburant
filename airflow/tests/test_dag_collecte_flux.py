import os
from pathlib import Path

import pytest

pytest.importorskip("airflow")
RACINE = Path(__file__).resolve().parents[2]
os.environ.setdefault("PYTHONPATH", str(RACINE))


@pytest.fixture(scope="module")
def dagbag():
    import sys

    sys.path.insert(0, str(RACINE))
    from airflow.models import DagBag

    return DagBag(dag_folder=str(RACINE / "airflow" / "dags"), include_examples=False)


def test_dag_importe_sans_erreur(dagbag):
    assert dagbag.import_errors == {}


def test_dag_collecte_flux_toutes_les_15_minutes(dagbag):
    dag = dagbag.dags.get("collecte_flux")
    assert dag is not None
    assert dag.timetable.summary == "*/15 * * * *"
    assert dag.catchup is False and dag.max_active_runs == 1


def test_ordre_extraire_valider_charger(dagbag):
    dag = dagbag.dags.get("collecte_flux")
    assert set(dag.task_ids) == {"extraire", "valider", "charger"}
    assert dag.get_task("extraire").downstream_task_ids == {"valider"}
    assert dag.get_task("valider").downstream_task_ids == {"charger"}
