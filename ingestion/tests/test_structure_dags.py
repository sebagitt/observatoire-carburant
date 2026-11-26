"""Structure des DAG : transformation_dbt déclenché par l'Asset carburant_bronze, émis par controler de chargement_bronze.

Exige Airflow (conteneur), ignoré sur l'hôte. Le dossier des DAG est lu dans DOSSIER_DAGS (copie de mutation),
par défaut /opt/airflow/dags (conteneur, monté depuis airflow/dags).
Rejouer : make test
"""
import os
from datetime import timedelta

import pytest

# airflow.models et pas airflow : sur l'hôte, le dossier airflow/ du dépôt s'importe comme paquet d'espace de noms vide.
pytest.importorskip("airflow.models")

DOSSIER_DAGS = os.environ.get("DOSSIER_DAGS", "/opt/airflow/dags")
ASSET = "carburant_bronze"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=DOSSIER_DAGS, include_examples=False)


@pytest.fixture(scope="module")
def transformation(dagbag):
    return dagbag.dags["transformation_dbt"]


def _noms_assets(objets):
    return [objet.name for objet in objets]


def test_aucune_erreur_import(dagbag):
    assert dagbag.import_errors == {}


def test_transformation_planifie_sur_le_seul_asset_bronze(transformation):
    timetable = transformation.timetable
    assert type(timetable).__name__ == "AssetTriggeredTimetable"
    assets = [asset for _, asset in timetable.asset_condition.iter_assets()]
    assert _noms_assets(assets) == [ASSET]


def test_transformation_catchup_false(transformation):
    assert transformation.catchup is False


def test_transformation_max_active_runs_1(transformation):
    assert transformation.max_active_runs == 1


def test_transformation_une_seule_tache(transformation):
    assert len(transformation.task_ids) == 1


def test_transformation_reprises_et_delais(transformation):
    (tache,) = transformation.tasks
    assert tache.retries == 1
    assert tache.retry_delay == timedelta(minutes=10)
    assert tache.execution_timeout == timedelta(minutes=30)


def test_controler_emet_l_asset_bronze(dagbag):
    controler = dagbag.dags["chargement_bronze"].get_task("controler")
    assert ASSET in _noms_assets(controler.outlets)
