"""Tâche `charger` du DAG collecte_flux : rejeu après un chargement terminé, et échec explicite d'un lot absent.

Aucune base ni réseau : les accès Postgres sont remplacés. Défaut d'origine : la tâche supprimait ses fichiers Parquet
en fin de chargement, puis une relance du même lot échouait sur pd.read_parquet (FileNotFoundError). Les fichiers sont
désormais conservés (la purge est séparée, voir test_lots.py). La logique de chargement est testée dans test_lots.py.
"""
import importlib.util
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("airflow")

RACINE = Path(__file__).resolve().parents[2]
CANDIDATS = [RACINE / "airflow" / "dags" / "collecte_flux.py", Path("/opt/airflow/dags/collecte_flux.py")]


def _charger_tache():
    chemin = next((c for c in CANDIDATS if c.exists()), None)
    if chemin is None:
        pytest.skip("collecte_flux.py introuvable")
    spec = importlib.util.spec_from_file_location("collecte_flux_sous_test", chemin)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.collecte_flux().get_task("charger").python_callable


class _ConnexionFactice:
    def close(self):
        pass


@pytest.fixture
def db_factice(monkeypatch):
    import ingestion.db as db

    monkeypatch.setattr(db, "connecter", lambda *a, **k: _ConnexionFactice())
    monkeypatch.setattr(db, "ensure_schema", lambda conn: None)
    monkeypatch.setattr(db, "charger_prix", lambda conn, df, ts: {"lot": len(df), "inserees": len(df), "conflits_prix": 0})
    monkeypatch.setattr(db, "charger_etats", lambda conn, df, ts, *a, **k: {"changees": len(df)})
    monkeypatch.setattr(db, "journaliser_passage", lambda *a, **k: True)


def _lot(tmp_path):
    chemin_prix, chemin_etat = tmp_path / "prix_20261109T100000Z.parquet", tmp_path / "etat_20261109T100000Z.parquet"
    pd.DataFrame({"station_id": [1]}).to_parquet(chemin_prix, index=False)
    pd.DataFrame({"station_id": [1]}).to_parquet(chemin_etat, index=False)
    ts = datetime(2026, 11, 9, 10, 0, tzinfo=timezone.utc).isoformat()
    return {"chemin": str(chemin_prix), "chemin_etat": str(chemin_etat), "collecte_ts": ts}, chemin_prix, chemin_etat


def test_rejeu_apres_chargement_termine_ne_plante_pas_et_garde_les_fichiers(tmp_path, db_factice):
    lot, chemin_prix, chemin_etat = _lot(tmp_path)
    charger = _charger_tache()

    charger(lot)  # premier passage : chargement terminé
    assert chemin_prix.exists() and chemin_etat.exists()  # plus de suppression en fin de tâche

    charger(lot)  # rejeu du même lot (retry) : aucune erreur


def test_lot_absent_sans_confirmation_fait_echouer_la_tache_sans_retry(tmp_path, db_factice):
    from airflow.exceptions import AirflowFailException

    lot, chemin_prix, chemin_etat = _lot(tmp_path)
    chemin_prix.unlink()
    chemin_etat.unlink()

    with pytest.raises(AirflowFailException, match="20261109T100000Z"):
        _charger_tache()(lot)
