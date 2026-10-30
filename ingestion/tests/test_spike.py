import pandas as pd

from ingestion.spike_flux import diff_snapshots


def test_detecte_un_changement_de_prix():
    a = pd.DataFrame({"id": ["1", "2"], "gazole": ["1.95", "1.80"]})
    b = pd.DataFrame({"id": ["1", "2"], "gazole": ["2.01", "1.80"]})
    r = diff_snapshots(a, b)
    assert r["changements_par_colonne"]["gazole"] == 1
    assert r["presents_both"] == 2


def test_compte_stations_absentes():
    a = pd.DataFrame({"id": ["1", "2"], "gazole": ["1.95", "1.80"]})
    b = pd.DataFrame({"id": ["1"], "gazole": ["1.95"]})
    r = diff_snapshots(a, b)
    assert r["seulement_a"] == 1
    assert r["seulement_b"] == 0


def test_detecte_les_doublons():
    a = pd.DataFrame({"id": ["1", "1"], "gazole": ["1.95", "1.95"]})
    b = pd.DataFrame({"id": ["1"], "gazole": ["1.95"]})
    assert diff_snapshots(a, b)["doublons_a"] == 1
