from datetime import datetime, timezone

import pandas as pd

from ingestion.carburants import aplatir_prix


def station(**kw):
    base = {"id": 1000001, "gazole_prix": 1.959, "gazole_maj": "2026-10-22T08:12:00+00:00"}
    base.update(kw)
    return base


def test_une_ligne_par_station_et_carburant():
    df = aplatir_prix([station(sp95_prix=1.99, sp95_maj="2026-10-22T08:30:00+00:00")])
    assert sorted(df["carburant"]) == ["gazole", "sp95"]
    assert list(df.columns) == ["station_id", "carburant", "maj", "prix"]


def test_carburant_sans_prix_est_ignore():
    df = aplatir_prix([station(e10_prix=None, e10_maj=None)])
    assert list(df["carburant"]) == ["gazole"]


def test_offset_de_la_source_est_ignore_heure_lue_comme_paris_ete():
    # Observé en prod : la source étiquette en UTC une heure qui est en réalité locale (Paris).
    df = aplatir_prix([station(gazole_maj="2026-10-23T10:09:46+00:00")])
    assert df.loc[0, "maj"] == pd.Timestamp("2026-10-23T08:09:46Z")


def test_offset_de_la_source_est_ignore_heure_lue_comme_paris_hiver():
    df = aplatir_prix([station(gazole_maj="2026-01-15T10:00:00+00:00")])
    assert df.loc[0, "maj"] == pd.Timestamp("2026-01-15T09:00:00Z")


def test_maj_naive_est_lue_en_heure_de_paris():
    df = aplatir_prix([station(gazole_maj="2026-10-22T10:12:00")])
    assert df.loc[0, "maj"] == pd.Timestamp(datetime(2026, 10, 22, 8, 12, tzinfo=timezone.utc))


def test_station_sans_id_est_ignoree():
    assert aplatir_prix([{"gazole_prix": 1.9, "gazole_maj": "2026-10-22T08:12:00+00:00"}]).empty


def test_flux_vide_donne_dataframe_vide_avec_les_bonnes_colonnes():
    df = aplatir_prix([])
    assert df.empty and list(df.columns) == ["station_id", "carburant", "maj", "prix"]
