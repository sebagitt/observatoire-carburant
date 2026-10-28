import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from quality.gx.prix_flux import valider_lot  # noqa: E402

MAINTENANT = datetime(2026, 10, 22, 9, 0, tzinfo=timezone.utc)


def lot_valide(n=6000):
    maj = MAINTENANT - timedelta(minutes=20)
    return pd.DataFrame(
        {
            "station_id": range(n),
            "carburant": ["gazole"] * n,
            "maj": pd.Series([maj] * n, dtype="datetime64[ns, UTC]"),
            "prix": [1.9] * n,
        }
    )


def noms_en_echec(df):
    r = valider_lot(df, maintenant=MAINTENANT)
    return r["success"], {e["expectation"] for e in r["echecs"]}


def test_lot_valide_passe():
    ok, echecs = noms_en_echec(lot_valide())
    assert ok and not echecs


def test_prix_nul_echoue():
    df = lot_valide()
    df.loc[0, "prix"] = None
    ok, echecs = noms_en_echec(df)
    assert not ok and "expect_column_values_to_not_be_null" in echecs


def test_prix_hors_plage_echoue():
    df = lot_valide()
    df.loc[0, "prix"] = 19.0
    ok, echecs = noms_en_echec(df)
    assert not ok and "expect_column_values_to_be_between" in echecs


def test_carburant_inconnu_echoue():
    df = lot_valide()
    df.loc[0, "carburant"] = "kerosene"
    ok, echecs = noms_en_echec(df)
    assert not ok and "expect_column_values_to_be_in_set" in echecs


def test_lot_trop_petit_echoue():
    ok, echecs = noms_en_echec(lot_valide(n=50))
    assert not ok and "expect_table_row_count_to_be_between" in echecs


def test_flux_perime_echoue():
    df = lot_valide()
    df["maj"] = df["maj"] - timedelta(hours=10)
    ok, echecs = noms_en_echec(df)
    assert not ok and "expect_column_max_to_be_between" in echecs


def test_dates_dans_le_futur_echouent():
    df = lot_valide()
    df["maj"] = df["maj"] + timedelta(hours=2)
    ok, echecs = noms_en_echec(df)
    assert not ok and "expect_column_max_to_be_between" in echecs


def test_doublon_de_cle_echoue():
    df = pd.concat([lot_valide(), lot_valide(n=1)], ignore_index=True)
    ok, echecs = noms_en_echec(df)
    assert not ok and "expect_compound_columns_to_be_unique" in echecs
