import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from quality.gx.station_etat_flux import valider_etats  # noqa: E402


def lot(n=6000):
    return pd.DataFrame(
        {"station_id": range(n), "etat_hash": [f"{i:064x}" for i in range(n)], "etat": ["{}"] * n}
    )


def echecs(df):
    r = valider_etats(df)
    return r["success"], {e["expectation"] for e in r["echecs"]}


def test_lot_valide_passe():
    assert echecs(lot()) == (True, set())


def test_station_en_double_echoue():
    ok, noms = echecs(pd.concat([lot(), lot(n=1)], ignore_index=True))
    assert not ok and "expect_column_values_to_be_unique" in noms


def test_hash_nul_echoue():
    df = lot()
    df.loc[0, "etat_hash"] = None
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_not_be_null" in noms


def test_lot_trop_petit_echoue():
    ok, noms = echecs(lot(n=50))
    assert not ok and "expect_table_row_count_to_be_between" in noms
