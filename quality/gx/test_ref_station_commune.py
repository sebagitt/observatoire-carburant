import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from quality.gx.ref_station_commune import COLONNES, LIGNES_MIN, valider_station_commune  # noqa: E402


def lot(n=9_600):
    """Un lot plausible : mesure du STOP 0 de J6 (17/11), 9 952 stations dont 11 sans code."""
    lignes = [{"station_id": 1_000_000 + i, "com_arm_code": f"{1001 + i % 5000:05d}"} for i in range(n)]
    lignes[0]["com_arm_code"] = None  # station sans code : acceptée
    lignes[1]["com_arm_code"] = "2A004"
    lignes[2]["com_arm_code"] = "2B033"
    lignes[3]["com_arm_code"] = "13201"  # arrondissement de Marseille
    return pd.DataFrame(lignes, columns=COLONNES)


def echecs(df):
    r = valider_station_commune(df)
    return r["success"], {e["expectation"] for e in r["echecs"]}


def test_minimum_valide_au_stop0_est_9500():
    assert LIGNES_MIN == 9_500


def test_lot_valide_passe_avec_codes_nuls_corse_et_arrondissements():
    assert echecs(lot()) == (True, set())


def test_code_mal_forme_echoue():
    for mauvais in ("1001", "010011", "2C004", "A1001", "01 01"):
        df = lot()
        df.loc[5, "com_arm_code"] = mauvais
        ok, noms = echecs(df)
        assert not ok and "expect_column_values_to_match_regex" in noms, mauvais


def test_lot_sous_le_minimum_echoue():
    ok, noms = echecs(lot(LIGNES_MIN - 1))
    assert not ok and "expect_table_row_count_to_be_between" in noms


def test_lot_au_minimum_passe():
    assert echecs(lot(LIGNES_MIN)) == (True, set())


def test_frontiere_du_minimum_9499_refuse_9500_accepte():
    # valeurs écrites en dur (pas LIGNES_MIN) : le test doit voir un changement de la constante
    ok, noms = echecs(lot(9_499))
    assert not ok and "expect_table_row_count_to_be_between" in noms
    assert echecs(lot(9_500)) == (True, set())


def test_station_en_double_echoue():
    df = pd.concat([lot(), lot().iloc[10:11]], ignore_index=True)
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_be_unique" in noms


def test_station_nulle_echoue():
    df = lot().astype({"station_id": object})
    df.loc[7, "station_id"] = None
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_not_be_null" in noms


def test_colonne_en_trop_echoue():
    ok, noms = echecs(lot().assign(dep_code="01"))
    assert not ok and "expect_table_columns_to_match_set" in noms
