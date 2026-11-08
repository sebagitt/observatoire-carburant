import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from quality.gx.ref_cog_commune import COLONNES, valider_cog_commune  # noqa: E402


def ligne(typecom, com, **autres):
    base = {c: None for c in COLONNES}
    base.update({"TYPECOM": typecom, "COM": com, "LIBELLE": f"Commune {com}", "NCC": f"COMMUNE {com}", "NCCENR": f"Commune {com}", "TNCC": "0"})
    if typecom == "COM":
        base.update({"REG": "84", "DEP": "01", "CTCD": "01D", "ARR": "011", "CAN": "0101"})
    else:
        base.update({"COMPARENT": "01001"})
    base.update(autres)
    return base


def lot():
    """Un lot plausible : proportions proches de celles du COG 2026 mesuré le 08/11 (34 875 COM, 2 105 COMD, 471 COMA, 45 ARM)."""
    lignes = [ligne("COM", f"{i:05d}") for i in range(1001, 35001)]
    lignes += [ligne("COMD", f"{i:05d}") for i in range(1001, 3001)]
    lignes += [ligne("COMA", f"{i:05d}") for i in range(5001, 5401)]
    lignes += [ligne("ARM", f"{13200 + i}", COMPARENT="13055", REG="93", DEP="13", CTCD="13D", ARR="132", CAN="1301") for i in range(1, 17)]
    return pd.DataFrame(lignes, columns=COLONNES)


def echecs(df):
    r = valider_cog_commune(df)
    return r["success"], {e["expectation"] for e in r["echecs"]}


def test_lot_valide_passe():
    assert echecs(lot()) == (True, set())


def test_couple_typecom_com_en_double_echoue():
    df = pd.concat([lot(), lot().iloc[:1]], ignore_index=True)
    ok, noms = echecs(df)
    assert not ok and "expect_compound_columns_to_be_unique" in noms


def test_meme_com_sous_deux_types_est_accepte():
    # une commune nouvelle et sa commune déléguée portent le même code : 584 codes dans ce cas dans le COG 2026
    df = pd.concat([lot(), pd.DataFrame([ligne("COMD", "09999")], columns=COLONNES)], ignore_index=True)
    assert echecs(df) == (True, set())


def test_code_commune_mal_forme_echoue():
    df = lot()
    df.loc[0, "COM"] = "1001"
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_match_regex" in noms


def test_corse_2a_et_2b_sont_acceptees():
    df = pd.concat([lot(), pd.DataFrame([ligne("COM", "2A004"), ligne("COM", "2B033")], columns=COLONNES)], ignore_index=True)
    assert echecs(df) == (True, set())


def test_typecom_inconnu_echoue():
    df = lot()
    df.loc[0, "TYPECOM"] = "XXX"
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_be_in_set" in noms


def test_libelle_nul_echoue():
    df = lot()
    df.loc[0, "LIBELLE"] = None
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_not_be_null" in noms


def test_arrondissement_sans_commune_parente_echoue():
    df = lot()
    premier_arm = df.index[df["TYPECOM"] == "ARM"][0]
    df.loc[premier_arm, "COMPARENT"] = None
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_not_be_null" in noms


def test_commune_sans_departement_echoue():
    df = lot()
    df.loc[0, "DEP"] = None
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_not_be_null" in noms


def test_lot_trop_petit_echoue():
    ok, noms = echecs(lot().iloc[:1000])
    assert not ok and "expect_table_row_count_to_be_between" in noms


def test_colonne_manquante_echoue():
    ok, noms = echecs(lot().drop(columns=["CAN"]))
    assert not ok and "expect_table_columns_to_match_set" in noms
