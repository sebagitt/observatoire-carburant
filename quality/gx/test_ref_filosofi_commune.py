import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from quality.gx.ref_filosofi_commune import COLONNES, valider_filosofi_commune  # noqa: E402


def ligne(niveau, code, med=25000.0, pr=15.0, statut="A", conf="F"):
    return {
        "GEO_OBJECT": niveau,
        "CODGEO": code,
        "MED_SL": med if statut == "A" and conf == "F" else None,
        "MED_SL_STATUT": statut,
        "MED_SL_CONF": conf,
        "PR_MD60": pr if statut == "A" and conf == "F" else None,
        "PR_MD60_STATUT": statut,
        "PR_MD60_CONF": conf,
    }


def lot():
    """Un lot plausible : proportions proches de celles mesurées le 08/11 (34 768 COM, 45 ARM ; MED_SL vide pour 11 %, PR_MD60 pour 85 %)."""
    lignes = []
    for i in range(1000, 35000):
        if i % 8 == 0:  # environ 12 % sans niveau de vie médian (valeur manquante)
            lignes.append(ligne("COM", f"{i:05d}", statut="O"))
        elif i % 7 == 0:  # PR_MD60 présent seulement pour une minorité
            lignes.append(ligne("COM", f"{i:05d}", pr=12.5))
        else:
            lignes.append({**ligne("COM", f"{i:05d}"), "PR_MD60": None, "PR_MD60_STATUT": "A", "PR_MD60_CONF": "C"})
    lignes += [ligne("ARM", f"{13200 + i}") for i in range(1, 17)]
    return pd.DataFrame(lignes, columns=COLONNES)


def echecs(df):
    r = valider_filosofi_commune(df)
    return r["success"], {e["expectation"] for e in r["echecs"]}


def test_lot_valide_passe():
    assert echecs(lot()) == (True, set())


def test_niveau_inconnu_echoue():
    df = lot()
    df.loc[0, "GEO_OBJECT"] = "EPCI"
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_be_in_set" in noms


def test_couple_niveau_code_en_double_echoue():
    df = pd.concat([lot(), lot().iloc[:1]], ignore_index=True)
    ok, noms = echecs(df)
    assert not ok and "expect_compound_columns_to_be_unique" in noms


def test_code_mal_forme_echoue():
    df = lot()
    df.loc[0, "CODGEO"] = "001"
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_match_regex" in noms


def test_taux_de_pauvrete_hors_bornes_echoue():
    df = lot()
    df.loc[df["PR_MD60"].notna().idxmax(), "PR_MD60"] = 140.0
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_be_between" in noms


def test_niveau_de_vie_negatif_echoue():
    df = lot()
    df.loc[df["MED_SL"].notna().idxmax(), "MED_SL"] = -5.0
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_be_between" in noms


def test_statut_inconnu_echoue():
    df = lot()
    df.loc[0, "MED_SL_STATUT"] = "Z"
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_be_in_set" in noms


def test_valeur_manquante_declaree_mais_valeur_presente_echoue():
    df = lot()
    manquante = df.index[df["MED_SL_STATUT"] == "O"][0]
    df.loc[manquante, "MED_SL"] = 20000.0
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_be_null" in noms


def test_valeur_normale_et_libre_sans_valeur_echoue():
    df = lot()
    normale = df.index[(df["MED_SL_STATUT"] == "A") & (df["MED_SL_CONF"] == "F")][0]
    df.loc[normale, "MED_SL"] = None
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_not_be_null" in noms


def test_niveau_de_vie_presque_toujours_vide_echoue():
    df = lot()
    df["MED_SL"] = None
    df["MED_SL_STATUT"] = "O"
    ok, noms = echecs(df)
    assert not ok and "expect_column_values_to_not_be_null" in noms


def test_lot_trop_petit_echoue():
    ok, noms = echecs(lot().iloc[:1000])
    assert not ok and "expect_table_row_count_to_be_between" in noms


def test_colonne_manquante_echoue():
    ok, noms = echecs(lot().drop(columns=["PR_MD60_CONF"]))
    assert not ok and "expect_table_columns_to_match_set" in noms
