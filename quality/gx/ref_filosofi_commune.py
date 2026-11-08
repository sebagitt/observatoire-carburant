"""Suite Great Expectations : validation de Filosofi 2023 (niveaux commune et arrondissement municipal) avant chargement.

Dictionnaire du fichier (DS_FILOSOFI_CC_2023_metadata.csv, mesuré le 08/11/2026) : OBS_STATUS « A » normale, « O » valeur manquante ;
CONF_STATUS « F » libre, « C » information statistique confidentielle.
Mesuré le 08/11/2026 : 34 768 communes (COM) et 45 arrondissements (ARM) ; toute valeur « O » est vide ; toute valeur « A » avec
CONF_STATUS « C » est vide ; toute valeur « A » avec CONF_STATUS « F » est renseignée ; MED_SL vide pour 11,4 % des communes,
PR_MD60 vide pour 84,9 %. Les bornes de volume et de complétude sont des marges autour de ces mesures, pas des valeurs documentées.
"""
from __future__ import annotations

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.expectations.row_conditions import Column

COLONNES = ["GEO_OBJECT", "CODGEO", "MED_SL", "MED_SL_STATUT", "MED_SL_CONF", "PR_MD60", "PR_MD60_STATUT", "PR_MD60_CONF"]
NIVEAUX = ["COM", "ARM"]
STATUTS = ["A", "O"]
CONFIDENTIALITES = ["F", "C"]
LIGNES_MIN, LIGNES_MAX = 30_000, 40_000
FORME_CODE = r"^(?:\d{5}|2[AB]\d{3})$"
PART_MIN_MED_SL = 0.80  # mesuré : 88,6 % de niveaux de vie renseignés
PART_MIN_PR_MD60 = 0.05  # mesuré : 15,1 % de taux renseignés ; ce seuil détecte l'absence totale (cas de la correction du 06/08/2026)


def construire_suite() -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name="ref_filosofi_commune_lot")
    attentes = [
        gxe.ExpectTableColumnsToMatchSet(column_set=COLONNES, exact_match=True),
        gxe.ExpectTableRowCountToBeBetween(min_value=LIGNES_MIN, max_value=LIGNES_MAX),
        gxe.ExpectColumnValuesToNotBeNull(column="GEO_OBJECT"),
        gxe.ExpectColumnValuesToNotBeNull(column="CODGEO"),
        gxe.ExpectColumnValuesToBeInSet(column="GEO_OBJECT", value_set=NIVEAUX),
        gxe.ExpectColumnValuesToMatchRegex(column="CODGEO", regex=FORME_CODE),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["GEO_OBJECT", "CODGEO"]),
        gxe.ExpectColumnValuesToBeInSet(column="MED_SL_STATUT", value_set=STATUTS),
        gxe.ExpectColumnValuesToBeInSet(column="PR_MD60_STATUT", value_set=STATUTS),
        gxe.ExpectColumnValuesToBeInSet(column="MED_SL_CONF", value_set=CONFIDENTIALITES),
        gxe.ExpectColumnValuesToBeInSet(column="PR_MD60_CONF", value_set=CONFIDENTIALITES),
        # bornes de valeurs (les valeurs vides sont ignorées par ces attentes)
        gxe.ExpectColumnValuesToBeBetween(column="MED_SL", min_value=0, strict_min=True),
        gxe.ExpectColumnValuesToBeBetween(column="PR_MD60", min_value=0, max_value=100),
        # cohérence des statuts, mesurée le 08/11/2026
        gxe.ExpectColumnValuesToBeNull(column="MED_SL", row_condition=Column("MED_SL_STATUT") == "O"),
        gxe.ExpectColumnValuesToBeNull(column="PR_MD60", row_condition=Column("PR_MD60_STATUT") == "O"),
        gxe.ExpectColumnValuesToNotBeNull(column="MED_SL", row_condition=(Column("MED_SL_STATUT") == "A") & (Column("MED_SL_CONF") == "F")),
        gxe.ExpectColumnValuesToNotBeNull(column="PR_MD60", row_condition=(Column("PR_MD60_STATUT") == "A") & (Column("PR_MD60_CONF") == "F")),
        # complétude d'ensemble
        gxe.ExpectColumnValuesToNotBeNull(column="MED_SL", mostly=PART_MIN_MED_SL),
        gxe.ExpectColumnValuesToNotBeNull(column="PR_MD60", mostly=PART_MIN_PR_MD60),
    ]
    for attente in attentes:
        suite.add_expectation(attente)
    return suite


def valider_filosofi_commune(df) -> dict:
    context = gx.get_context(mode="ephemeral")
    asset = context.data_sources.add_pandas("lot_filosofi").add_dataframe_asset("filosofi")
    batch = asset.add_batch_definition_whole_dataframe("tout").get_batch(batch_parameters={"dataframe": df})
    resultat = batch.validate(construire_suite())
    echecs = [
        {"expectation": r.expectation_config.type, "colonne": r.expectation_config.kwargs.get("column")}
        for r in resultat.results
        if not r.success
    ]
    return {"success": bool(resultat.success), "echecs": echecs}
