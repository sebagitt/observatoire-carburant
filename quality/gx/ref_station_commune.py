"""Suite Great Expectations : validation du rattachement station commune (flux quotidien) avant chargement dans ref_station_commune.

Règles tirées de la mesure du STOP 0 de J6 (17/11/2026, fichier publié à 07:07:55 UTC) : 9 952 stations, 9 941 avec un
com_arm_code, aucun code hors format (cinq chiffres, ou 2A et 2B suivis de trois chiffres). Minimum de 9 500 lignes validé
au STOP 0. Une station sans code est acceptée (NULL) : la couverture est mesurée dans dbt.
"""
from __future__ import annotations

import great_expectations as gx
import great_expectations.expectations as gxe

COLONNES = ["station_id", "com_arm_code"]
LIGNES_MIN = 9_500
FORME_CODE = r"^[0-9][0-9AB][0-9]{3}$"


def construire_suite() -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name="ref_station_commune_lot")
    attentes = [
        gxe.ExpectTableColumnsToMatchSet(column_set=COLONNES, exact_match=True),
        gxe.ExpectTableRowCountToBeBetween(min_value=LIGNES_MIN),
        gxe.ExpectColumnValuesToNotBeNull(column="station_id"),
        gxe.ExpectColumnValuesToBeUnique(column="station_id"),
        # les valeurs nulles sont ignorées par cette attente : une station sans code passe
        gxe.ExpectColumnValuesToMatchRegex(column="com_arm_code", regex=FORME_CODE),
    ]
    for attente in attentes:
        suite.add_expectation(attente)
    return suite


def valider_station_commune(df) -> dict:
    context = gx.get_context(mode="ephemeral")
    asset = context.data_sources.add_pandas("lot_station_commune").add_dataframe_asset("stations")
    batch = asset.add_batch_definition_whole_dataframe("tout").get_batch(batch_parameters={"dataframe": df})
    resultat = batch.validate(construire_suite())
    echecs = [
        {"expectation": r.expectation_config.type, "colonne": r.expectation_config.kwargs.get("column")}
        for r in resultat.results
        if not r.success
    ]
    return {"success": bool(resultat.success), "echecs": echecs}
