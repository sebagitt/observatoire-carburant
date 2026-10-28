"""Suite Great Expectations : validation d'un lot d'états de stations avant chargement."""
from __future__ import annotations

import great_expectations as gx
import great_expectations.expectations as gxe

COLONNES = ["station_id", "etat_hash", "etat"]
LIGNES_MIN, LIGNES_MAX = 5_000, 50_000


def construire_suite() -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name="station_etat_flux_lot")
    for attente in (
        gxe.ExpectTableColumnsToMatchSet(column_set=COLONNES, exact_match=True),
        gxe.ExpectTableRowCountToBeBetween(min_value=LIGNES_MIN, max_value=LIGNES_MAX),
        gxe.ExpectColumnValuesToNotBeNull(column="station_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="etat_hash"),
        gxe.ExpectColumnValuesToNotBeNull(column="etat"),
        gxe.ExpectColumnValuesToBeUnique(column="station_id"),
    ):
        suite.add_expectation(attente)
    return suite


def valider_etats(df) -> dict:
    context = gx.get_context(mode="ephemeral")
    asset = context.data_sources.add_pandas("lot_etat").add_dataframe_asset("etats")
    batch = asset.add_batch_definition_whole_dataframe("tout").get_batch(batch_parameters={"dataframe": df})
    resultat = batch.validate(construire_suite())
    echecs = [
        {"expectation": r.expectation_config.type, "colonne": r.expectation_config.kwargs.get("column")}
        for r in resultat.results
        if not r.success
    ]
    return {"success": bool(resultat.success), "echecs": echecs}
