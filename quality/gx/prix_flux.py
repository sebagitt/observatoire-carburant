"""Suite Great Expectations : validation d'un lot de prix avant chargement dans Postgres."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import great_expectations as gx
import great_expectations.expectations as gxe

CARBURANTS = ["gazole", "sp95", "sp98", "e10", "e85", "gplc"]
COLONNES = ["station_id", "carburant", "maj", "prix"]
LIGNES_MIN, LIGNES_MAX = 5_000, 200_000
PRIX_MIN, PRIX_MAX = 0.5, 4.0
FRAICHEUR_MAX = timedelta(hours=3)
TOLERANCE_FUTUR = timedelta(minutes=10)


def construire_suite(maintenant: datetime) -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name="prix_flux_lot")
    attentes = [
        gxe.ExpectTableColumnsToMatchSet(column_set=COLONNES, exact_match=True),
        gxe.ExpectTableRowCountToBeBetween(min_value=LIGNES_MIN, max_value=LIGNES_MAX),
        gxe.ExpectColumnValuesToNotBeNull(column="station_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="carburant"),
        gxe.ExpectColumnValuesToNotBeNull(column="maj"),
        gxe.ExpectColumnValuesToNotBeNull(column="prix"),
        gxe.ExpectColumnValuesToBeInSet(column="carburant", value_set=CARBURANTS),
        gxe.ExpectColumnValuesToBeBetween(column="prix", min_value=PRIX_MIN, max_value=PRIX_MAX),
        # Fraîcheur : la maj la plus récente doit être récente, et jamais dans le futur
        # (garde-fou contre une erreur de fuseau UTC / heure de Paris).
        gxe.ExpectColumnMaxToBeBetween(
            column="maj",
            min_value=maintenant - FRAICHEUR_MAX,
            max_value=maintenant + TOLERANCE_FUTUR,
        ),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["station_id", "carburant", "maj"]),
    ]
    for attente in attentes:
        suite.add_expectation(attente)
    return suite


def valider_lot(df, maintenant: datetime | None = None) -> dict:
    maintenant = maintenant or datetime.now(timezone.utc)
    context = gx.get_context(mode="ephemeral")
    asset = context.data_sources.add_pandas("lot").add_dataframe_asset("prix")
    batch = asset.add_batch_definition_whole_dataframe("tout").get_batch(
        batch_parameters={"dataframe": df}
    )
    resultat = batch.validate(construire_suite(maintenant))
    echecs = [
        {
            "expectation": r.expectation_config.type,
            "colonne": r.expectation_config.kwargs.get("column"),
            "observe": {k: v for k, v in (r.result or {}).items() if k.startswith(("observed", "unexpected_count"))},
        }
        for r in resultat.results
        if not r.success
    ]
    return {"success": bool(resultat.success), "echecs": echecs}
