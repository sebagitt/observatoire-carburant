"""Suite Great Expectations : validation de v_commune_2026.csv (COG) avant chargement dans ref_cog_commune.

Règles tirées de la documentation INSEE (page des modalités du 24/02/2026) : TYPECOM vaut COM, COMA, COMD ou ARM ;
COMPARENT désigne la commune parente des arrondissements municipaux et des communes associées ou déléguées.
Règles tirées de la mesure du 08/11/2026 sur le fichier téléchargé : 37 496 lignes (34 875 COM, 2 105 COMD, 471 COMA, 45 ARM),
aucun doublon (TYPECOM, COM), tous les COM sur 5 chiffres ou 2A/2B suivis de 3 chiffres, REG et DEP toujours renseignés pour
un COM. Les bornes de volume sont une marge large autour de cette mesure, pas une valeur documentée.
"""
from __future__ import annotations

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.expectations.row_conditions import Column

COLONNES = ["TYPECOM", "COM", "REG", "DEP", "CTCD", "ARR", "TNCC", "NCC", "NCCENR", "LIBELLE", "CAN", "COMPARENT"]
TYPES = ["COM", "COMA", "COMD", "ARM"]
LIGNES_MIN, LIGNES_MAX = 30_000, 45_000
FORME_COM = r"^(?:\d{5}|2[AB]\d{3})$"


def construire_suite() -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name="ref_cog_commune_lot")
    attentes = [
        gxe.ExpectTableColumnsToMatchSet(column_set=COLONNES, exact_match=True),
        gxe.ExpectTableRowCountToBeBetween(min_value=LIGNES_MIN, max_value=LIGNES_MAX),
        gxe.ExpectColumnValuesToNotBeNull(column="TYPECOM"),
        gxe.ExpectColumnValuesToNotBeNull(column="COM"),
        gxe.ExpectColumnValuesToNotBeNull(column="LIBELLE"),
        gxe.ExpectColumnValuesToBeInSet(column="TYPECOM", value_set=TYPES),
        gxe.ExpectColumnValuesToMatchRegex(column="COM", regex=FORME_COM),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["TYPECOM", "COM"]),
        # documenté : la commune parente existe pour les arrondissements, les communes associées et déléguées
        gxe.ExpectColumnValuesToNotBeNull(column="COMPARENT", row_condition=Column("TYPECOM") != "COM"),
        # mesuré le 08/11/2026 : région et département toujours renseignés pour une commune
        gxe.ExpectColumnValuesToNotBeNull(column="REG", row_condition=Column("TYPECOM") == "COM"),
        gxe.ExpectColumnValuesToNotBeNull(column="DEP", row_condition=Column("TYPECOM") == "COM"),
    ]
    for attente in attentes:
        suite.add_expectation(attente)
    return suite


def valider_cog_commune(df) -> dict:
    context = gx.get_context(mode="ephemeral")
    asset = context.data_sources.add_pandas("lot_cog").add_dataframe_asset("communes")
    batch = asset.add_batch_definition_whole_dataframe("tout").get_batch(batch_parameters={"dataframe": df})
    resultat = batch.validate(construire_suite())
    echecs = [
        {"expectation": r.expectation_config.type, "colonne": r.expectation_config.kwargs.get("column")}
        for r in resultat.results
        if not r.success
    ]
    return {"success": bool(resultat.success), "echecs": echecs}
