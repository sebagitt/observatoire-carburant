"""Lecture seule du bronze Databricks : python -m ingestion.lire_bronze [--table T] [--debut ISO --fin ISO].

Une requête par table : total, min et max de collecte_ts, doublons de clé (count(*) moins count(DISTINCT clé)) et, si
une plage est donnée, les lignes de [debut, fin[ (count(*) FILTER, page count de la documentation Databricks SQL lue le
2026-11-16). Avant chaque requête : assurer_entrepot (peut démarrer l'entrepôt, aucune écriture de données).
Aucune connexion Postgres, aucune table créée. Sortie : une ligne LECTURE par table, uniquement des nombres et des dates.
Code retour : 0 lu, 1 échec (seul le nom du type d'exception est affiché), 2 variable absente ou option invalide.
Le SDK n'est importé que dans creer_client (bootstrap) : ce module peut être importé sans lui.
Limite : count(DISTINCT ...) ignore les clés contenant NULL, une telle ligne est comptée dans les doublons.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime, timezone

from ingestion.adaptateurs_databricks import _litteral, assurer_entrepot
from ingestion.bootstrap_databricks import PAUSE_REQUETE, ErreurBootstrap, lire_variables
from ingestion.chargement_databricks import SCHEMA_BRONZE, _identifiant, executer

NOMS_VARIABLES = ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_CATALOG", "DATABRICKS_WAREHOUSE_ID")
# Liste blanche : table bronze -> clé d'unicité. Aucun autre nom n'entre dans le SQL.
CLES = {
    "prix_flux": "station_id, carburant, maj",
    "station_etat_flux": "station_id, collecte_ts",
}
TABLES = tuple(CLES)

_ENTIER = re.compile(r"[0-9]+")
_HORODATAGE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?(Z|[+-][0-9]{2}:?[0-9]{2})?")


def _verifier_plage(debut, fin) -> None:
    if (debut is None) != (fin is None):
        raise ValueError("debut et fin vont ensemble")
    if debut is None:
        return
    for borne in (debut, fin):
        if not isinstance(borne, datetime) or borne.tzinfo is None or borne.utcoffset() is None:
            raise ValueError("borne invalide : datetime avec fuseau attendu")
    if fin <= debut:
        raise ValueError("fin doit être postérieure à debut")


def sql_lecture(catalogue: str, table: str, debut: datetime | None = None, fin: datetime | None = None) -> str:
    """Texte de l'unique SELECT de lecture d'une table bronze. Rien n'est exécuté ici."""
    if table not in CLES:
        raise ValueError("table non autorisée")
    _identifiant(catalogue, "catalogue")
    _verifier_plage(debut, fin)
    colonnes = [
        "count(*) AS total",
        "min(collecte_ts) AS min_ts",
        "max(collecte_ts) AS max_ts",
        f"count(*) - count(DISTINCT {CLES[table]}) AS doublons",
    ]
    if debut is not None:
        colonnes.append(
            f"count(*) FILTER (WHERE collecte_ts >= TIMESTAMP '{_litteral(debut)}' "
            f"AND collecte_ts < TIMESTAMP '{_litteral(fin)}') AS lignes_plage"
        )
    return f"SELECT {', '.join(colonnes)} FROM {catalogue}.{SCHEMA_BRONZE}.{table}"


def _entier(brut) -> int:
    if isinstance(brut, str) and _ENTIER.fullmatch(brut):
        return int(brut)
    if isinstance(brut, int) and not isinstance(brut, bool) and brut >= 0:
        return brut
    raise ErreurBootstrap("réponse de lecture inattendue")


def _horodatage(brut) -> str | None:
    if brut is None:
        return None
    if isinstance(brut, str) and _HORODATAGE.fullmatch(brut):
        return brut
    raise ErreurBootstrap("réponse de lecture inattendue")


def lire_table(client, catalogue: str, entrepot: str, table: str, debut=None, fin=None, dormir=time.sleep, horloge=time.monotonic) -> dict:
    """Assure l'entrepôt, exécute la lecture, rend des nombres et des dates validés."""
    instruction = sql_lecture(catalogue, table, debut, fin)
    assurer_entrepot(client, entrepot, dormir=dormir, horloge=horloge)
    reponse = executer(client, entrepot, instruction, dormir, PAUSE_REQUETE)
    lignes = getattr(getattr(reponse, "result", None), "data_array", None)
    attendu = 5 if debut is not None else 4
    if not lignes or len(lignes) != 1 or len(lignes[0]) != attendu:
        raise ErreurBootstrap("réponse de lecture inattendue")
    ligne = lignes[0]
    return {
        "table": table,
        "total": _entier(ligne[0]),
        "min": _horodatage(ligne[1]),
        "max": _horodatage(ligne[2]),
        "doublons": _entier(ligne[3]),
        "plage": (debut, fin) if debut is not None else None,
        "lignes_plage": _entier(ligne[4]) if debut is not None else None,
    }


def _iso(borne: datetime) -> str:
    return borne.astimezone(timezone.utc).isoformat()


def formater(resultat: dict) -> str:
    texte = (
        f"LECTURE table={resultat['table']} total={resultat['total']} min={resultat['min'] or 'aucun'} "
        f"max={resultat['max'] or 'aucun'} doublons={resultat['doublons']}"
    )
    if resultat["plage"] is not None:
        debut, fin = resultat["plage"]
        texte += f" plage={_iso(debut)}/{_iso(fin)} lignes={resultat['lignes_plage']}"
    return texte


def _date(texte: str) -> datetime:
    try:
        valeur = datetime.fromisoformat(texte)
    except ValueError:
        raise argparse.ArgumentTypeError("date ISO attendue") from None
    if valeur.tzinfo is None or valeur.utcoffset() is None:
        raise argparse.ArgumentTypeError("date avec fuseau attendue")
    return valeur


def lire_arguments(argv):
    parseur = argparse.ArgumentParser(prog="python -m ingestion.lire_bronze", description="Lit le total des tables bronze (lecture seule).")
    parseur.add_argument("--table", choices=(*TABLES, "toutes"), default="toutes")
    parseur.add_argument("--debut", type=_date)
    parseur.add_argument("--fin", type=_date)
    args = parseur.parse_args(argv)
    try:
        _verifier_plage(args.debut, args.fin)
    except ValueError as erreur:
        parseur.error(str(erreur))
    return args


def main(argv=None, env=None, sortie=print, creer_client=None, dormir=time.sleep, horloge=time.monotonic) -> int:
    args = lire_arguments(argv)
    env = dict(os.environ) if env is None else env
    variables = lire_variables(env)
    manquantes = [nom for nom in NOMS_VARIABLES if nom not in variables]
    if manquantes:
        sortie("KO variable manquante : " + ", ".join(manquantes))
        return 2
    if creer_client is None:
        from ingestion.bootstrap_databricks import creer_client
    tables = TABLES if args.table == "toutes" else (args.table,)
    try:
        client = creer_client(env)
        for table in tables:
            resultat = lire_table(
                client, variables["DATABRICKS_CATALOG"], variables["DATABRICKS_WAREHOUSE_ID"], table,
                args.debut, args.fin, dormir=dormir, horloge=horloge,
            )
            sortie(formater(resultat))
        return 0
    except Exception as erreur:  # noqa: BLE001 - jamais le message : il peut contenir l'hôte, le jeton ou l'entrepôt
        sortie(f"ERREUR {type(erreur).__name__}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
