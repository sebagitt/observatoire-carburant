"""Tâches du DAG chargement_bronze (logique testable sans Airflow ; le DAG ne fait que les appeler).

tache_charger : assure l'entrepôt, puis charger_bronze.main sur toutes les tables (prix_flux puis station_etat_flux),
  48 fenêtres et 3 appels au plus par table, avec le même client. Les lignes de bilan existantes vont dans `sortie`.
  Échec (ChargementEchoue) si une variable manque, si main rend un code non nul, si aucun bilan n'est produit ou si un
  statut n'est ni ok ni rien_a_faire (le dernier appel d'une table rend normalement rien_a_faire : aucune fenêtre close).
tache_controler : une lecture par table (lire_bronze.lire_table, qui assure l'entrepôt avant sa requête) ;
  DoublonsBronze si une table a des doublons de clé.
Les messages ne contiennent que des noms de variables, de tables, de statuts et des nombres. Le SDK n'est importé que
dans creer_client (bootstrap), appelé seulement si aucun client factice n'est fourni.
"""
from __future__ import annotations

import os
import time

STATUTS_ACCEPTES = frozenset({"ok", "rien_a_faire"})
ARGUMENTS_CHARGEMENT = ["--table", "toutes", "--max-fenetres", "48", "--max-appels", "3"]
NOMS_VARIABLES = ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_CATALOG", "DATABRICKS_WAREHOUSE_ID")


class ChargementEchoue(RuntimeError):
    """Le chargement n'est pas allé au bout : variable absente, code non nul ou statut non accepté."""


class DoublonsBronze(RuntimeError):
    """Une table bronze contient des doublons de clé."""


def _variables(env) -> dict[str, str]:
    from ingestion.bootstrap_databricks import lire_variables

    variables = lire_variables(env)
    manquantes = [nom for nom in NOMS_VARIABLES if nom not in variables]
    if manquantes:
        raise ChargementEchoue("variable manquante : " + ", ".join(manquantes))
    return variables


def tache_charger(env=None, sortie=print, creer_client=None, main=None, traiter=None, dormir=time.sleep) -> list[dict]:
    env = dict(os.environ) if env is None else env
    variables = _variables(env)
    from ingestion.adaptateurs_databricks import assurer_entrepot

    if creer_client is None:
        from ingestion.bootstrap_databricks import creer_client
    if main is None:
        from ingestion.charger_bronze import main
    if traiter is None:
        from ingestion.orchestration_export import traiter_table as traiter

    client = creer_client(env)
    duree = assurer_entrepot(client, variables["DATABRICKS_WAREHOUSE_ID"], dormir=dormir)
    sortie(f"ENTREPOT pret en {duree:.1f} s")

    bilans: list[dict] = []

    def enregistrer(*args, **kwargs):
        resultat = traiter(*args, **kwargs)
        bilans.append(resultat)
        return resultat

    code = main(ARGUMENTS_CHARGEMENT, env=env, sortie=sortie, creer_client=lambda e: client, traiter=enregistrer)
    refuses = [f"{b.get('table')}={b.get('statut')}" for b in bilans if b.get("statut") not in STATUTS_ACCEPTES]
    if code != 0 or refuses or not bilans:
        raise ChargementEchoue(f"chargement en échec : code {code}, {len(bilans)} bilans, statuts refusés {refuses}")
    return bilans


def tache_controler(env=None, sortie=print, creer_client=None, dormir=time.sleep) -> list[dict]:
    env = dict(os.environ) if env is None else env
    variables = _variables(env)
    from ingestion.lire_bronze import TABLES, formater, lire_table

    if creer_client is None:
        from ingestion.bootstrap_databricks import creer_client

    client = creer_client(env)
    resultats = []
    for table in TABLES:
        resultat = lire_table(client, variables["DATABRICKS_CATALOG"], variables["DATABRICKS_WAREHOUSE_ID"], table, dormir=dormir)
        sortie(formater(resultat))
        resultats.append(resultat)
    doublons = [f"{r['table']}={r['doublons']}" for r in resultats if r["doublons"] > 0]
    if doublons:
        raise DoublonsBronze("doublons de clé dans le bronze : " + ", ".join(doublons))
    return resultats
