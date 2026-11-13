"""Chargement du bronze Databricks : python -m ingestion.charger_bronze [--table T] [--max-fenetres N] [--max-appels N].

Contrôle d'abord la présence des quatre variables DATABRICKS_* (par leurs noms : aucune valeur n'est lue ni affichée), puis
crée la table de curseur, le client et les dépendances, et boucle sur traiter_table, table par table, jusqu'à : aucune
fenêtre traitée, un échec, ou --max-appels atteint (par table). Code retour : 0 tout vert, 1 échec, 2 variable absente
ou option invalide. La sortie ne contient que des nombres, des dates et des noms de types d'exception.
Le SDK n'est importé que dans creer_client (bootstrap) : ce module peut être importé sans lui.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

NOMS_VARIABLES = ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_CATALOG", "DATABRICKS_WAREHOUSE_ID")
TABLES = ("prix_flux", "station_etat_flux")
SOURCE = "instantane"


def _borne(minimum: int, maximum: int):
    def convertir(texte: str) -> int:
        try:
            valeur = int(texte)
        except ValueError:
            raise argparse.ArgumentTypeError("entier attendu") from None
        if not minimum <= valeur <= maximum:
            raise argparse.ArgumentTypeError(f"valeur comprise entre {minimum} et {maximum} attendue")
        return valeur

    return convertir


def lire_arguments(argv):
    parseur = argparse.ArgumentParser(prog="python -m ingestion.charger_bronze", description="Charge les fenêtres closes vers carburant_bronze.")
    parseur.add_argument("--table", choices=(*TABLES, "toutes"), default="toutes")
    parseur.add_argument("--max-fenetres", type=_borne(1, 1000), default=48)
    parseur.add_argument("--max-appels", type=_borne(1, 50), default=1)
    return parseur.parse_args(argv)


def _variables_manquantes(env) -> list[str]:
    return [nom for nom in NOMS_VARIABLES if not (env.get(nom) or "").strip()]


def _date(valeur) -> str:
    return valeur.astimezone(timezone.utc).isoformat() if isinstance(valeur, datetime) else str(valeur)


def formater_bilan(bilan: dict, numero: int) -> list[str]:
    lignes = [
        f"[{bilan['table']} appel {numero}] statut={bilan['statut']} raison={bilan['raison']} traitees={bilan['fenetres_traitees']} "
        f"vides={bilan['fenetres_vides']} verifiees={bilan['fenetres_verifiees']} fichiers={len(bilan['fichiers_envoyes'])} "
        f"curseur={_date(bilan['curseur_avant'])} -> {_date(bilan['curseur_apres'])}"
    ]
    for echec in bilan["echecs"]:
        fenetre = echec.get("fenetre")
        plage = f"[{_date(fenetre[0])}, {_date(fenetre[1])}[" if fenetre else "-"
        lignes.append(
            f"  ECHEC phase={echec['phase']} fenetre={plage} erreur={echec.get('erreur')} "
            f"compte_postgres={echec.get('compte_postgres')} compte_destination={echec.get('compte_destination')}"
        )
    return lignes


def main(argv=None, env=None, sortie=print, connecter=None, creer_table=None, creer_client=None, construire=None, traiter=None, maintenant=None) -> int:
    args = lire_arguments(argv)
    env = dict(os.environ) if env is None else env
    manquantes = _variables_manquantes(env)
    if manquantes:
        sortie("KO variable manquante : " + ", ".join(manquantes))
        return 2
    # Imports tardifs, un par dépendance réellement utilisée : ni la base ni le SDK ne sont touchés avant le contrôle
    # des variables, et un faux injecté (tests sur l'hôte, sans pandas) évite l'import du vrai module.
    from ingestion.bootstrap_databricks import lire_variables

    if connecter is None:
        from ingestion.db import connecter
    if creer_table is None:
        from ingestion.export_curseur import creer_table_curseur as creer_table
    if creer_client is None:
        from ingestion.bootstrap_databricks import creer_client
    if construire is None:
        from ingestion.adaptateurs_databricks import construire_dependances as construire
    if traiter is None:
        from ingestion.orchestration_export import traiter_table as traiter
    maintenant = maintenant or (lambda: datetime.now(timezone.utc))
    tables = TABLES if args.table == "toutes" else (args.table,)
    conn = None
    try:
        conn = connecter()
        creer_table(conn)
        variables = lire_variables(env)
        client = creer_client(env)
        envoyer, charger, compter = construire(client, variables["DATABRICKS_CATALOG"], variables["DATABRICKS_WAREHOUSE_ID"], SOURCE)
        for table in tables:
            for numero in range(1, args.max_appels + 1):
                bilan = traiter(conn, table, SOURCE, maintenant(), envoyer, charger, compter, max_fenetres=args.max_fenetres)
                for ligne in formater_bilan(bilan, numero):
                    sortie(ligne)
                if bilan["statut"] == "echec":
                    return 1
                if bilan["fenetres_traitees"] == 0:
                    break
        return 0
    except Exception as erreur:  # noqa: BLE001 - jamais le message : il peut contenir l'hôte, le jeton ou l'entrepôt
        sortie(f"ERREUR {type(erreur).__name__}")
        return 1
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


if __name__ == "__main__":
    sys.exit(main())
