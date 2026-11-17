"""Chargement des référentiels vers carburant_bronze : python -m ingestion.charger_referentiels_bronze --table T|toutes.

Remplacement complet par table (petit volume, rafraîchissement annuel ou ponctuel : ni curseur ni fenêtres). Pour chaque
table, dans l'ordre : COUNT Postgres (l'attendu), export Parquet en mémoire (schéma fixe, tri par clé), envoi dans
/Volumes/workspace/carburant_bronze/depot/referentiels/<table>/<table>__<horodatage UTC>.parquet, puis
CREATE OR REPLACE TABLE ... FROM read_files sur ce seul fichier, puis COUNT Databricks. Un écart arrête la commande.
Garde : toute instruction envoyée ne nomme que workspace.carburant_bronze.<table de la liste> et ne lit que le dossier
referentiels du Volume. Code retour : 0 tout vert, 1 échec, 2 variable absente ou option invalide. La sortie ne contient
que des noms de table, des noms de fichier, des nombres et des noms de types d'exception.
Le SDK n'est importé que dans creer_client (bootstrap) : ce module peut être importé sans lui.
"""
from __future__ import annotations

import argparse
import io
import os
import re
import sys
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

NOMS_VARIABLES = ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_CATALOG", "DATABRICKS_WAREHOUSE_ID")
CATALOGUE = "workspace"
SCHEMA = "carburant_bronze"
DOSSIER_VOLUME = f"/Volumes/{CATALOGUE}/{SCHEMA}/depot/referentiels/"
MESSAGE_GARDE = "instruction hors workspace.carburant_bronze (garde des référentiels)"

_TEXTE, _HORODATAGE = pa.string(), pa.timestamp("us", tz="UTC")
# Liste blanche : colonnes lues dans Postgres et leur type Parquet, clé de tri. Rien d'autre n'entre dans le SQL.
_SCHEMAS = {
    "ref_cog_commune": (
        [("millesime", pa.int32()), *((c, _TEXTE) for c in ("typecom", "com", "reg", "dep", "ctcd", "arr", "tncc", "ncc", "nccenr", "libelle", "can", "comparent")), ("charge_ts", _HORODATAGE)],
        "millesime, typecom, com",
    ),
    "ref_cog_departement": (
        [("millesime", pa.int32()), *((c, _TEXTE) for c in ("dep", "reg", "cheflieu", "tncc", "ncc", "nccenr", "libelle")), ("charge_ts", _HORODATAGE)],
        "millesime, dep",
    ),
    "ref_cog_region": (
        [("millesime", pa.int32()), *((c, _TEXTE) for c in ("reg", "cheflieu", "tncc", "ncc", "nccenr", "libelle")), ("charge_ts", _HORODATAGE)],
        "millesime, reg",
    ),
    "ref_filosofi_commune": (
        [
            ("millesime", pa.int32()), ("geo_object", _TEXTE), ("codgeo", _TEXTE),
            ("med_sl", pa.float64()), ("med_sl_statut", _TEXTE), ("med_sl_conf", _TEXTE),
            ("pr_md60", pa.float64()), ("pr_md60_statut", _TEXTE), ("pr_md60_conf", _TEXTE),
            ("charge_ts", _HORODATAGE),
        ],
        "millesime, geo_object, codgeo",
    ),
    "ref_station_commune": (
        [("station_id", pa.int64()), ("com_arm_code", _TEXTE), ("fichier_date", pa.date32()), ("charge_ts", _HORODATAGE)],
        "station_id",
    ),
}
TABLES = tuple(_SCHEMAS)

_NOM_QUALIFIE = re.compile(r"\b[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*\b")
_CHEMIN_VOLUME = re.compile(r"/Volumes/[^'\s]*")
_FICHIER = re.compile(r"(?P<table>[a-z_]+)__\d{8}T\d{6}Z\.parquet")
_ENTREPOT = re.compile(r"[A-Za-z0-9_-]+")


class ErreurChargementReferentiel(Exception):
    """Écart de compte ou export incohérent : la commande s'arrête."""


def _table(table) -> str:
    if table not in _SCHEMAS:
        raise ValueError("table non autorisée")
    return table


def _catalogue(catalogue) -> str:
    if catalogue != CATALOGUE:
        raise ValueError("catalogue non autorisé")
    return catalogue


def garde_schema(sql: str) -> str:
    """Refuse toute instruction qui n'est pas un remplacement ou un comptage d'une table de la liste dans workspace.carburant_bronze."""
    if not isinstance(sql, str) or not sql.startswith(("CREATE OR REPLACE TABLE ", "SELECT count(*) FROM ")):
        raise ValueError(MESSAGE_GARDE)
    noms = _NOM_QUALIFIE.findall(sql)
    if not noms or any(nom.split(".")[:2] != [CATALOGUE, SCHEMA] or nom.split(".")[2] not in _SCHEMAS for nom in noms):
        raise ValueError(MESSAGE_GARDE)
    if any(not chemin.startswith(DOSSIER_VOLUME) for chemin in _CHEMIN_VOLUME.findall(sql)):
        raise ValueError(MESSAGE_GARDE)
    return sql


def nom_fichier(table: str, horodatage: datetime) -> str:
    return f"{_table(table)}__{horodatage.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.parquet"


def chemin_referentiel(catalogue: str, table: str, fichier: str) -> str:
    correspondance = _FICHIER.fullmatch(fichier) if isinstance(fichier, str) else None
    if not correspondance or correspondance["table"] != _table(table):
        raise ValueError("nom de fichier invalide")
    return f"/Volumes/{_catalogue(catalogue)}/{SCHEMA}/depot/referentiels/{table}/{fichier}"


def sql_remplacer(catalogue: str, table: str, fichier: str) -> str:
    chemin = chemin_referentiel(catalogue, table, fichier)
    return garde_schema(
        f"CREATE OR REPLACE TABLE {catalogue}.{SCHEMA}.{table} AS "
        f"SELECT *, _metadata.file_path AS _fichier_source, current_timestamp() AS _charge_ts "
        f"FROM read_files('{chemin}', format => 'parquet')"
    )


def sql_compter(catalogue: str, table: str) -> str:
    return garde_schema(f"SELECT count(*) FROM {_catalogue(catalogue)}.{SCHEMA}.{_table(table)}")


def compter_postgres(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {_table(table)}")
        n = cur.fetchone()[0]
    conn.rollback()  # lecture seule : ne laisse pas de transaction ouverte
    return n


def exporter_referentiel(conn, table: str) -> tuple[bytes, int]:
    """Parquet (snappy) de toute la table, trié par clé, types fixés par _SCHEMAS. NUMERIC devient un double."""
    colonnes, tri = _SCHEMAS[_table(table)]
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(c for c, _ in colonnes)} FROM {table} ORDER BY {tri}")
        lignes = cur.fetchall()
    conn.rollback()
    tableaux = []
    for i, (_, type_) in enumerate(colonnes):
        valeurs = [ligne[i] for ligne in lignes]
        if pa.types.is_floating(type_):
            valeurs = [None if v is None else float(v) for v in valeurs]
        tableaux.append(pa.array(valeurs, type=type_))
    tampon = io.BytesIO()
    pq.write_table(pa.Table.from_arrays(tableaux, schema=pa.schema(colonnes)), tampon, compression="snappy")
    return tampon.getvalue(), len(lignes)


def traiter_referentiel(conn, catalogue: str, table: str, horodatage: datetime, envoyer, executer, compter) -> dict:
    attendu = compter_postgres(conn, table)
    octets, exportees = exporter_referentiel(conn, table)
    if exportees != attendu:
        raise ErreurChargementReferentiel(f"{table} : export de {exportees} lignes, attendu {attendu}")
    fichier = nom_fichier(table, horodatage)
    envoyer(chemin_referentiel(catalogue, table, fichier), octets)
    executer(sql_remplacer(catalogue, table, fichier))
    obtenu = compter(table)
    if obtenu != attendu:
        raise ErreurChargementReferentiel(f"{table} : écart de compte, attendu {attendu}, obtenu {obtenu}")
    return {"table": table, "fichier": fichier, "compte_postgres": attendu, "compte_destination": obtenu}


def construire_dependances(client, catalogue: str, entrepot: str, pas: float = 10, max_interrogations: int = 12, dormir=time.sleep, horloge=time.monotonic):
    """(envoyer, executer, compter) sur le vrai client ; chaque instruction passe par la garde avant tout appel."""
    from ingestion.adaptateurs_databricks import _lire_entier, assurer_entrepot
    from ingestion.bootstrap_databricks import PAUSE_REQUETE
    from ingestion.chargement_databricks import envoyer_parquet, executer as executer_sql

    _catalogue(catalogue)
    if not isinstance(entrepot, str) or not _ENTREPOT.fullmatch(entrepot):
        raise ValueError("entrepôt invalide")

    def envoyer(chemin: str, octets: bytes) -> None:
        morceaux = chemin[len(DOSSIER_VOLUME):].split("/") if isinstance(chemin, str) and chemin.startswith(DOSSIER_VOLUME) else []
        if len(morceaux) != 2 or chemin_referentiel(catalogue, morceaux[0], morceaux[1]) != chemin:
            raise ValueError("chemin hors du dossier referentiels du Volume")
        envoyer_parquet(client, chemin, octets)

    def executer(sql: str):
        garde_schema(sql)
        assurer_entrepot(client, entrepot, pas, max_interrogations, dormir, horloge)
        return executer_sql(client, entrepot, sql, dormir, PAUSE_REQUETE)

    def compter(table: str) -> int:
        return _lire_entier(executer(sql_compter(catalogue, table)))

    return envoyer, executer, compter


def lire_arguments(argv):
    parseur = argparse.ArgumentParser(prog="python -m ingestion.charger_referentiels_bronze", description="Remplace les référentiels dans carburant_bronze.")
    parseur.add_argument("--table", choices=(*TABLES, "toutes"), required=True)
    return parseur.parse_args(argv)


def main(argv=None, env=None, sortie=print, connecter=None, creer_client=None, construire=None, traiter=None, maintenant=None) -> int:
    args = lire_arguments(argv)
    env = dict(os.environ) if env is None else env
    manquantes = [nom for nom in NOMS_VARIABLES if not (env.get(nom) or "").strip()]
    if manquantes:
        sortie("KO variable manquante : " + ", ".join(manquantes))
        return 2
    from ingestion.bootstrap_databricks import lire_variables

    if connecter is None:
        from ingestion.db import connecter
    if creer_client is None:
        from ingestion.bootstrap_databricks import creer_client
    construire = construire or construire_dependances
    traiter = traiter or traiter_referentiel
    maintenant = maintenant or (lambda: datetime.now(timezone.utc))
    tables = TABLES if args.table == "toutes" else (args.table,)
    conn = None
    try:
        conn = connecter()
        variables = lire_variables(env)
        catalogue = variables["DATABRICKS_CATALOG"]
        envoyer, executer, compter = construire(creer_client(env), catalogue, variables["DATABRICKS_WAREHOUSE_ID"])
        for table in tables:
            try:
                bilan = traiter(conn, catalogue, table, maintenant(), envoyer, executer, compter)
            except ErreurChargementReferentiel as erreur:
                sortie(f"ECHEC {erreur}")
                return 1
            sortie(f"[{table}] fichier={bilan['fichier']} compte_postgres={bilan['compte_postgres']} compte_destination={bilan['compte_destination']} OK")
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
