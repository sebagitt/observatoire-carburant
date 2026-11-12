"""Couche Databricks du chargement : noms validés, chemins du Volume, envoi, SQL (COPY INTO, CREATE TABLE).

Aucun appel réseau à l'import, aucun SDK importé ici : le client est fourni par l'appelant (voir
bootstrap_databricks.creer_client). Les noms sont validés par motif strict avant toute écriture dans du SQL ou un chemin,
et les messages d'erreur ne reprennent jamais la valeur refusée. Le catalogue vient toujours de l'appelant.
"""
from __future__ import annotations

import io
import re
import time

from ingestion.bootstrap_databricks import PAUSE_REQUETE, ErreurBootstrap, _executer

SCHEMA_BRONZE = "carburant_bronze"
VOLUME = "depot"
MAX_FICHIERS = 1000  # limite de FILES (page COPY INTO lue le 2026-11-12)

_IDENTIFIANT = re.compile(r"[A-Za-z0-9_]+")
_FICHIER_FENETRE = re.compile(r"[A-Za-z0-9_]+__[A-Za-z0-9_]+\.parquet")

# Liste blanche : colonnes lues dans le Parquet, puis DDL, par table. Rien d'autre n'entre dans le SQL.
_COLONNES = {
    "prix_flux": "station_id, carburant, maj, prix, collecte_ts",
    "station_etat_flux": "station_id, collecte_ts, etat_hash, etat",
}
_DDL = {
    "prix_flux": (
        "station_id BIGINT, carburant STRING, maj TIMESTAMP, prix DECIMAL(6,3), collecte_ts TIMESTAMP, "
        "_fichier_source STRING, _charge_ts TIMESTAMP"
    ),
    "station_etat_flux": (
        "station_id BIGINT, collecte_ts TIMESTAMP, etat_hash STRING, etat STRING, "
        "_fichier_source STRING, _charge_ts TIMESTAMP"
    ),
}
_CLAUSE_FILES = " FILES = ({})"


def _identifiant(valeur: str, nom: str) -> str:
    if not isinstance(valeur, str) or not _IDENTIFIANT.fullmatch(valeur):
        raise ValueError(f"{nom} invalide")
    return valeur


def _table(table: str) -> str:
    if table not in _COLONNES:
        raise ValueError("table non autorisée")
    return table


def _fichier(nom: str) -> str:
    if not isinstance(nom, str) or not _FICHIER_FENETRE.fullmatch(nom):
        raise ValueError("nom de fichier invalide")
    return nom


def _decouper(nom_fichier_complet: str) -> tuple[str, str, str]:
    morceaux = nom_fichier_complet.split("/") if isinstance(nom_fichier_complet, str) else []
    if len(morceaux) != 3:
        raise ValueError("nom de fichier complet invalide")
    table, source, fichier = morceaux
    return _table(table), _identifiant(source, "source"), _fichier(fichier)


def _dossier(catalogue: str, table: str, source: str) -> str:
    return f"/Volumes/{_identifiant(catalogue, 'catalogue')}/{SCHEMA_BRONZE}/{VOLUME}/{_table(table)}/{_identifiant(source, 'source')}/"


def chemin_volume(catalogue: str, nom_fichier_complet: str) -> str:
    """Chemin du fichier dans le Volume, à partir du nom <table>/<source>/<fenêtre>.parquet de fenetres.nom_fichier."""
    table, source, fichier = _decouper(nom_fichier_complet)
    return _dossier(catalogue, table, source) + fichier


def envoyer_parquet(client, chemin: str, octets: bytes) -> None:
    """Téléverse les octets au chemin donné (remplace un fichier existant : même nom, même contenu attendu)."""
    try:
        client.files.upload(file_path=chemin, contents=io.BytesIO(octets), overwrite=True)
    except Exception:  # noqa: BLE001 - le message du SDK peut contenir l'hôte ou le jeton : on ne le propage pas
        raise ErreurBootstrap("envoi impossible") from None


def sql_copy_into(catalogue: str, table: str, source: str, fichiers: list[str]) -> str:
    """COPY INTO sur la liste EXACTE de fichiers (jamais le dossier entier), avec colonnes de traçabilité."""
    dossier = _dossier(catalogue, table, source)
    if not fichiers:
        raise ValueError("liste de fichiers vide")
    if len(fichiers) > MAX_FICHIERS:
        raise ValueError("trop de fichiers")
    liste = ", ".join(f"'{_fichier(nom)}'" for nom in fichiers)
    clause = _CLAUSE_FILES.format(liste)
    return (
        f"COPY INTO {catalogue}.{SCHEMA_BRONZE}.{table} FROM (SELECT {_COLONNES[table]}, "
        f"_metadata.file_path AS _fichier_source, current_timestamp() AS _charge_ts FROM '{dossier}') "
        f"FILEFORMAT = PARQUET{clause}"
    )


def sql_creer_table(catalogue: str, table: str) -> str:
    """CREATE TABLE IF NOT EXISTS d'une table bronze. Rend seulement le texte : rien n'est exécuté ici."""
    return f"CREATE TABLE IF NOT EXISTS {_identifiant(catalogue, 'catalogue')}.{SCHEMA_BRONZE}.{_table(table)} ({_DDL[table]})"


def executer(client, entrepot: str, instruction: str, dormir=time.sleep, pause: float = PAUSE_REQUETE):
    """Exécute une instruction par l'API d'instructions SQL (attente de 50 s, au plus 5 interrogations)."""
    try:
        return _executer(client, entrepot, instruction, dormir, pause)
    except ErreurBootstrap:
        raise
    except Exception:  # noqa: BLE001 - jamais le message du SDK (hôte, jeton)
        raise ErreurBootstrap("erreur du SDK") from None
