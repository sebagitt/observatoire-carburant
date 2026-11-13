"""Adaptateurs réels pour traiter_table : entrepôt prêt, envoi, chargement, comptage côté destination.

Le client (WorkspaceClient du SDK) est fourni par l'appelant : ce module n'importe pas le SDK et ne fait aucun appel
réseau à l'import. Les erreurs levées sont des ErreurBootstrap génériques : jamais le message du SDK (hôte, jeton,
identifiant d'entrepôt). Les bornes de comptage sont des datetime avec fuseau, converties en UTC ; aucune chaîne reçue
n'entre dans le SQL.
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timezone

from ingestion.bootstrap_databricks import PAUSE_REQUETE, ErreurBootstrap
from ingestion.chargement_databricks import (
    SCHEMA_BRONZE,
    _decouper,
    _identifiant,
    _table,
    chemin_volume,
    envoyer_parquet,
    executer,
    sql_copy_into,
)

_ENTREPOT = re.compile(r"[A-Za-z0-9_-]+")
_ENTIER = re.compile(r"[0-9]+")
_INUTILISABLES = frozenset({"FAILED", "DELETED", "DELETING"})
_TRANSITOIRES = frozenset({"STARTING", "STOPPING", "STOPPED"})


def _etat(client, entrepot: str) -> str:
    try:
        reponse = client.warehouses.get(id=entrepot)
    except Exception:  # noqa: BLE001 - le message du SDK peut contenir l'hôte ou l'identifiant
        raise ErreurBootstrap("lecture de l'état de l'entrepôt impossible") from None
    etat = getattr(reponse, "state", None)
    return str(getattr(etat, "value", etat))


def assurer_entrepot(client, entrepot: str, pas: float = 10, max_interrogations: int = 12, dormir=time.sleep, horloge=time.monotonic) -> float:
    """Attend que l'entrepôt soit RUNNING et rend la durée d'attente en secondes.

    RUNNING : retour immédiat, start n'est jamais appelé. STARTING : attente. STOPPED : un seul start, puis attente.
    STOPPING : attente de STOPPED, puis un seul start. FAILED, DELETED ou dépassement : ErreurBootstrap."""
    if max_interrogations < 1 or pas < 0:
        raise ValueError("pas ou nombre d'interrogations invalide")
    debut = horloge()
    demarre = False
    for i in range(max_interrogations):
        etat = _etat(client, entrepot)
        if etat == "RUNNING":
            return horloge() - debut
        if etat in _INUTILISABLES:
            raise ErreurBootstrap(f"entrepôt inutilisable ({etat})")
        if etat not in _TRANSITOIRES:
            raise ErreurBootstrap("état d'entrepôt inattendu")
        if etat == "STOPPED" and not demarre:
            try:
                client.warehouses.start(id=entrepot)
            except Exception:  # noqa: BLE001 - jamais le message du SDK
                raise ErreurBootstrap("démarrage de l'entrepôt impossible") from None
            demarre = True
        if i < max_interrogations - 1:
            dormir(pas)
    raise ErreurBootstrap(f"entrepôt non prêt après {max_interrogations} interrogations")


def _litteral(borne: datetime) -> str:
    """'AAAA-MM-JJ HH:MM:SS.ffffff+00:00' à partir d'un datetime avec fuseau, converti en UTC."""
    if not isinstance(borne, datetime) or borne.tzinfo is None or borne.utcoffset() is None:
        raise ValueError("borne invalide : datetime avec fuseau attendu")
    return borne.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f") + "+00:00"


def _lire_entier(reponse) -> int:
    lignes = getattr(getattr(reponse, "result", None), "data_array", None)
    if not lignes or len(lignes) != 1 or len(lignes[0]) != 1:
        raise ErreurBootstrap("réponse de comptage inattendue")
    brut = lignes[0][0]
    if isinstance(brut, str) and _ENTIER.fullmatch(brut):
        return int(brut)
    if isinstance(brut, int) and not isinstance(brut, bool) and brut >= 0:
        return brut
    raise ErreurBootstrap("réponse de comptage inattendue")


def construire_dependances(
    client,
    catalogue: str,
    entrepot: str,
    source: str,
    pas: float = 10,
    max_interrogations: int = 12,
    dormir=time.sleep,
    horloge=time.monotonic,
):
    """(envoyer, charger, compter_destination), compatibles avec orchestration_export.traiter_table."""
    _identifiant(catalogue, "catalogue")
    _identifiant(source, "source")
    if not isinstance(entrepot, str) or not _ENTREPOT.fullmatch(entrepot):
        raise ValueError("entrepôt invalide")

    def pret():
        return assurer_entrepot(client, entrepot, pas, max_interrogations, dormir, horloge)

    def envoyer(nom: str, octets: bytes) -> None:
        if _decouper(nom)[1] != source:
            raise ValueError("source du fichier différente de celle de l'export")
        envoyer_parquet(client, chemin_volume(catalogue, nom), octets)

    def charger(table: str, fichiers: list[str]) -> None:
        instruction = sql_copy_into(catalogue, _table(table), source, fichiers)
        pret()
        executer(client, entrepot, instruction, dormir, PAUSE_REQUETE)

    def compter_destination(table: str, debut: datetime, fin: datetime) -> int:
        instruction = (
            f"SELECT count(*) FROM {catalogue}.{SCHEMA_BRONZE}.{_table(table)} "
            f"WHERE collecte_ts >= TIMESTAMP '{_litteral(debut)}' AND collecte_ts < TIMESTAMP '{_litteral(fin)}'"
        )
        pret()
        return _lire_entier(executer(client, entrepot, instruction, dormir, PAUSE_REQUETE))

    return envoyer, charger, compter_destination
