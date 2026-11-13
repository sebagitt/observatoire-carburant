"""Orchestration de l'export par fenêtres : Postgres -> Parquet -> envoi -> chargement -> contrôle des comptes -> curseur.

Aucun SDK ni réseau ici : envoyer, charger et compter_destination sont fournis par l'appelant (faux dans les tests).
  envoyer(nom, octets)            nom = <table>/<source>/<fenêtre>.parquet (sortie de fenetres.nom_fichier) ; l'appelant
                                  en fait le chemin du Volume avec chargement_databricks.chemin_volume(catalogue, nom).
  charger(table, fichiers)        un seul appel pour tous les fichiers ; fichiers = noms de fenêtre seuls
                                  (<borne>__<borne>.parquet), la source est liée par l'appelant (sql_copy_into).
  compter_destination(table, debut, fin)
                                  nombre de lignes de la fenêtre côté destination. Pour la future implémentation réelle :
                                  écrire les bornes en littéraux d'horodatage avec décalage explicite +00:00.
Le curseur n'avance que sur le plus long préfixe de fenêtres dont le compte destination égale le compte Postgres.
Les bilans ne contiennent que des nombres, des dates et des noms de types d'exception : jamais le message d'une erreur.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from ingestion.export_curseur import (
    TABLES_EXPORTABLES,
    CurseurPerimeError,
    avancer_curseur,
    initialiser_curseur,
    lire_curseur,
)
from ingestion.export_flux import exporter_fenetre
from ingestion.fenetres import MARGE_PAR_DEFAUT, fenetres_a_exporter, nom_fichier

MAX_FICHIERS = 1000  # limite de FILES pour un COPY INTO

# Requêtes fixes par table de la liste blanche : aucun nom de table n'est interpolé.
_COMPTES = {
    "prix_flux": "SELECT count(*) FROM prix_flux WHERE collecte_ts >= %s AND collecte_ts < %s",
    "station_etat_flux": "SELECT count(*) FROM station_etat_flux WHERE collecte_ts >= %s AND collecte_ts < %s",
}
assert set(_COMPTES) == set(TABLES_EXPORTABLES)


def _compter_postgres(conn, table_source: str, debut: datetime, fin: datetime) -> int:
    with conn.cursor() as cur:
        cur.execute(_COMPTES[table_source], (debut, fin))
        n = cur.fetchone()[0]
    conn.rollback()  # lecture seule
    return n


def traiter_table(
    conn,
    table_source: str,
    source: str,
    maintenant: datetime,
    envoyer,
    charger,
    compter_destination,
    marge: timedelta = MARGE_PAR_DEFAUT,
    max_fenetres: int = 48,
) -> dict:
    """Exporte les fenêtres closes (au plus max_fenetres), les charge, vérifie les comptes, avance le curseur."""
    if table_source not in TABLES_EXPORTABLES:
        raise ValueError("table non exportable")
    if not 1 <= max_fenetres <= MAX_FICHIERS:
        raise ValueError(f"max_fenetres doit être compris entre 1 et {MAX_FICHIERS}")
    bilan = {
        "table": table_source,
        "source": source,
        "statut": "ok",
        "raison": None,
        "fenetres_traitees": 0,
        "fenetres_vides": 0,
        "fenetres_verifiees": 0,
        "fichiers_envoyes": [],
        "curseur_avant": None,
        "curseur_apres": None,
        "echecs": [],
        "echec": None,
    }

    def echouer(phase, fenetre=None, erreur=None, **plus):
        info = {"phase": phase, "fenetre": fenetre, "erreur": type(erreur).__name__ if erreur else None, **plus}
        bilan["echecs"].append(info)
        bilan["echec"] = bilan["echecs"][0]
        bilan["statut"] = "echec"

    def terminer():
        bilan["curseur_apres"] = lire_curseur(conn, table_source, source)
        return bilan

    curseur = lire_curseur(conn, table_source, source)
    if curseur is None:
        curseur = initialiser_curseur(conn, table_source, source)
    if curseur is None:
        bilan.update(statut="rien_a_faire", raison="table source vide")
        return bilan
    bilan["curseur_avant"] = curseur
    fenetres = fenetres_a_exporter(curseur, maintenant, marge)[:max_fenetres]
    if not fenetres:
        bilan.update(statut="rien_a_faire", raison="aucune fenêtre close")
        return terminer()

    # Phase 1 : export et envoi, fenêtre par fenêtre ; le premier échec arrête la phase.
    traitees: list[tuple[datetime, datetime, int]] = []
    for debut, fin in fenetres:
        n = _compter_postgres(conn, table_source, debut, fin)
        if n == 0:
            traitees.append((debut, fin, 0))
            continue
        nom = nom_fichier(table_source, source, debut, fin)
        try:
            octets = exporter_fenetre(conn, table_source, debut, fin)
        except Exception as erreur:  # noqa: BLE001 - jamais le message
            echouer("export", (debut, fin), erreur)
            break
        try:
            envoyer(nom, octets)
        except Exception as erreur:  # noqa: BLE001 - jamais le message
            echouer("envoi", (debut, fin), erreur)
            break
        traitees.append((debut, fin, n))
        bilan["fichiers_envoyes"].append(nom.rsplit("/", 1)[1])
    bilan["fenetres_traitees"] = len(traitees)
    bilan["fenetres_vides"] = sum(1 for _, _, n in traitees if n == 0)

    # Phase 2 : un seul chargement pour tous les fichiers envoyés.
    if bilan["fichiers_envoyes"]:
        try:
            charger(table_source, list(bilan["fichiers_envoyes"]))
        except Exception as erreur:  # noqa: BLE001 - jamais le message
            echouer("chargement", None, erreur)
            return terminer()  # curseur inchangé

    # Phase 3 : compte destination = compte Postgres, dans l'ordre ; arrêt à la première fenêtre en désaccord.
    verifiees = 0
    for debut, fin, attendu in traitees:
        try:
            obtenu = compter_destination(table_source, debut, fin)
        except Exception as erreur:  # noqa: BLE001 - jamais le message
            echouer("comptage", (debut, fin), erreur)
            break
        if obtenu != attendu:
            echouer("comptage", (debut, fin), None, compte_postgres=attendu, compte_destination=obtenu)
            break
        verifiees += 1
    bilan["fenetres_verifiees"] = verifiees
    if verifiees:
        try:
            avancer_curseur(conn, table_source, source, curseur, traitees[verifiees - 1][1])
        except CurseurPerimeError as erreur:
            echouer("curseur", None, erreur)
    return terminer()
