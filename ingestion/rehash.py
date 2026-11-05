"""Recalcul des hash de station_etat_flux avec la règle canonique (listes dans les chaînes JSON triées).

Opération de maintenance, rejouable (un second passage écrit 0 ligne). Exception assumée à la règle append-only
de ce tampon : c'est un UPDATE de station_etat_flux. Seul etat_hash est réécrit (etat et collecte_ts ne bougent
pas), les anciens hash sont conservés dans SAUVEGARDE, à supprimer après vérification.

Retour arrière : un UPDATE depuis SAUVEGARDE n'est valable que tant qu'aucun passage n'a écrit de version avec la
règle canonique. Dès qu'un run a chargé des états après le recalcul, il faut restaurer le pg_dump pris avant.

Simulation par défaut : python -m ingestion.rehash [--confirmer]
"""
from __future__ import annotations

import argparse

from psycopg2.extras import execute_values

from ingestion.carburants import construire_etats, hash_etat, telecharger_flux
from ingestion.db import connecter

SAUVEGARDE = "station_etat_flux_hash_sauvegarde"
SEUIL_CHANGEMENTS = 0.3
LOT_UPDATE = 5_000


class SauvegardeExistanteError(RuntimeError):
    """Une sauvegarde d'anciens hash existe déjà : on ne l'écrase jamais."""


def recalculer_hash(conn, confirmer: bool = False) -> dict:
    """Simule (par défaut) ou écrit le recalcul. Une seule transaction, annulée si le contrôle final échoue."""
    with conn.cursor() as cur:
        if confirmer:
            cur.execute("LOCK TABLE station_etat_flux IN SHARE ROW EXCLUSIVE MODE")
        cur.execute("SELECT station_id, collecte_ts, etat_hash, etat FROM station_etat_flux ORDER BY 1, 2")
        lignes = cur.fetchall()
    nouveaux = [hash_etat(etat) for _, _, _, etat in lignes]
    a_modifier = [(sid, ts, nouveau, ancien) for (sid, ts, ancien, _), nouveau in zip(lignes, nouveaux) if nouveau != ancien]
    doublons, precedent = 0, None
    for (sid, _, ancien, _), nouveau in zip(lignes, nouveaux):
        if precedent and precedent[0] == sid and precedent[1] != ancien and precedent[2] == nouveau:
            doublons += 1  # deux versions consécutives qui ne différaient que par l'ordre
        precedent = (sid, ancien, nouveau)
    resultat = {"lignes": len(lignes), "a_modifier": len(a_modifier), "doublons_consecutifs": doublons, "ecrites": 0}
    if not confirmer or not a_modifier:
        conn.rollback()
        return resultat
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (SAUVEGARDE,))
            if cur.fetchone()[0] is not None:
                raise SauvegardeExistanteError(f"{SAUVEGARDE} existe déjà : vérifier puis la supprimer avant de rejouer")
            cur.execute(
                f"CREATE TABLE {SAUVEGARDE} (station_id BIGINT NOT NULL, collecte_ts TIMESTAMPTZ NOT NULL, "
                "ancien_hash TEXT NOT NULL, PRIMARY KEY (station_id, collecte_ts))"
            )
            execute_values(
                cur,
                f"INSERT INTO {SAUVEGARDE} (station_id, collecte_ts, ancien_hash) VALUES %s",
                [(sid, ts, ancien) for sid, ts, _, ancien in a_modifier],
                page_size=LOT_UPDATE,
            )
            for debut in range(0, len(a_modifier), LOT_UPDATE):
                lot = [(sid, ts, nouveau) for sid, ts, nouveau, _ in a_modifier[debut : debut + LOT_UPDATE]]
                execute_values(
                    cur,
                    "UPDATE station_etat_flux e SET etat_hash = v.nouveau "
                    "FROM (VALUES %s) AS v(station_id, collecte_ts, nouveau) "
                    "WHERE e.station_id = v.station_id AND e.collecte_ts = v.collecte_ts "
                    "AND e.etat_hash IS DISTINCT FROM v.nouveau",
                    lot,
                    template="(%s, %s::timestamptz, %s)",
                    page_size=len(lot),
                )
                resultat["ecrites"] += cur.rowcount
            cur.execute("SELECT etat_hash, etat FROM station_etat_flux")
            if resultat["ecrites"] != len(a_modifier) or any(h != hash_etat(etat) for h, etat in cur.fetchall()):
                raise RuntimeError("Contrôle final en échec : recalcul annulé")
    except Exception:
        conn.rollback()
        raise
    conn.commit()
    return resultat


def prevoir_changements(conn, etats) -> dict:
    """Stations connues dont l'état du lot diffère du dernier état stocké (hash recalculé), sans rien écrire."""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT ON (station_id) station_id, etat FROM station_etat_flux ORDER BY station_id, collecte_ts DESC")
        dernier = {sid: hash_etat(etat) for sid, etat in cur.fetchall()}
    conn.rollback()
    connues = changees = 0
    for ligne in etats.itertuples(index=False):
        if int(ligne.station_id) in dernier:
            connues += 1
            changees += dernier[int(ligne.station_id)] != ligne.etat_hash
    return {"connues": connues, "changees": changees}


def main(argv=None) -> None:
    parseur = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parseur.add_argument("--confirmer", action="store_true", help="écrit le recalcul (simulation sinon)")
    confirmer = parseur.parse_args(argv).confirmer
    conn = connecter()
    try:
        r = recalculer_hash(conn, confirmer=confirmer)
        p = prevoir_changements(conn, construire_etats(telecharger_flux()))
    finally:
        conn.close()
    print("MODE                       :", "ECRITURE" if confirmer else "SIMULATION (rien n'est écrit)")
    print("lignes station_etat_flux   :", r["lignes"])
    print("hash à modifier            :", r["a_modifier"])
    print("versions consécutives devenues identiques :", r["doublons_consecutifs"])
    print("lignes écrites             :", r["ecrites"])
    part = p["changees"] / p["connues"] if p["connues"] else 0
    print(f"prévision prochain run     : {p['changees']} stations changées sur {p['connues']} ({part:.1%}), seuil {SEUIL_CHANGEMENTS:.0%}")


if __name__ == "__main__":
    main()
