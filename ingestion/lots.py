"""Lots Parquet de la collecte : chargement en base (rejouable) et purge des lots anciens et confirmés.

Un lot = deux fichiers (prix_<horodatage>.parquet et etat_<horodatage>.parquet) écrits par la tâche `extraire`.
La tâche `charger` ne les supprime plus : un retry rejoue le chargement sans effet (clés déjà vues, états identiques).
La purge, séparée, ne retire un lot que s'il a plus de 48 h ET si son contenu est retrouvé dans Postgres
(prix par clé, états par station et hash). Chaque confirmation est écrite dans un journal (JSON, une ligne par lot).
Un fichier absent n'est jamais déduit "déjà chargé" de passage_flux (non exhaustif) : seule une entrée du journal
de purge l'atteste, sinon le chargement échoue en nommant le lot.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from ingestion import db

LOG = logging.getLogger(__name__)
JOURNAL_NOM = "purge_journal.jsonl"
AGE_MIN_PURGE = timedelta(hours=48)
_MOTIF = re.compile(r"(prix|etat)_(\d{8}T\d{6}Z)\.parquet")


class LotAbsentError(RuntimeError):
    """Fichier d'un lot absent sans confirmation de chargement : rien n'est chargé, le lot est nommé dans le message."""


def identifiant_lot(lot: dict) -> str:
    correspondance = _MOTIF.fullmatch(Path(lot["chemin"]).name)
    return correspondance.group(2) if correspondance else Path(lot["chemin"]).stem


def _confirme_au_journal(journal: Path, identifiant: str) -> bool:
    if not journal.exists():
        return False
    for ligne in journal.read_text(encoding="utf-8").splitlines():
        try:
            entree = json.loads(ligne)
        except ValueError:
            continue
        if entree.get("lot") == identifiant and entree.get("confirme") is True:
            return True
    return False


def charger_lot(lot: dict, run_planifie: datetime | None = None, *, conn=None, journaliser=None, journal: Path | None = None) -> dict:
    """Charge un lot dans Postgres (prix, états, journal des passages) sans supprimer ses fichiers.

    EtatsInstablesError est relevée après la journalisation du passage (les prix sont déjà chargés).
    """
    chemin, chemin_etat = Path(lot["chemin"]), Path(lot["chemin_etat"])
    identifiant = identifiant_lot(lot)
    absents = [c.name for c in (chemin, chemin_etat) if not c.exists()]
    if absents:
        if _confirme_au_journal(journal or chemin.parent / JOURNAL_NOM, identifiant):
            LOG.info("Lot %s : fichiers purgés, chargement confirmé au journal, rien à faire", identifiant)
            return {"deja_charge": True, "lot": identifiant}
        raise LotAbsentError(
            f"Lot {identifiant} : fichier(s) absent(s) ({', '.join(absents)}) et aucune confirmation de chargement "
            f"au journal de purge : rien n'est chargé"
        )

    collecte_ts = datetime.fromisoformat(lot["collecte_ts"])
    journaliser = journaliser or db.journaliser_passage
    propre = conn is None
    if propre:
        conn = db.connecter()
    try:
        db.ensure_schema(conn)
        compteurs = {"prix": db.charger_prix(conn, pd.read_parquet(chemin), collecte_ts)}
        LOG.info("Prix : %s", compteurs["prix"])
        if compteurs["prix"]["conflits_prix"]:
            LOG.warning("%d prix corrigés à la source sans nouvelle maj (ignorés)", compteurs["prix"]["conflits_prix"])
        try:
            compteurs["etats"] = db.charger_etats(conn, pd.read_parquet(chemin_etat), collecte_ts)
        except db.EtatsInstablesError:
            # Les prix sont déjà chargés ; on ne réessaie pas, le problème est dans la source.
            journaliser(collecte_ts, run_planifie, compteurs["prix"]["lot"], compteurs["prix"]["inserees"], None)
            raise
        LOG.info("États : %s", compteurs["etats"])
        journaliser(
            collecte_ts, run_planifie, compteurs["prix"]["lot"], compteurs["prix"]["inserees"], compteurs["etats"]["changees"]
        )
    finally:
        if propre:
            conn.close()
    return compteurs


def _compter_prix_retrouves(cur, df: pd.DataFrame) -> int:
    cur.execute(
        """
        SELECT count(*) FROM unnest(%s::bigint[], %s::text[], %s::timestamptz[]) AS t(s, c, m)
        WHERE EXISTS (SELECT 1 FROM prix_flux p WHERE p.station_id = t.s AND p.carburant = t.c AND p.maj = t.m)
        """,
        ([int(x) for x in df.station_id], list(df.carburant), [x.to_pydatetime() for x in df.maj]),
    )
    return cur.fetchone()[0]


def _compter_etats_retrouves(cur, df: pd.DataFrame) -> int:
    cur.execute(
        """
        SELECT count(*) FROM unnest(%s::bigint[], %s::text[]) AS t(s, h)
        WHERE EXISTS (SELECT 1 FROM station_etat_flux e WHERE e.station_id = t.s AND e.etat_hash = t.h)
        """,
        ([int(x) for x in df.station_id], list(df.etat_hash)),
    )
    return cur.fetchone()[0]


def purger_lots(conn, dossier: Path, *, maintenant: datetime | None = None, age_min: timedelta = AGE_MIN_PURGE, journal: Path | None = None) -> dict:
    """Supprime les lots de plus de `age_min` dont tout le contenu est retrouvé dans Postgres. Renvoie le bilan.

    Un lot non confirmé (ou trop récent, incomplet, illisible) n'est jamais supprimé. La confirmation est journalisée
    avant la suppression des fichiers.
    """
    dossier = Path(dossier)
    maintenant = maintenant or datetime.now(timezone.utc)
    journal = journal or dossier / JOURNAL_NOM
    bilan: dict = {"supprimes": [], "refuses": {}}
    identifiants = sorted({m.group(2) for f in dossier.glob("*.parquet") if (m := _MOTIF.fullmatch(f.name))})
    for identifiant in identifiants:
        chemin, chemin_etat = dossier / f"prix_{identifiant}.parquet", dossier / f"etat_{identifiant}.parquet"
        horodatage = datetime.strptime(identifiant, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        age = maintenant - horodatage
        if age <= age_min:
            bilan["refuses"][identifiant] = f"âge {age.total_seconds() / 3600:.1f} h, seuil {age_min.total_seconds() / 3600:.0f} h"
            continue
        if not (chemin.exists() and chemin_etat.exists()):
            bilan["refuses"][identifiant] = "lot incomplet (un des deux fichiers est absent)"
            continue
        try:
            df_prix, df_etats = pd.read_parquet(chemin), pd.read_parquet(chemin_etat)
            with conn.cursor() as cur:
                prix_retrouvees = _compter_prix_retrouves(cur, df_prix)
                etats_retrouves = _compter_etats_retrouves(cur, df_etats)
            conn.rollback()
        except Exception as erreur:
            bilan["refuses"][identifiant] = f"non vérifiable ({type(erreur).__name__})"
            try:
                conn.rollback()
            except Exception:
                pass
            continue
        if prix_retrouvees != len(df_prix) or etats_retrouves != len(df_etats):
            bilan["refuses"][identifiant] = (
                f"non confirmé : prix {prix_retrouvees}/{len(df_prix)}, états {etats_retrouves}/{len(df_etats)}"
            )
            continue
        entree = {
            "lot": identifiant,
            "confirme": True,
            "confirme_le": maintenant.isoformat(),
            "age_heures": round(age.total_seconds() / 3600, 1),
            "prix_lignes": len(df_prix),
            "prix_retrouvees": prix_retrouvees,
            "etats_lignes": len(df_etats),
            "etats_retrouves": etats_retrouves,
        }
        with journal.open("a", encoding="utf-8") as sortie:
            sortie.write(json.dumps(entree, ensure_ascii=False) + "\n")
            sortie.flush()
            os.fsync(sortie.fileno())
        chemin.unlink()
        chemin_etat.unlink()
        bilan["supprimes"].append(identifiant)
    return bilan
