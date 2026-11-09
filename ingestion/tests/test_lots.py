"""Chargement d'un lot Parquet (rejeu sans effet, fichier absent) et purge des lots anciens et confirmés.

Base : schéma temporaire de la fixture `conn` (les vraies tables ne sont jamais touchées). Aucun lot réel n'est lu.
"""
import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from ingestion.carburants import construire_etats
from ingestion.db import charger_etats, charger_prix, enregistrer_passage, ensure_schema
from ingestion.lots import LotAbsentError, charger_lot, purger_lots

HORODATAGE = "20261020T100000Z"
COLLECTE_TS = datetime(2026, 10, 20, 10, 0, tzinfo=timezone.utc)
MAINTENANT = datetime(2026, 10, 23, 10, 0, tzinfo=timezone.utc)  # 72 h après le lot


def prix(n=5):
    return pd.DataFrame(
        {
            "station_id": list(range(n)),
            "carburant": ["gazole"] * n,
            "maj": [pd.Timestamp("2026-10-20T08:00:00Z")] * n,
            "prix": [1.9 + i / 100 for i in range(n)],
        }
    )


def etats(n=5):
    return construire_etats([{"id": i, "adresse": f"{i} rue A", "ville": "X"} for i in range(n)])


def ecrire_lot(dossier, horodatage=HORODATAGE, df_prix=None, df_etats=None):
    chemin, chemin_etat = dossier / f"prix_{horodatage}.parquet", dossier / f"etat_{horodatage}.parquet"
    (prix() if df_prix is None else df_prix).to_parquet(chemin, index=False)
    (etats() if df_etats is None else df_etats).to_parquet(chemin_etat, index=False)
    ts = datetime.strptime(horodatage, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return {"chemin": str(chemin), "chemin_etat": str(chemin_etat), "collecte_ts": ts.isoformat()}


def compter(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]


def journaliser_sur(conn):
    return lambda ts, run, lues, inserees, changes: enregistrer_passage(conn, ts, run, lues, inserees, changes)


def charger_en_base(conn, lot):
    ensure_schema(conn)
    ts = datetime.fromisoformat(lot["collecte_ts"])
    charger_prix(conn, pd.read_parquet(lot["chemin"]), ts)
    charger_etats(conn, pd.read_parquet(lot["chemin_etat"]), ts)


# --- charger_lot -------------------------------------------------------------------------------------------------


def test_rejeu_fichiers_presents_sans_erreur_ni_doublon(conn, tmp_path):
    lot = ecrire_lot(tmp_path)
    journaliser = journaliser_sur(conn)

    premier = charger_lot(lot, conn=conn, journaliser=journaliser)
    apres_1 = [compter(conn, t) for t in ("prix_flux", "station_etat_flux", "passage_flux")]
    second = charger_lot(lot, conn=conn, journaliser=journaliser)
    apres_2 = [compter(conn, t) for t in ("prix_flux", "station_etat_flux", "passage_flux")]

    assert apres_1 == [5, 5, 1]
    assert apres_2 == apres_1
    assert premier["prix"]["inserees"] == 5 and second["prix"]["inserees"] == 0
    assert second["etats"]["inserees"] == 0
    assert (tmp_path / f"prix_{HORODATAGE}.parquet").exists() and (tmp_path / f"etat_{HORODATAGE}.parquet").exists()


def test_fichier_absent_sans_confirmation_echoue_en_nommant_le_lot(conn, tmp_path):
    lot = ecrire_lot(tmp_path)
    (tmp_path / f"prix_{HORODATAGE}.parquet").unlink()

    with pytest.raises(LotAbsentError, match=HORODATAGE):
        charger_lot(lot, conn=conn, journaliser=journaliser_sur(conn))


def test_fichier_absent_avec_confirmation_au_journal_ne_recharge_pas(conn, tmp_path):
    lot = ecrire_lot(tmp_path)
    for nom in (f"prix_{HORODATAGE}.parquet", f"etat_{HORODATAGE}.parquet"):
        (tmp_path / nom).unlink()
    (tmp_path / "purge_journal.jsonl").write_text(json.dumps({"lot": HORODATAGE, "confirme": True}) + "\n")
    ensure_schema(conn)

    resultat = charger_lot(lot, conn=conn, journaliser=journaliser_sur(conn))

    assert resultat == {"deja_charge": True, "lot": HORODATAGE}
    assert compter(conn, "prix_flux") == 0


# --- purger_lots -------------------------------------------------------------------------------------------------


def lire_journal(dossier):
    chemin = dossier / "purge_journal.jsonl"
    return [json.loads(ligne) for ligne in chemin.read_text().splitlines()] if chemin.exists() else []


def test_purge_refuse_un_lot_non_confirme(conn, tmp_path):
    ecrire_lot(tmp_path)
    ensure_schema(conn)  # base vide : rien n'est confirmé

    bilan = purger_lots(conn, tmp_path, maintenant=MAINTENANT)

    assert (tmp_path / f"prix_{HORODATAGE}.parquet").exists() and (tmp_path / f"etat_{HORODATAGE}.parquet").exists()
    assert bilan["supprimes"] == [] and HORODATAGE in bilan["refuses"]
    assert lire_journal(tmp_path) == []


def test_purge_refuse_un_lot_dont_une_cle_de_prix_manque(conn, tmp_path):
    lot = ecrire_lot(tmp_path)
    charger_en_base(conn, lot)
    plus_grand = pd.concat([prix(), prix(6).tail(1).assign(station_id=99)], ignore_index=True)
    plus_grand.to_parquet(tmp_path / f"prix_{HORODATAGE}.parquet", index=False)

    bilan = purger_lots(conn, tmp_path, maintenant=MAINTENANT)

    assert bilan["supprimes"] == [] and HORODATAGE in bilan["refuses"]
    assert (tmp_path / f"prix_{HORODATAGE}.parquet").exists()


def test_purge_refuse_un_lot_dont_un_etat_manque(conn, tmp_path):
    lot = ecrire_lot(tmp_path)
    charger_en_base(conn, lot)
    etats(6).to_parquet(tmp_path / f"etat_{HORODATAGE}.parquet", index=False)  # une station jamais stockée

    bilan = purger_lots(conn, tmp_path, maintenant=MAINTENANT)

    assert bilan["supprimes"] == [] and HORODATAGE in bilan["refuses"]


def test_purge_refuse_un_lot_confirme_de_moins_de_48_h(conn, tmp_path):
    lot = ecrire_lot(tmp_path)
    charger_en_base(conn, lot)

    bilan = purger_lots(conn, tmp_path, maintenant=COLLECTE_TS + timedelta(hours=47, minutes=59))

    assert bilan["supprimes"] == [] and HORODATAGE in bilan["refuses"]
    assert (tmp_path / f"prix_{HORODATAGE}.parquet").exists()


def test_purge_supprime_un_lot_confirme_de_plus_de_48_h_et_journalise(conn, tmp_path):
    lot = ecrire_lot(tmp_path)
    charger_en_base(conn, lot)

    bilan = purger_lots(conn, tmp_path, maintenant=MAINTENANT)

    assert bilan["supprimes"] == [HORODATAGE] and bilan["refuses"] == {}
    assert not (tmp_path / f"prix_{HORODATAGE}.parquet").exists()
    assert not (tmp_path / f"etat_{HORODATAGE}.parquet").exists()
    entree = lire_journal(tmp_path)[0]
    assert entree["lot"] == HORODATAGE and entree["confirme"] is True
    assert (entree["prix_lignes"], entree["prix_retrouvees"]) == (5, 5)
    assert (entree["etats_lignes"], entree["etats_retrouves"]) == (5, 5)
