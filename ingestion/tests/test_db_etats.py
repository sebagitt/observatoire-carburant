import pytest

from ingestion.carburants import construire_etats
from ingestion.db import EtatsInstablesError, charger_etats, ensure_schema
import pandas as pd

TS = [pd.Timestamp(f"2026-10-23T0{h}:00:00Z").to_pydatetime() for h in range(1, 6)]


def etats(n=10, modifies=(), adresse="1 rue A", extra=0):
    stations = [
        {"id": i, "adresse": "autre rue" if i in modifies else adresse, "ville": "X"}
        for i in range(n + extra)
    ]
    return construire_etats(stations)


def total(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM station_etat_flux")
        return cur.fetchone()[0]


def test_premier_chargement_insere_tout(conn):
    ensure_schema(conn)
    r = charger_etats(conn, etats(), TS[0])
    assert (r["inserees"], r["nouvelles"], r["changees"]) == (10, 10, 0)


def test_chargement_identique_insere_zero(conn):
    ensure_schema(conn)
    charger_etats(conn, etats(), TS[0])
    assert charger_etats(conn, etats(), TS[1])["inserees"] == 0
    assert total(conn) == 10


def test_seules_les_stations_modifiees_sont_versionnees(conn):
    ensure_schema(conn)
    charger_etats(conn, etats(), TS[0])
    r = charger_etats(conn, etats(modifies=(2, 3)), TS[1])
    assert (r["inserees"], r["changees"]) == (2, 2)
    assert total(conn) == 12


def test_retour_a_un_etat_precedent_cree_une_nouvelle_version(conn):
    ensure_schema(conn)
    charger_etats(conn, etats(), TS[0])
    charger_etats(conn, etats(modifies=(0,)), TS[1])
    charger_etats(conn, etats(), TS[2])
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM station_etat_flux WHERE station_id = 0")
        assert cur.fetchone()[0] == 3


def test_nouvelle_station_n_est_pas_un_changement(conn):
    ensure_schema(conn)
    charger_etats(conn, etats(), TS[0])
    r = charger_etats(conn, etats(extra=2), TS[1])
    assert (r["inserees"], r["nouvelles"], r["changees"]) == (2, 2, 0)


def test_garde_fou_changement_massif_leve_et_n_ecrit_rien(conn):
    ensure_schema(conn)
    charger_etats(conn, etats(), TS[0])
    with pytest.raises(EtatsInstablesError, match="adresse"):
        charger_etats(conn, etats(adresse="champ volatil"), TS[1])
    assert total(conn) == 10
