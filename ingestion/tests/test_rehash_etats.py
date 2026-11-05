import hashlib
import json

import pandas as pd
import pytest
from psycopg2.extras import Json

from ingestion.carburants import construire_etats, hash_etat
from ingestion.db import EtatsInstablesError, charger_etats, ensure_schema
from ingestion.rehash import SAUVEGARDE, SauvegardeExistanteError, prevoir_changements, recalculer_hash

TS = [pd.Timestamp(f"2026-10-23T0{h}:00:00Z").to_pydatetime() for h in range(1, 6)]
SERVICES = '{"service": ["Bar", "Lavage automatique"]}'
SERVICES_REORDONNES = '{"service": ["Lavage automatique", "Bar"]}'


def ancien_hash(etat):
    """Règle d'avant la canonisation : la chaîne JSON est hachée telle quelle."""
    return hashlib.sha256(json.dumps(etat, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def inserer(conn, station_id, ts, etat):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO station_etat_flux (station_id, collecte_ts, etat_hash, etat) VALUES (%s, %s, %s, %s)",
            (station_id, ts, ancien_hash(etat), Json(etat)),
        )


def peupler(conn, n=3, services=SERVICES):
    ensure_schema(conn)
    for i in range(n):
        inserer(conn, i, TS[0], {"adresse": f"{i} rue A", "services": services})
    conn.commit()


def hashs(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT station_id, collecte_ts, etat_hash FROM station_etat_flux ORDER BY 1, 2")
        return cur.fetchall()


def sauvegarde_existe(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (SAUVEGARDE,))
        return cur.fetchone()[0] is not None


def stations(n=10, modifies=(), services=SERVICES):
    return [
        {"id": i, "adresse": "autre rue" if i in modifies else f"{i} rue A", "services": services}
        for i in range(n)
    ]


def test_simulation_compte_sans_rien_ecrire(conn):
    peupler(conn)
    inserer(conn, 0, TS[1], {"adresse": "0 rue A", "services": SERVICES_REORDONNES})  # même état, autre ordre
    conn.commit()
    avant = hashs(conn)
    r = recalculer_hash(conn)
    assert (r["lignes"], r["a_modifier"], r["ecrites"]) == (4, 4, 0)
    assert r["doublons_consecutifs"] == 1
    assert hashs(conn) == avant
    assert not sauvegarde_existe(conn)


def test_ecriture_met_a_jour_le_hash_sans_toucher_etat_et_sauvegarde_l_ancien(conn):
    peupler(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT station_id, collecte_ts, etat_hash, etat FROM station_etat_flux ORDER BY 1, 2")
        avant = cur.fetchall()
    r = recalculer_hash(conn, confirmer=True)
    assert (r["a_modifier"], r["ecrites"]) == (3, 3)
    with conn.cursor() as cur:
        cur.execute("SELECT station_id, collecte_ts, etat_hash, etat FROM station_etat_flux ORDER BY 1, 2")
        apres = cur.fetchall()
        cur.execute(f"SELECT station_id, collecte_ts, ancien_hash FROM {SAUVEGARDE} ORDER BY 1, 2")
        sauvegarde = cur.fetchall()
    assert [(a[0], a[1], a[3]) for a in apres] == [(a[0], a[1], a[3]) for a in avant]
    assert all(a[2] == hash_etat(a[3]) for a in apres)
    assert sauvegarde == [(a[0], a[1], a[2]) for a in avant]


def test_rejeu_n_ecrit_rien_et_garde_la_sauvegarde(conn):
    peupler(conn)
    recalculer_hash(conn, confirmer=True)
    apres_premier = hashs(conn)
    r = recalculer_hash(conn, confirmer=True)
    assert (r["a_modifier"], r["ecrites"]) == (0, 0)
    assert hashs(conn) == apres_premier
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {SAUVEGARDE}")
        assert cur.fetchone()[0] == 3


def test_ecriture_refusee_si_une_sauvegarde_existe_deja(conn):
    peupler(conn)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {SAUVEGARDE} (station_id BIGINT, collecte_ts TIMESTAMPTZ, ancien_hash TEXT)")
    conn.commit()
    avant = hashs(conn)
    with pytest.raises(SauvegardeExistanteError):
        recalculer_hash(conn, confirmer=True)
    assert hashs(conn) == avant


def test_prevision_compte_les_stations_qui_changeraient_au_prochain_passage(conn):
    peupler(conn, n=10)
    lot = construire_etats(stations(modifies=(1, 2), services=SERVICES_REORDONNES))
    assert prevoir_changements(conn, lot) == {"connues": 10, "changees": 2}


def test_garde_fou_intact_apres_recalcul(conn):
    peupler(conn, n=10)
    recalculer_hash(conn, confirmer=True)
    # 100 % de stations avec seulement l'ordre des services modifié : plus de faux changements.
    r = charger_etats(conn, construire_etats(stations(services=SERVICES_REORDONNES)), TS[1])
    assert (r["changees"], r["inserees"]) == (0, 0)
    # 40 % de vrais changements : le garde-fou lève toujours et n'écrit rien.
    with pytest.raises(EtatsInstablesError):
        charger_etats(conn, construire_etats(stations(modifies=(0, 1, 2, 3), services=SERVICES_REORDONNES)), TS[2])
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM station_etat_flux")
        assert cur.fetchone()[0] == 10
