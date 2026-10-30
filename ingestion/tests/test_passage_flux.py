import pandas as pd

from ingestion.db import ensure_schema, enregistrer_passage, journaliser_passage

TS1 = pd.Timestamp("2026-10-30T12:45:05Z").to_pydatetime()
RUN = pd.Timestamp("2026-10-30T12:45:00Z").to_pydatetime()


def lignes(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT collecte_ts, run_planifie, lignes_lues, lignes_inserees, etats_changes FROM passage_flux"
        )
        return cur.fetchall()


def test_table_creee_et_schema_rejouable(conn):
    ensure_schema(conn)
    ensure_schema(conn)
    assert lignes(conn) == []


def test_ecrit_les_valeurs_du_passage(conn):
    ensure_schema(conn)
    assert enregistrer_passage(conn, TS1, RUN, 31190, 107, 12) is True
    assert lignes(conn) == [(TS1, RUN, 31190, 107, 12)]


def test_rejeu_du_meme_passage_garde_la_premiere_ecriture(conn):
    ensure_schema(conn)
    enregistrer_passage(conn, TS1, RUN, 31190, 107, 12)
    assert enregistrer_passage(conn, TS1, RUN, 31190, 0, 0) is True
    assert lignes(conn) == [(TS1, RUN, 31190, 107, 12)]


def test_run_planifie_et_etats_peuvent_etre_absents(conn):
    ensure_schema(conn)
    assert enregistrer_passage(conn, TS1, None, 31190, 0, None) is True
    assert lignes(conn) == [(TS1, None, 31190, 0, None)]


def test_echec_d_ecriture_ne_leve_pas_et_laisse_la_connexion_utilisable(conn):
    ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE passage_flux")
    conn.commit()
    assert enregistrer_passage(conn, TS1, RUN, 31190, 107, 12) is False
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)


def test_connexion_impossible_ne_leve_pas():
    dsn = "postgresql://nul:nul@127.0.0.1:1/nul"
    assert journaliser_passage(TS1, RUN, 31190, 107, 12, dsn=dsn) is False
