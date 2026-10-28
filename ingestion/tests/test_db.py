import pandas as pd

from ingestion.db import charger_prix, ensure_schema

TS1 = pd.Timestamp("2026-10-22T08:15:00Z").to_pydatetime()
TS2 = pd.Timestamp("2026-10-22T08:30:00Z").to_pydatetime()


def lot(prix=1.959):
    return pd.DataFrame(
        {
            "station_id": [1, 1, 2],
            "carburant": ["gazole", "sp95", "gazole"],
            "maj": pd.to_datetime(["2026-10-22T08:00Z"] * 3),
            "prix": [prix, 1.99, 1.90],
        }
    )


def compter(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM prix_flux")
        return cur.fetchone()[0]


def test_schema_est_rejouable(conn):
    ensure_schema(conn)
    ensure_schema(conn)
    assert compter(conn) == 0


def test_deux_executions_consecutives_zero_doublon(conn):
    ensure_schema(conn)
    r1 = charger_prix(conn, lot(), TS1)
    r2 = charger_prix(conn, lot(), TS2)
    assert (r1["inserees"], r2["inserees"]) == (3, 0)
    assert r2["deja_vues"] == 3
    assert compter(conn) == 3


def test_nouvelle_maj_est_inseree(conn):
    ensure_schema(conn)
    charger_prix(conn, lot(), TS1)
    nouveau = lot()
    nouveau["maj"] = pd.to_datetime(["2026-10-22T08:20Z"] * 3)
    assert charger_prix(conn, nouveau, TS2)["inserees"] == 3
    assert compter(conn) == 6


def test_collecte_ts_du_premier_passage_est_conserve(conn):
    ensure_schema(conn)
    charger_prix(conn, lot(), TS1)
    charger_prix(conn, lot(), TS2)
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT collecte_ts FROM prix_flux")
        assert [r[0] for r in cur.fetchall()] == [TS1]


def test_prix_corrige_sans_nouvelle_maj_est_signale_pas_ecrase(conn):
    ensure_schema(conn)
    charger_prix(conn, lot(prix=1.959), TS1)
    r = charger_prix(conn, lot(prix=1.999), TS2)
    assert r["inserees"] == 0 and r["conflits_prix"] == 1
    with conn.cursor() as cur:
        cur.execute("SELECT prix FROM prix_flux WHERE station_id=1 AND carburant='gazole'")
        assert float(cur.fetchone()[0]) == 1.959


def test_doublon_dans_un_meme_lot_ne_plante_pas(conn):
    ensure_schema(conn)
    double = pd.concat([lot(), lot()], ignore_index=True)
    assert charger_prix(conn, double, TS1)["inserees"] == 3
