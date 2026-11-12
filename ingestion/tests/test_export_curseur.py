from datetime import datetime, timezone

import pytest

from ingestion.db import ensure_schema
from ingestion.export_curseur import (
    CurseurPerimeError,
    TABLES_EXPORTABLES,
    avancer_curseur,
    creer_table_curseur,
    initialiser_curseur,
    lire_curseur,
)

UTC = timezone.utc


def t(jour, heure, minute=0, seconde=0):
    return datetime(2026, 11, jour, heure, minute, seconde, tzinfo=UTC)


@pytest.fixture
def base(conn):
    """Schéma temporaire avec les tables sources et la table de curseur."""
    ensure_schema(conn)
    creer_table_curseur(conn)
    return conn


def inserer_prix(conn, collecte_ts, station_id=1):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO prix_flux (station_id, carburant, maj, prix, collecte_ts) "
            "VALUES (%s, 'Gazole', %s, 1.234, %s)",
            (station_id, collecte_ts, collecte_ts),
        )
    conn.commit()


def inserer_etat(conn, collecte_ts, station_id=1):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO station_etat_flux (station_id, collecte_ts, etat_hash, etat) "
            "VALUES (%s, %s, 'h', '{}'::jsonb)",
            (station_id, collecte_ts),
        )
    conn.commit()


def lignes_curseur(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT table_source, source, curseur FROM export_curseur ORDER BY 1, 2")
        return cur.fetchall()


def test_liste_blanche_des_tables():
    assert set(TABLES_EXPORTABLES) == {"prix_flux", "station_etat_flux"}


def test_ensure_schema_seul_ne_cree_pas_export_curseur(conn):
    ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = current_schema() AND table_name = 'export_curseur'"
        )
        assert cur.fetchone()[0] == 0


def test_creer_table_curseur_est_rejouable(conn):
    creer_table_curseur(conn)
    creer_table_curseur(conn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = current_schema() AND table_name = 'export_curseur'"
        )
        assert cur.fetchone()[0] == 1


def test_lire_curseur_sans_ligne_renvoie_none(base):
    assert lire_curseur(base, "prix_flux", "instantane") is None


def test_initialiser_sur_table_vide_ne_fait_rien(base):
    assert initialiser_curseur(base, "prix_flux", "instantane") is None
    assert lignes_curseur(base) == []


def test_initialiser_plancher_a_l_heure_utc_du_minimum(base):
    inserer_prix(base, t(12, 10, 37, 12))
    inserer_prix(base, t(12, 9, 59, 59), station_id=2)
    inserer_prix(base, t(12, 11, 5), station_id=3)
    assert initialiser_curseur(base, "prix_flux", "instantane") == t(12, 9)
    assert lire_curseur(base, "prix_flux", "instantane") == t(12, 9)


def test_initialiser_etats_utilise_la_table_des_etats(base):
    inserer_prix(base, t(1, 1))
    inserer_etat(base, t(12, 10, 37))
    assert initialiser_curseur(base, "station_etat_flux", "instantane") == t(12, 10)


def test_initialiser_ne_modifie_jamais_une_ligne_existante(base):
    inserer_prix(base, t(12, 10, 37))
    initialiser_curseur(base, "prix_flux", "instantane")
    inserer_prix(base, t(11, 3, 0), station_id=2)
    assert initialiser_curseur(base, "prix_flux", "instantane") == t(12, 10)
    assert lire_curseur(base, "prix_flux", "instantane") == t(12, 10)
    assert len(lignes_curseur(base)) == 1


def test_une_ligne_par_table_et_par_source(base):
    inserer_prix(base, t(12, 10, 37))
    inserer_etat(base, t(12, 8, 1))
    initialiser_curseur(base, "prix_flux", "instantane")
    initialiser_curseur(base, "prix_flux", "annuel")
    initialiser_curseur(base, "station_etat_flux", "instantane")
    assert lignes_curseur(base) == [
        ("prix_flux", "annuel", t(12, 10)),
        ("prix_flux", "instantane", t(12, 10)),
        ("station_etat_flux", "instantane", t(12, 8)),
    ]


@pytest.mark.parametrize("table", ["passage_flux", "export_curseur", "prix_flux; DROP TABLE prix_flux", ""])
def test_table_hors_liste_blanche_refusee(base, table):
    with pytest.raises(ValueError):
        lire_curseur(base, table, "instantane")
    with pytest.raises(ValueError):
        initialiser_curseur(base, table, "instantane")
    with pytest.raises(ValueError):
        avancer_curseur(base, table, "instantane", t(12, 10), t(12, 11))


def amorcer(base):
    inserer_prix(base, t(12, 10, 37))
    initialiser_curseur(base, "prix_flux", "instantane")  # curseur = 10:00


def test_avancer_met_a_jour_et_valide_la_transaction(base):
    amorcer(base)
    avancer_curseur(base, "prix_flux", "instantane", t(12, 10), t(12, 12))
    base.rollback()  # ce qui n'est pas validé disparaît ici
    assert lire_curseur(base, "prix_flux", "instantane") == t(12, 12)
    with base.cursor() as cur:
        cur.execute("SELECT mis_a_jour IS NOT NULL FROM export_curseur")
        assert cur.fetchone()[0] is True


def test_avancer_avec_un_ancien_perime_est_refuse(base):
    amorcer(base)
    avancer_curseur(base, "prix_flux", "instantane", t(12, 10), t(12, 12))
    with pytest.raises(CurseurPerimeError):
        avancer_curseur(base, "prix_flux", "instantane", t(12, 10), t(12, 13))
    assert lire_curseur(base, "prix_flux", "instantane") == t(12, 12)


def test_avancer_une_ligne_absente_est_refuse(base):
    with pytest.raises(CurseurPerimeError):
        avancer_curseur(base, "prix_flux", "instantane", t(12, 10), t(12, 11))


@pytest.mark.parametrize("nouveau", [t(12, 9), t(12, 10)])
def test_avancer_en_arriere_ou_sur_place_est_refuse(base, nouveau):
    amorcer(base)
    with pytest.raises(ValueError):
        avancer_curseur(base, "prix_flux", "instantane", t(12, 10), nouveau)
    assert lire_curseur(base, "prix_flux", "instantane") == t(12, 10)


def test_avancer_vers_un_horaire_non_aligne_est_refuse(base):
    amorcer(base)
    with pytest.raises(ValueError):
        avancer_curseur(base, "prix_flux", "instantane", t(12, 10), t(12, 11, 30))
    assert lire_curseur(base, "prix_flux", "instantane") == t(12, 10)


def test_avancer_sans_fuseau_est_refuse(base):
    amorcer(base)
    with pytest.raises(ValueError):
        avancer_curseur(base, "prix_flux", "instantane", t(12, 10), datetime(2026, 11, 12, 12))
    with pytest.raises(ValueError):
        avancer_curseur(base, "prix_flux", "instantane", datetime(2026, 11, 12, 10), t(12, 12))
