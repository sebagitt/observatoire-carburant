import datetime as dt
import hashlib
import json

import pandas as pd
import pytest

from ingestion.ref_station_commune import (
    URL_QUOTIDIEN,
    charger_station_commune,
    lire_quotidien,
    telecharger_quotidien,
)
from ingestion.referentiels import ErreurReferentiel

# Extrait fixe au format de l'export JSON du jeu prix-carburants-quotidien (select=id,com_arm_code,prix_maj) :
# une ligne par (station, carburant), le code commune répété sur chaque ligne.
EXTRAIT = [
    {"id": 1000001, "com_arm_code": "01001", "prix_maj": "2026-11-16T07:12:00+00:00"},
    {"id": 1000001, "com_arm_code": "01001", "prix_maj": "2026-11-15T07:12:00+00:00"},
    {"id": 20000001, "com_arm_code": "2A004", "prix_maj": "2026-11-16T23:59:00+00:00"},
    {"id": 13001001, "com_arm_code": " 13201 ", "prix_maj": None},  # arrondissement, espaces retirés
    {"id": 5000001, "com_arm_code": None, "prix_maj": "2026-11-10T08:00:00+00:00"},  # station sans code
    {"id": 5000001, "com_arm_code": "", "prix_maj": "2026-11-10T08:00:00+00:00"},
    {"id": 6000001, "com_arm_code": "", "prix_maj": "2026-11-12T08:00:00+00:00"},  # code sur une ligne seulement
    {"id": 6000001, "com_arm_code": "06088", "prix_maj": "2026-11-12T08:00:00+00:00"},
    {"id": None, "com_arm_code": "01001", "prix_maj": "2026-11-16T07:12:00+00:00"},  # sans identifiant : ignorée
]


def ecrire(chemin, lignes=EXTRAIT):
    chemin.write_text(json.dumps(lignes), encoding="utf-8")
    return chemin


def valide(df):
    return {"success": True, "echecs": []}


def lignes_table(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT station_id, com_arm_code, fichier_date FROM ref_station_commune ORDER BY station_id")
        return cur.fetchall()


# --- lecture --------------------------------------------------------------------------------------------------------


def test_lecture_une_ligne_par_station_code_nettoye_et_vide_a_nul(tmp_path):
    df, fichier_date = lire_quotidien(ecrire(tmp_path / "q.json"))
    assert list(df.columns) == ["station_id", "com_arm_code"]
    obtenu = dict(zip(df["station_id"], df["com_arm_code"]))
    assert obtenu == {1000001: "01001", 5000001: None, 6000001: "06088", 13001001: "13201", 20000001: "2A004"}
    assert list(df["station_id"]) == sorted(obtenu)


def test_lecture_date_du_fichier_est_le_jour_utc_du_dernier_prix_maj(tmp_path):
    _, fichier_date = lire_quotidien(ecrire(tmp_path / "q.json"))
    assert fichier_date == dt.date(2026, 11, 16)


def test_lecture_refuse_un_fichier_sans_aucun_prix_maj(tmp_path):
    with pytest.raises(ErreurReferentiel, match="prix_maj"):
        lire_quotidien(ecrire(tmp_path / "q.json", [{"id": 1, "com_arm_code": "01001", "prix_maj": None}]))


def test_plusieurs_codes_pour_une_station_echec_explicite_qui_nomme_la_station(tmp_path):
    lignes = EXTRAIT + [{"id": 1000001, "com_arm_code": "01002", "prix_maj": "2026-11-16T07:12:00+00:00"}]
    with pytest.raises(ErreurReferentiel, match=r"plusieurs com_arm_code.*1000001"):
        lire_quotidien(ecrire(tmp_path / "q.json", lignes))


def test_lecture_refuse_un_contenu_qui_n_est_pas_une_liste(tmp_path):
    (tmp_path / "q.json").write_text('{"error_code": "x"}', encoding="utf-8")
    with pytest.raises(ErreurReferentiel, match="liste"):
        lire_quotidien(tmp_path / "q.json")


# --- chargement (schéma temporaire isolé, jamais la vraie table) ---------------------------------------------------------


def test_chargement_ecrit_les_lignes_et_la_date(conn, tmp_path):
    df, fichier_date = lire_quotidien(ecrire(tmp_path / "q.json"))
    assert charger_station_commune(conn, df, fichier_date, validateur=valide) == 5
    assert lignes_table(conn)[:2] == [(1000001, "01001", dt.date(2026, 11, 16)), (5000001, None, dt.date(2026, 11, 16))]
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM ref_station_commune WHERE charge_ts IS NULL")
        assert cur.fetchone()[0] == 0


def test_remplacement_complet_idempotent(conn, tmp_path):
    df, fichier_date = lire_quotidien(ecrire(tmp_path / "q.json"))
    charger_station_commune(conn, df, fichier_date, validateur=valide)
    premier = lignes_table(conn)
    charger_station_commune(conn, df, fichier_date, validateur=valide)
    assert lignes_table(conn) == premier and len(premier) == 5
    reduit = pd.DataFrame({"station_id": [1000001], "com_arm_code": ["01001"]})
    charger_station_commune(conn, reduit, dt.date(2026, 11, 17), validateur=valide)
    assert lignes_table(conn) == [(1000001, "01001", dt.date(2026, 11, 17))]  # stations disparues retirées


def test_validation_en_echec_n_ecrit_rien_et_garde_l_ancien_chargement(conn, tmp_path):
    df, fichier_date = lire_quotidien(ecrire(tmp_path / "q.json"))
    charger_station_commune(conn, df, fichier_date, validateur=valide)

    def refuse(df):
        return {"success": False, "echecs": [{"expectation": "expect_column_values_to_match_regex", "colonne": "com_arm_code"}]}

    with pytest.raises(ErreurReferentiel, match="validation"):
        charger_station_commune(conn, df.iloc[:1], fichier_date, validateur=refuse)
    assert len(lignes_table(conn)) == 5


def test_echec_a_l_insertion_laisse_la_table_precedente_intacte(conn, tmp_path):
    df, fichier_date = lire_quotidien(ecrire(tmp_path / "q.json"))
    charger_station_commune(conn, df, fichier_date, validateur=valide)
    avant = lignes_table(conn)
    # doublon de station_id : le DELETE passe, l'INSERT viole la clé primaire
    casse = pd.DataFrame({"station_id": [7000001, 7000001], "com_arm_code": ["01001", "01001"]})
    with pytest.raises(Exception, match="duplicate key|unique"):
        charger_station_commune(conn, casse, dt.date(2026, 11, 17), validateur=valide)
    conn.rollback()
    assert lignes_table(conn) == avant and len(avant) == 5


def test_main_affiche_le_fichier_choisi_et_sa_date(tmp_path, monkeypatch, capsys):
    import ingestion.db
    import ingestion.ref_station_commune as module

    dossier = tmp_path / "station_commune"
    dossier.mkdir()
    ecrire(dossier / "quotidien_20261116T080000Z.json", [{"id": 1, "com_arm_code": "01001", "prix_maj": "2026-11-15T10:00:00+00:00"}])
    recent = ecrire(dossier / "quotidien_20261117T080000Z.json")
    monkeypatch.setenv("REFERENTIELS_DIR", str(tmp_path))
    monkeypatch.setattr(ingestion.db, "connecter", lambda: type("Faux", (), {"close": lambda self: None})())
    recus = []
    monkeypatch.setattr(module, "charger_station_commune", lambda conn, df, d: recus.append((len(df), d)) or len(df))
    assert module.main([]) == 0
    sortie = capsys.readouterr().out
    assert f"fichier choisi : {recent}" in sortie
    assert "fichier_date : 2026-11-16" in sortie
    assert recus == [(5, dt.date(2026, 11, 16))]


def test_validateur_par_defaut_est_la_suite_gx(conn):
    petit = pd.DataFrame({"station_id": [1], "com_arm_code": ["01001"]})  # sous le minimum de 9 500
    with pytest.raises(ErreurReferentiel, match="expect_table_row_count_to_be_between"):
        charger_station_commune(conn, petit, dt.date(2026, 11, 16))


# --- téléchargement (aucun réseau : la fonction d'obtention est injectée) ----------------------------------------------


def test_telechargement_ecrit_le_fichier_et_donne_taille_et_empreinte(tmp_path):
    contenu = json.dumps(EXTRAIT).encode()
    appels = []

    def obtenir(url, params):
        appels.append((url, params))
        return contenu

    chemin, octets, empreinte = telecharger_quotidien(tmp_path / "station_commune", obtenir=obtenir)
    assert chemin.read_bytes() == contenu and chemin.parent == tmp_path / "station_commune"
    assert (octets, empreinte) == (len(contenu), hashlib.sha256(contenu).hexdigest())
    assert appels == [(URL_QUOTIDIEN, {"select": "id,com_arm_code,prix_maj", "limit": -1})]
    assert URL_QUOTIDIEN == "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-carburants-quotidien/exports/json"
