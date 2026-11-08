import csv
import hashlib
import shutil

import pytest

from ingestion.referentiels import (
    COLONNES_COMMUNE,
    COLONNES_DEPARTEMENT,
    COLONNES_REGION,
    ErreurReferentiel,
    charger_cog,
    lire_csv,
    telecharger_cog,
)


def ecrire(chemin, colonnes, lignes, bom=False):
    """Écrit un CSV au format du COG mesuré : virgule, tout entre guillemets, fins de ligne CRLF, UTF-8."""
    with open(chemin, "w", encoding="utf-8-sig" if bom else "utf-8", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
        w.writerow(colonnes)
        w.writerows(lignes)


COMMUNES = [
    ["COM", "01001", "84", "01", "01D", "012", "5", "ABERGEMENT CLEMENCIAT", "Abergement-Clémenciat", "L'Abergement-Clémenciat", "0108", ""],
    ["COMD", "01015", "", "", "", "", "1", "ARBIGNIEU", "Arbignieu", "Arbignieu", "", "01015"],
    ["COM", "2A004", "94", "2A", "2AD", "2A1", "0", "AJACCIO", "Ajaccio", "Ajaccio", "2A01", ""],
]
DEPARTEMENTS = [["01", "84", "01053", "5", "AIN", "Ain", "Ain"], ["2A", "94", "2A004", "0", "CORSE DU SUD", "Corse-du-Sud", "Corse-du-Sud"]]
REGIONS = [["84", "69123", "1", "AUVERGNE RHONE ALPES", "Auvergne-Rhône-Alpes", "Auvergne-Rhône-Alpes"], ["94", "2A004", "0", "CORSE", "Corse", "Corse"]]


@pytest.fixture
def dossier(tmp_path):
    ecrire(tmp_path / "v_commune_2026.csv", COLONNES_COMMUNE, COMMUNES)
    ecrire(tmp_path / "v_departement_2026.csv", COLONNES_DEPARTEMENT, DEPARTEMENTS)
    ecrire(tmp_path / "v_region_2026.csv", COLONNES_REGION, REGIONS)
    return tmp_path


def valide(df):
    return {"success": True, "echecs": []}


def compter(conn, table, millesime=None):
    with conn.cursor() as cur:
        if millesime is None:
            cur.execute(f"SELECT count(*) FROM {table}")
        else:
            cur.execute(f"SELECT count(*) FROM {table} WHERE millesime = %s", (millesime,))
        return cur.fetchone()[0]


# --- lecture --------------------------------------------------------------------------------------------------------


def test_lecture_garde_les_zeros_de_tete_la_corse_et_les_accents_et_les_vides_sont_nuls(dossier):
    df = lire_csv(dossier / "v_commune_2026.csv", COLONNES_COMMUNE)
    assert list(df["COM"]) == ["01001", "01015", "2A004"]
    assert df.loc[0, "LIBELLE"] == "L'Abergement-Clémenciat"
    assert df.loc[1, "REG"] is None and df.loc[1, "COMPARENT"] == "01015"
    assert df.loc[0, "COMPARENT"] is None


def test_lecture_accepte_un_bom_utf8(tmp_path):
    ecrire(tmp_path / "v_region_2026.csv", COLONNES_REGION, REGIONS, bom=True)
    assert list(lire_csv(tmp_path / "v_region_2026.csv", COLONNES_REGION)["REG"]) == ["84", "94"]


def test_lecture_refuse_une_colonne_manquante_ou_en_trop(tmp_path):
    ecrire(tmp_path / "a.csv", COLONNES_REGION[:-1], [r[:-1] for r in REGIONS])
    with pytest.raises(ErreurReferentiel, match="colonnes"):
        lire_csv(tmp_path / "a.csv", COLONNES_REGION)
    ecrire(tmp_path / "b.csv", COLONNES_REGION + ["EXTRA"], [r + ["x"] for r in REGIONS])
    with pytest.raises(ErreurReferentiel, match="colonnes"):
        lire_csv(tmp_path / "b.csv", COLONNES_REGION)


# --- chargement (schéma temporaire isolé, jamais la vraie table) ---------------------------------------------------------


def test_chargement_ecrit_les_trois_tables_avec_le_millesime(conn, dossier):
    comptes = charger_cog(conn, dossier, 2026, validateur=valide)
    assert comptes == {"ref_cog_commune": 3, "ref_cog_departement": 2, "ref_cog_region": 2}
    assert compter(conn, "ref_cog_commune", 2026) == 3
    with conn.cursor() as cur:
        cur.execute("SELECT typecom, com, dep, comparent FROM ref_cog_commune WHERE com = '01015'")
        assert cur.fetchone() == ("COMD", "01015", None, "01015")


def test_chargement_rejoue_est_idempotent(conn, dossier):
    charger_cog(conn, dossier, 2026, validateur=valide)
    charger_cog(conn, dossier, 2026, validateur=valide)
    assert compter(conn, "ref_cog_commune") == 3 and compter(conn, "ref_cog_departement") == 2 and compter(conn, "ref_cog_region") == 2


def test_deux_millesimes_coexistent_et_le_rejeu_de_l_un_ne_touche_pas_l_autre(conn, dossier):
    for nom in ("v_commune", "v_departement", "v_region"):
        shutil.copy(dossier / f"{nom}_2026.csv", dossier / f"{nom}_2025.csv")
    charger_cog(conn, dossier, 2025, validateur=valide)
    charger_cog(conn, dossier, 2026, validateur=valide)
    charger_cog(conn, dossier, 2026, validateur=valide)
    assert compter(conn, "ref_cog_commune", 2025) == 3 and compter(conn, "ref_cog_commune", 2026) == 3


def test_validation_en_echec_n_ecrit_rien(conn, dossier):
    def refuse(df):
        return {"success": False, "echecs": [{"expectation": "expect_x", "colonne": "COM"}]}

    with pytest.raises(ErreurReferentiel, match="validation"):
        charger_cog(conn, dossier, 2026, validateur=refuse)
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('ref_cog_commune')")
        assert cur.fetchone()[0] is None or compter(conn, "ref_cog_commune") == 0


def test_echec_en_cours_de_chargement_annule_tout_y_compris_la_suppression_du_millesime(conn, dossier):
    charger_cog(conn, dossier, 2026, validateur=valide)
    ecrire(dossier / "v_commune_2026.csv", COLONNES_COMMUNE, COMMUNES + [COMMUNES[0]])  # doublon (TYPECOM, COM)
    with pytest.raises(Exception):
        charger_cog(conn, dossier, 2026, validateur=valide)
    conn.rollback()
    assert compter(conn, "ref_cog_commune", 2026) == 3  # l'ancien chargement est intact


# --- téléchargement (aucun réseau : la fonction d'obtention est injectée) ----------------------------------------------


def test_telechargement_ecrit_les_fichiers_et_donne_taille_et_empreinte(tmp_path):
    contenus = {"v_commune_2026.csv": b'"COM"\r\n"01001"\r\n', "v_departement_2026.csv": b'"DEP"\r\n', "v_region_2026.csv": b'"REG"\r\n'}
    appels = []

    def obtenir(url):
        appels.append(url)
        return contenus[url.rsplit("/", 1)[1]]

    resultat = telecharger_cog(tmp_path / "cog" / "2026", obtenir=obtenir)
    assert sorted(resultat) == sorted(contenus)
    for nom, contenu in contenus.items():
        assert (tmp_path / "cog" / "2026" / nom).read_bytes() == contenu
        assert resultat[nom] == (len(contenu), hashlib.sha256(contenu).hexdigest())
    assert all(u.startswith("https://www.insee.fr/fr/statistiques/fichier/8740222/") for u in appels)
