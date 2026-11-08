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
    charger_filosofi,
    lire_csv,
    lire_filosofi,
    telecharger_cog,
    telecharger_filosofi,
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


# --- Filosofi : fichier long, point-virgule, dans un zip -------------------------------------------------------------------

ENTETE_FILOSOFI = ["GEO", "GEO_OBJECT", "FILOSOFI_MEASURE", "UNIT_MULT", "UNIT_MEASURE", "CONF_STATUS", "OBS_STATUS", "TIME_PERIOD", "OBS_VALUE"]


def ligne_f(geo, niveau, mesure, valeur, obs="A", conf="F", periode="2023"):
    return [geo, niveau, mesure, "0", "EUR_YR", conf, obs, periode, valeur]


LIGNES_FILOSOFI = [
    ligne_f("01001", "COM", "MED_SL", "26400"),
    ligne_f("01001", "COM", "PR_MD60", ""),  # confidentiel : statut normal, valeur vide
    ligne_f("15031", "COM", "MED_SL", "", obs="O"),  # valeur manquante
    ligne_f("15031", "COM", "PR_MD60", "", obs="O"),
    ligne_f("13201", "ARM", "MED_SL", "21300"),
    ligne_f("13201", "ARM", "PR_MD60", "28.1"),
    ligne_f("13055", "COM", "GI_SL", "0.32"),  # autre mesure : ignorée
    ligne_f("001", "AAV2020", "MED_SL", "30000"),  # autre niveau : ignoré
]


def ecrire_zip_filosofi(chemin, lignes=LIGNES_FILOSOFI, entete=ENTETE_FILOSOFI, millesime=2023):
    import zipfile

    corps = ";".join(f'"{c}"' for c in entete) + "\n"
    for ligne in lignes:
        champs = [f'"{v}"' if i != 7 else v for i, v in enumerate(ligne)]  # TIME_PERIOD n'est pas entre guillemets
        corps += ";".join(champs) + "\n"
    with zipfile.ZipFile(chemin, "w") as z:
        z.writestr(f"DS_FILOSOFI_CC_{millesime}_data.csv", corps)
        z.writestr(f"DS_FILOSOFI_CC_{millesime}_metadata.csv", '"COD_VAR";"LIB_VAR";"COD_MOD";"LIB_MOD";"GEO_OBJECT"\n')
    return chemin


def test_lire_filosofi_pivote_les_deux_mesures_et_garde_les_statuts(tmp_path):
    df = lire_filosofi(ecrire_zip_filosofi(tmp_path / "f.zip"), 2023).set_index(["GEO_OBJECT", "CODGEO"])
    assert sorted(df.index) == [("ARM", "13201"), ("COM", "01001"), ("COM", "15031")]  # autres mesures et autres niveaux écartés
    assert df.loc[("COM", "01001"), "MED_SL"] == 26400.0 and df.loc[("COM", "01001"), "PR_MD60"] is None
    assert df.loc[("COM", "01001"), "PR_MD60_STATUT"] == "A" and df.loc[("COM", "01001"), "MED_SL_CONF"] == "F"
    assert df.loc[("COM", "15031"), "MED_SL"] is None and df.loc[("COM", "15031"), "MED_SL_STATUT"] == "O"
    assert df.loc[("ARM", "13201"), "PR_MD60"] == 28.1


def test_lire_filosofi_refuse_un_entete_inattendu(tmp_path):
    zip_ = ecrire_zip_filosofi(tmp_path / "f.zip", entete=ENTETE_FILOSOFI[:-1], lignes=[l[:-1] for l in LIGNES_FILOSOFI])
    with pytest.raises(ErreurReferentiel, match="colonnes"):
        lire_filosofi(zip_, 2023)


def test_lire_filosofi_refuse_une_autre_periode(tmp_path):
    zip_ = ecrire_zip_filosofi(tmp_path / "f.zip", lignes=[ligne_f("01001", "COM", "MED_SL", "26400", periode="2021")])
    with pytest.raises(ErreurReferentiel, match="TIME_PERIOD"):
        lire_filosofi(zip_, 2023)


def test_lire_filosofi_refuse_une_valeur_non_numerique(tmp_path):
    zip_ = ecrire_zip_filosofi(tmp_path / "f.zip", lignes=[ligne_f("01001", "COM", "MED_SL", "26,4k")])
    with pytest.raises(ErreurReferentiel, match="OBS_VALUE"):
        lire_filosofi(zip_, 2023)


def test_lire_filosofi_refuse_un_zip_sans_fichier_de_donnees(tmp_path):
    import zipfile

    chemin = tmp_path / "vide.zip"
    with zipfile.ZipFile(chemin, "w") as z:
        z.writestr("autre.csv", "x")
    with pytest.raises(ErreurReferentiel, match="données"):
        lire_filosofi(chemin, 2023)


def test_chargement_filosofi_idempotent_et_millesimes_separes(conn, tmp_path):
    zip_ = ecrire_zip_filosofi(tmp_path / "f.zip")
    assert charger_filosofi(conn, zip_, 2023, validateur=valide) == {"ref_filosofi_commune": 3}
    charger_filosofi(conn, zip_, 2023, validateur=valide)
    assert compter(conn, "ref_filosofi_commune", 2023) == 3
    zip_2021 = ecrire_zip_filosofi(tmp_path / "f21.zip", millesime=2021, lignes=[ligne_f("01001", "COM", "MED_SL", "25000", periode="2021")])
    charger_filosofi(conn, zip_2021, 2021, validateur=valide)
    assert compter(conn, "ref_filosofi_commune", 2021) == 1 and compter(conn, "ref_filosofi_commune", 2023) == 3
    with conn.cursor() as cur:
        cur.execute("SELECT med_sl, pr_md60, med_sl_statut FROM ref_filosofi_commune WHERE millesime = 2023 AND codgeo = '15031'")
        assert cur.fetchone() == (None, None, "O")


def test_chargement_filosofi_refuse_par_la_validation_n_ecrit_rien(conn, tmp_path):
    def refuse(df):
        return {"success": False, "echecs": [{"expectation": "expect_x", "colonne": "MED_SL"}]}

    with pytest.raises(ErreurReferentiel, match="validation"):
        charger_filosofi(conn, ecrire_zip_filosofi(tmp_path / "f.zip"), 2023, validateur=refuse)
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('ref_filosofi_commune')")
        assert cur.fetchone()[0] is None


def test_telechargement_filosofi_ecrit_le_zip_et_donne_taille_et_empreinte(tmp_path):
    contenu = b"PK-faux-zip"
    appels = []

    def obtenir(url):
        appels.append(url)
        return contenu

    resultat = telecharger_filosofi(tmp_path / "filosofi" / "2023", obtenir=obtenir)
    assert resultat == {"FILOSOFI_CC_csv.zip": (len(contenu), hashlib.sha256(contenu).hexdigest())}
    assert (tmp_path / "filosofi" / "2023" / "FILOSOFI_CC_csv.zip").read_bytes() == contenu
    assert appels == ["https://www.insee.fr/fr/statistiques/fichier/8984752/FILOSOFI_CC_csv.zip"]
