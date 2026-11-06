import subprocess
import sys
import time

import pytest

from ingestion import bootstrap_databricks
from ingestion.bootstrap_databricks import (
    DELAI_CONNEXION,
    SCHEMAS_AUTORISES,
    lire_instructions,
    main,
    sans_secret,
    controler_connexion,
    verifier_perimetre,
)


def test_le_sql_cree_les_trois_schemas_et_un_volume_sans_catalogue():
    instructions = lire_instructions()
    assert len(instructions) == 4
    assert all("IF NOT EXISTS" in i.upper() for i in instructions)
    assert not any("CATALOG" in i.upper() for i in instructions)
    cree = verifier_perimetre(instructions)
    assert sorted(cree["schemas"]) == sorted(SCHEMAS_AUTORISES)
    assert cree["volumes"] == ["carburant_bronze.depot"]


@pytest.mark.parametrize("autre", ["bronze", "silver", "gold", "ci_silver", "ci_gold", "default", "information_schema"])
def test_aucun_schema_de_l_autre_projet_n_est_touche(autre):  # EnergiScope : bronze, silver, gold, ci_*, default
    for instruction in (f"CREATE SCHEMA IF NOT EXISTS {autre}", f"CREATE VOLUME IF NOT EXISTS {autre}.depot"):
        with pytest.raises(ValueError):
            verifier_perimetre([instruction])


@pytest.mark.parametrize(
    "instruction",
    [
        "DROP SCHEMA IF EXISTS carburant_bronze",
        "DROP SCHEMA carburant_bronze CASCADE",
        "CREATE CATALOG IF NOT EXISTS workspace",
        "CREATE SCHEMA carburant_bronze",
        "ALTER SCHEMA carburant_gold SET OWNER TO `x`",
        "GRANT ALL PRIVILEGES ON SCHEMA carburant_gold TO `x`",
        "CREATE SCHEMA IF NOT EXISTS workspace.carburant_bronze",
        "USE CATALOG workspace",
    ],
)
def test_toute_autre_instruction_est_refusee(instruction):
    with pytest.raises(ValueError):
        verifier_perimetre([instruction])


def test_lecture_ignore_commentaires_et_normalise_les_espaces(tmp_path):
    fichier = tmp_path / "x.sql"
    fichier.write_text("-- un commentaire ; avec point-virgule\nCREATE   SCHEMA IF NOT EXISTS carburant_gold; -- fin\n", encoding="utf-8")
    assert lire_instructions(fichier) == ["CREATE SCHEMA IF NOT EXISTS carburant_gold"]


def test_aucun_secret_en_sortie():
    env = {"DATABRICKS_HOST": "https://adb-1.example", "DATABRICKS_TOKEN": "jeton-secret", "AUTRE": "ok"}
    sortie = sans_secret("échec vers https://adb-1.example avec jeton-secret et ok", env)
    assert "jeton-secret" not in sortie and "adb-1.example" not in sortie and "ok" in sortie
    assert sans_secret("rien", {}) == "rien"


ENV = {"DATABRICKS_HOST": "https://hote-factice.invalid", "DATABRICKS_TOKEN": "jeton-factice-123"}


def enfant(code):
    return [sys.executable, "-c", code]


def test_delai_dur_par_defaut_de_30_secondes():
    assert DELAI_CONNEXION == 30


def test_ko_sans_rien_lancer_si_une_variable_manque(monkeypatch):
    def interdit(*args, **kwargs):
        raise AssertionError("aucun processus ne doit être lancé")

    monkeypatch.setattr(subprocess, "run", interdit)
    assert controler_connexion({"DATABRICKS_HOST": "https://x"}) == "KO"
    assert controler_connexion({}) == "KO"


def test_ok_si_le_processus_enfant_reussit():
    assert controler_connexion(ENV, commande=enfant("raise SystemExit(0)")) == "OK"


def test_ko_si_le_processus_enfant_echoue():
    assert controler_connexion(ENV, commande=enfant("raise SystemExit(3)")) == "KO"


def test_delai_dur_tue_un_processus_enfant_bloque():
    debut = time.monotonic()
    assert controler_connexion(ENV, commande=enfant("import time; time.sleep(60)"), delai=1) == "KO"
    assert time.monotonic() - debut < 10  # sans délai dur, ce test durerait 60 s


def test_aucun_secret_ni_sortie_de_l_enfant(capfd):
    code = "import os, sys; v = os.environ['DATABRICKS_TOKEN']; print(v); print(v, file=sys.stderr); raise SystemExit(1)"
    resultat = controler_connexion(ENV, commande=enfant(code))
    sorties = capfd.readouterr()
    assert resultat == "KO"
    assert sorties.out == "" and sorties.err == ""


def test_main_n_affiche_que_ok_ou_ko(monkeypatch, capsys):
    monkeypatch.setattr(bootstrap_databricks, "controler_connexion", lambda env: "OK")
    main(["--connexion"])
    assert capsys.readouterr().out == "OK\n"
    monkeypatch.setattr(bootstrap_databricks, "controler_connexion", lambda env: "KO")
    main(["--connexion"])
    assert capsys.readouterr().out == "KO\n"
