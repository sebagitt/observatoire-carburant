"""Tâches dbt du futur DAG transformation_dbt : faux executer, fausse sortie, aucun dbt, aucun réseau, aucun jeton.

Bibliothèque standard seulement : tourne dans le conteneur (make test) et sur l'hôte (.venv).
"""
from types import SimpleNamespace

import pytest

from ingestion.taches_dbt import (
    DBT_EXE,
    PROJET_DBT,
    SELECTION_CHAINE,
    ErreurDbt,
    commande_dbt,
    environnement_dbt,
    lancer_dbt,
    masquer,
)

ENV = {
    "DATABRICKS_HOST": "https://hote-factice.invalid",
    "DATABRICKS_TOKEN": "jeton-factice-123",
    "DATABRICKS_CATALOG": "workspace",
    "DATABRICKS_WAREHOUSE_ID": "entrepot-factice-456",
}
SECRETS = ("hote-factice.invalid", "jeton-factice-123", "entrepot-factice-456")
MODELES = ["dim_station", "fct_ruptures", "fct_prix", "mart_prix_commune_jour"]


class FauxExecuter:
    """Remplace subprocess.run : garde les arguments reçus, rend un code et des sorties prévus."""

    def __init__(self, code=0, stdout="", stderr=""):
        self.resultat = SimpleNamespace(returncode=code, stdout=stdout, stderr=stderr)
        self.args, self.kwargs = None, None

    def __call__(self, *args, **kwargs):
        self.args, self.kwargs = args, kwargs
        return self.resultat


def sans_secret(texte):
    return not any(s in texte for s in SECRETS)


# --- commande_dbt ---------------------------------------------------------------------------------------------------


def test_commande_build_des_quatre_modeles_dans_l_ordre():
    assert list(SELECTION_CHAINE) == MODELES
    assert commande_dbt() == [DBT_EXE, "build", "--select", *MODELES]
    assert not any("+" in morceau for morceau in commande_dbt())


def test_commande_liste_de_chaines_sans_secret():
    commande = commande_dbt()
    assert isinstance(commande, list)
    assert all(isinstance(morceau, str) for morceau in commande)
    assert sans_secret(" ".join(commande))


def test_commande_sans_option_de_chemin():
    assert not any(morceau.startswith("--project-dir") or morceau.startswith("--profiles-dir")
                   or morceau.startswith("--target-path") or morceau.startswith("--log-path")
                   for morceau in commande_dbt())


# --- environnement_dbt ----------------------------------------------------------------------------------------------


def test_environnement_cibles_et_logs_sous_tmp_hors_du_projet():
    env = environnement_dbt(ENV)
    for nom in ("DBT_TARGET_PATH", "DBT_LOG_PATH"):
        assert env[nom].startswith("/tmp/")
        assert not env[nom].startswith(PROJET_DBT)
    assert env["DBT_TARGET_PATH"] == "/tmp/dbt/target"
    assert env["DBT_LOG_PATH"] == "/tmp/dbt/logs"


def test_environnement_projet_profils_et_statistiques_coupees():
    env = environnement_dbt(ENV)
    assert env["DBT_PROJECT_DIR"] == PROJET_DBT
    assert env["DBT_PROFILES_DIR"] == PROJET_DBT
    assert env["DBT_SEND_ANONYMOUS_USAGE_STATS"] == "false"


def test_environnement_garde_les_variables_databricks_sans_modifier_base():
    base = dict(ENV)
    env = environnement_dbt(base)
    assert base == ENV
    assert env is not base
    for nom, valeur in ENV.items():
        assert env[nom] == valeur


# --- masquer --------------------------------------------------------------------------------------------------------


def test_masquer_hote_complet():
    assert masquer("connexion a https://hote-factice.invalid refusee", ENV) == "connexion a *** refusee"


def test_masquer_hote_sans_https():
    assert masquer("hote hote-factice.invalid injoignable", ENV) == "hote *** injoignable"


def test_masquer_hote_avec_barre_finale_dans_base():
    base = {**ENV, "DATABRICKS_HOST": "https://hote-factice.invalid/"}
    assert masquer("hote hote-factice.invalid/sql", base) == "hote ***/sql"


def test_masquer_jeton_et_entrepot():
    assert masquer("jeton jeton-factice-123 entrepot entrepot-factice-456", ENV) == "jeton *** entrepot ***"


def test_masquer_valeur_vide_ignoree():
    base = {**ENV, "DATABRICKS_TOKEN": ""}
    assert masquer("ligne sans secret", base) == "ligne sans secret"
    assert masquer("hote hote-factice.invalid", base) == "hote ***"


# --- lancer_dbt -----------------------------------------------------------------------------------------------------


def test_lancer_code_non_nul_leve_sans_sortie_ni_secret_dans_le_message():
    stdout = "connexion https://hote-factice.invalid/sql refusee\njeton jeton-factice-123 invalide"
    stderr = "Erreur entrepot entrepot-factice-456"
    executer = FauxExecuter(code=1, stdout=stdout, stderr=stderr)
    recues = []
    with pytest.raises(ErreurDbt) as erreur:
        lancer_dbt(commande_dbt(), environnement_dbt(ENV), executer=executer, sortie=recues.append)
    message = str(erreur.value)
    assert "code 1" in message
    assert sans_secret(message)
    for ligne in (stdout + "\n" + stderr).splitlines() + recues:
        assert ligne not in message
    assert erreur.value.__cause__ is None
    assert len(recues) == 3
    assert all(sans_secret(ligne) for ligne in recues)


def test_lancer_code_zero_appel_tel_quel_sans_shell():
    commande = commande_dbt()
    env = environnement_dbt(ENV)
    executer = FauxExecuter(code=0, stdout="Done. PASS=61 WARN=0 ERROR=0\n")
    recues = []
    assert lancer_dbt(commande, env, executer=executer, sortie=recues.append) is None
    assert executer.args == (commande,)
    assert executer.kwargs == {"env": env, "capture_output": True, "text": True, "check": False}
    assert "shell" not in executer.kwargs
    assert recues == ["Done. PASS=61 WARN=0 ERROR=0"]
