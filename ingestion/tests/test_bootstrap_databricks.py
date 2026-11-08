import subprocess
import sys
import time
import types
from types import SimpleNamespace

import pytest

from ingestion import bootstrap_databricks
from ingestion.bootstrap_databricks import (
    DELAI_CONNEXION,
    MAX_TENTATIVES,
    SCHEMAS_AUTORISES,
    catalogue_valide,
    controler_authentification,
    controler_entrepot,
    controler_variables,
    extraire_colonne,
    lancer_bootstrap,
    lire_instructions,
    lister_volumes,
    lire_variables,
    main,
    qualifier,
    sans_secret,
    variables_manquantes,
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


ENV = {
    "DATABRICKS_HOST": "https://hote-factice.invalid",
    "DATABRICKS_TOKEN": "jeton-factice-123",
    "DATABRICKS_CATALOG": "workspace",
    "DATABRICKS_WAREHOUSE_ID": "entrepot-factice-456",
}
SECRETS = ("hote-factice", "jeton-factice-123", "entrepot-factice-456")
EXISTANTS = ["bronze", "silver", "gold", "ci_silver", "ci_gold", "default", "information_schema"]
NOUVEAUX = ["carburant_bronze", "carburant_silver", "carburant_gold"]


def enfant(code):
    return [sys.executable, "-c", code]


# --- contrôle 1 : variables -----------------------------------------------------------------------------------


def test_variables_completes_ok():
    assert controler_variables(ENV) == "OK variables"


def test_variable_manquante_nommee_sans_valeur():
    sans_jeton = {k: v for k, v in ENV.items() if k != "DATABRICKS_TOKEN"}
    assert controler_variables(sans_jeton) == "KO variable manquante : DATABRICKS_TOKEN"
    assert variables_manquantes({}) == ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_CATALOG", "DATABRICKS_WAREHOUSE_ID"]
    assert controler_variables({**ENV, "DATABRICKS_WAREHOUSE_ID": "   "}) == "KO variable manquante : DATABRICKS_WAREHOUSE_ID"
    for secret in SECRETS:
        assert secret not in controler_variables({"DATABRICKS_HOST": ENV["DATABRICKS_HOST"]})


def test_lire_variables_retire_espaces_et_guillemets():
    lues = lire_variables({"DATABRICKS_HOST": ' "https://x" ', "DATABRICKS_TOKEN": "'abc'", "AUTRE": "ignoré"})
    assert lues == {"DATABRICKS_HOST": "https://x", "DATABRICKS_TOKEN": "abc"}


@pytest.mark.parametrize("nom,attendu", [("workspace", True), ("main_2", True), ("a;DROP", False), ("w s", False), ("", False), ("1abc", False), ("a.b", False)])
def test_catalogue_valide_par_regex(nom, attendu):
    assert catalogue_valide(nom) is attendu


def test_catalogue_invalide_donne_ko_sans_afficher_la_valeur():
    assert controler_variables({**ENV, "DATABRICKS_CATALOG": "workspace; DROP"}) == "KO catalogue invalide"


# --- contrôle 2 : authentification (processus enfant, délai dur) ---------------------------------------------------


def test_delai_dur_par_defaut_de_30_secondes():
    assert DELAI_CONNEXION == 30


def test_authentification_ko_sans_rien_lancer_si_une_variable_manque(monkeypatch):
    def interdit(*args, **kwargs):
        raise AssertionError("aucun processus ne doit être lancé")

    monkeypatch.setattr(subprocess, "run", interdit)
    assert controler_authentification({"DATABRICKS_HOST": "https://x"}) == "KO variable manquante : DATABRICKS_TOKEN"


def test_authentification_ok_si_le_processus_enfant_reussit():
    assert controler_authentification(ENV, commande=enfant("raise SystemExit(0)")) == "OK authentification"


def test_authentification_ko_si_le_processus_enfant_echoue():
    assert controler_authentification(ENV, commande=enfant("raise SystemExit(3)")) == "KO authentification"


def test_delai_dur_tue_un_processus_enfant_bloque():
    debut = time.monotonic()
    assert controler_authentification(ENV, commande=enfant("import time; time.sleep(60)"), delai=1) == "KO authentification"
    assert time.monotonic() - debut < 10  # sans délai dur, ce test durerait 60 s


def test_authentification_aucun_secret_ni_sortie_de_l_enfant(capfd):
    code = "import os, sys; v = os.environ['DATABRICKS_TOKEN']; print(v); print(v, file=sys.stderr); raise SystemExit(1)"
    resultat = controler_authentification(ENV, commande=enfant(code))
    sorties = capfd.readouterr()
    assert resultat == "KO authentification"
    assert sorties.out == "" and sorties.err == ""


# --- faux SDK ------------------------------------------------------------------------------------------------------


def reponse(etat, lignes=None, identifiant="id-1"):
    resultat = SimpleNamespace(data_array=lignes) if lignes is not None else None
    return SimpleNamespace(statement_id=identifiant, status=SimpleNamespace(state=SimpleNamespace(value=etat)), result=resultat)


class FauxStatements:
    """Faux client.statement_execution : un catalogue en mémoire, des états programmables, aucune connexion."""

    def __init__(self, schemas=EXISTANTS, etats_select=("SUCCEEDED",), echec_a=None, retirer=None, sans_volume=False):
        self.sans_volume = sans_volume  # le CREATE VOLUME réussit mais le volume n'existe pas ensuite (simule un défaut)
        self.schemas = set(schemas)
        self.etats_select = list(etats_select)
        self.appels = []  # (statement, warehouse_id, wait_timeout)
        self.interrogations = 0
        self.echec_a = echec_a  # rang (1, 2...) de l'instruction CREATE qui échoue
        self.retirer = retirer  # schéma que la 1re instruction fait disparaître (simule un effet de bord)
        self.creates = 0
        self.volumes = set()
        self.journal_sortie = None
        self.instructions_affichees_avant = []

    def execute_statement(self, statement, warehouse_id, wait_timeout=None, **kwargs):
        self.appels.append((statement, warehouse_id, wait_timeout))
        if statement.startswith("SELECT 1"):
            return reponse(self.etats_select.pop(0) if self.etats_select else "SUCCEEDED")
        if statement.startswith("SHOW VOLUMES IN "):
            lignes = [["carburant_bronze", v.rsplit(".", 1)[1]] for v in sorted(self.volumes)] if not self.sans_volume else []
            return reponse("SUCCEEDED", lignes)
        if statement.startswith("SHOW SCHEMAS IN "):
            return reponse("SUCCEEDED", [[s] for s in sorted(self.schemas)])
        if statement.startswith("CREATE "):
            self.creates += 1
            if self.journal_sortie is not None and self.creates == 1:
                self.instructions_affichees_avant = list(self.journal_sortie)
            if self.echec_a == self.creates:
                return reponse("FAILED")
            nom = statement.rsplit(" ", 1)[1]
            if statement.startswith("CREATE SCHEMA"):
                self.schemas.add(nom.split(".")[1])
            else:
                self.volumes.add(nom)
            if self.retirer:
                self.schemas.discard(self.retirer)
            return reponse("SUCCEEDED")
        raise AssertionError(f"instruction inattendue : {statement}")

    def get_statement(self, statement_id):
        self.interrogations += 1
        return reponse(self.etats_select.pop(0) if self.etats_select else "SUCCEEDED")


def faux_client(**kwargs):
    se = FauxStatements(**kwargs)
    return SimpleNamespace(statement_execution=se), se


# --- contrôle 3 : entrepôt prêt (SELECT 1, scrutation bornée) ---------------------------------------------------------


def test_entrepot_pret_du_premier_coup():
    client, se = faux_client(etats_select=["SUCCEEDED"])
    pauses = []
    assert controler_entrepot(client, ENV["DATABRICKS_WAREHOUSE_ID"], dormir=pauses.append) == "OK entrepôt prêt"
    assert se.appels[0][0] == "SELECT 1" and len(se.appels) == 1 and pauses == []


def test_entrepot_a_froid_pending_puis_pret_scrute_sans_redemarrer():
    client, se = faux_client(etats_select=["PENDING", "PENDING", "RUNNING", "SUCCEEDED"])
    pauses = []
    assert controler_entrepot(client, ENV["DATABRICKS_WAREHOUSE_ID"], dormir=pauses.append, pause=7) == "OK entrepôt prêt"
    assert len(se.appels) == 1  # une seule soumission : jamais de redémarrage ni de nouvelle requête
    assert se.interrogations == 3 and pauses == [7, 7, 7]


def test_entrepot_jamais_plus_de_5_tentatives():
    client, se = faux_client(etats_select=["PENDING"] * 20)
    pauses = []
    assert controler_entrepot(client, ENV["DATABRICKS_WAREHOUSE_ID"], dormir=pauses.append) == "KO entrepôt (délai dépassé)"
    assert MAX_TENTATIVES == 5 and se.interrogations == 5 and len(pauses) == 5 and len(se.appels) == 1


@pytest.mark.parametrize("etat", ["FAILED", "CANCELED", "CLOSED"])
def test_entrepot_ko_immediat_si_la_requete_echoue(etat):
    client, se = faux_client(etats_select=[etat])
    assert controler_entrepot(client, ENV["DATABRICKS_WAREHOUSE_ID"], dormir=lambda s: None) == "KO entrepôt (échec)"
    assert se.interrogations == 0


def test_entrepot_ko_distinct_de_l_authentification_et_sans_secret_si_le_sdk_leve():
    class Casse:
        def execute_statement(self, *args, **kwargs):
            raise RuntimeError("401 https://hote-factice.invalid jeton-factice-123 entrepot-factice-456")

    resultat = controler_entrepot(SimpleNamespace(statement_execution=Casse()), ENV["DATABRICKS_WAREHOUSE_ID"], dormir=lambda s: None)
    assert resultat == "KO entrepôt (erreur)" != "KO authentification"
    assert not any(secret in resultat for secret in SECRETS)


# --- noms qualifiés et périmètre --------------------------------------------------------------------------------------

QUALIFIEES = [
    "CREATE SCHEMA IF NOT EXISTS workspace.carburant_bronze",
    "CREATE SCHEMA IF NOT EXISTS workspace.carburant_silver",
    "CREATE SCHEMA IF NOT EXISTS workspace.carburant_gold",
    "CREATE VOLUME IF NOT EXISTS workspace.carburant_bronze.depot",
]


def test_qualifier_donne_les_noms_catalogue_schema_exacts():
    assert qualifier(lire_instructions(), "workspace") == QUALIFIEES


def test_perimetre_sur_noms_qualifies_accepte_le_bon_catalogue():
    cree = verifier_perimetre(QUALIFIEES, "workspace")
    assert sorted(cree["schemas"]) == sorted(SCHEMAS_AUTORISES) and cree["volumes"] == ["carburant_bronze.depot"]


@pytest.mark.parametrize(
    "instruction",
    [
        "CREATE SCHEMA IF NOT EXISTS autre.carburant_bronze",  # mauvais catalogue
        "CREATE SCHEMA IF NOT EXISTS carburant_bronze",  # non qualifié alors qu'un catalogue est imposé
        "CREATE SCHEMA IF NOT EXISTS workspace.bronze",  # schéma de l'autre projet
        "CREATE SCHEMA IF NOT EXISTS workspace.ci_silver",
        "CREATE SCHEMA IF NOT EXISTS workspace.default",
        "CREATE VOLUME IF NOT EXISTS workspace.silver.depot",
        "CREATE SCHEMA IF NOT EXISTS workspace.carburant_bronze; DROP SCHEMA workspace.bronze",
        "DROP SCHEMA IF EXISTS workspace.carburant_bronze",
        "CREATE CATALOG IF NOT EXISTS workspace",
    ],
)
def test_perimetre_qualifie_refuse_tout_le_reste(instruction):
    with pytest.raises(ValueError):
        verifier_perimetre([instruction], "workspace")


def test_qualifier_refuse_un_catalogue_invalide():
    with pytest.raises(ValueError):
        qualifier(lire_instructions(), "workspace; DROP")


# --- exécution : deux passages, SHOW SCHEMAS avant, après 1, après 2 -----------------------------------------------------


def lancer(client):
    sorties = []
    code = lancer_bootstrap(client, ENV["DATABRICKS_WAREHOUSE_ID"], "workspace", sortie=sorties.append, dormir=lambda s: None)
    return code, sorties


def test_deux_passages_trois_schemas_nouveaux_et_run_2_identique():
    client, se = faux_client()
    code, sorties = lancer(client)
    assert code == 0
    assert [a[0] for a in se.appels if a[0].startswith("SHOW SCHEMAS")] == ["SHOW SCHEMAS IN workspace"] * 3
    assert [a[0] for a in se.appels if a[0].startswith("CREATE")] == QUALIFIEES * 2
    texte = "\n".join(sorties)
    assert "OK : 3 schémas créés" in texte and "run 2 identique au run 1" in texte
    assert se.schemas == set(EXISTANTS) | set(NOUVEAUX) and se.volumes == {"workspace.carburant_bronze.depot"}


def test_les_instructions_exactes_sont_affichees_avant_le_premier_appel_d_ecriture():
    client, se = faux_client()
    sorties = []
    se.journal_sortie = sorties
    lancer_bootstrap(client, ENV["DATABRICKS_WAREHOUSE_ID"], "workspace", sortie=sorties.append, dormir=lambda s: None)
    avant = "\n".join(se.instructions_affichees_avant)
    for instruction in QUALIFIEES:
        assert instruction in avant


def test_arret_a_la_premiere_erreur_sans_second_passage():
    client, se = faux_client(echec_a=2)
    code, sorties = lancer(client)
    assert code == 1
    assert se.creates == 2  # la 3e instruction et le run 2 ne partent pas
    texte = "\n".join(sorties)
    assert "KO instruction 2" in texte and "carburant_silver" in texte
    assert [a[0] for a in se.appels].count("SHOW SCHEMAS IN workspace") == 1


def test_ko_si_un_schema_existant_disparait():
    client, se = faux_client(retirer="ci_gold")
    code, sorties = lancer(client)
    assert code == 1 and "KO : schémas existants modifiés" in "\n".join(sorties)


def test_instruction_pending_scrutee_au_plus_5_fois():
    client, se = faux_client()
    original = se.execute_statement

    def lent(statement, warehouse_id, wait_timeout=None, **kwargs):
        r = original(statement, warehouse_id, wait_timeout=wait_timeout, **kwargs)
        if statement.endswith("workspace.carburant_bronze") and se.creates == 1:
            se.etats_select[:] = ["PENDING"] * 20
            return reponse("PENDING")
        return r

    se.execute_statement = lent
    code, sorties = lancer(client)
    assert code == 1 and se.interrogations == 5 and "KO instruction 1" in "\n".join(sorties)


def test_aucun_secret_dans_les_sorties_du_bootstrap():
    client, se = faux_client()
    code, sorties = lancer(client)
    assert code == 0 and not any(secret in ligne for ligne in sorties for secret in SECRETS)


# --- main ------------------------------------------------------------------------------------------------------------


def test_main_controles_s_arrete_au_premier_ko(monkeypatch, capsys):
    appels = []
    monkeypatch.setattr(bootstrap_databricks, "controler_variables", lambda env: appels.append("v") or "KO variable manquante : DATABRICKS_TOKEN")
    monkeypatch.setattr(bootstrap_databricks, "controler_authentification", lambda env: appels.append("a") or "OK authentification")
    assert main(["--controles"]) == 1
    assert appels == ["v"] and capsys.readouterr().out == "KO variable manquante : DATABRICKS_TOKEN\n"


def test_main_controles_enchaine_variables_authentification_entrepot(monkeypatch, capsys):
    monkeypatch.setattr(bootstrap_databricks, "controler_variables", lambda env: "OK variables")
    monkeypatch.setattr(bootstrap_databricks, "controler_authentification", lambda env: "OK authentification")
    monkeypatch.setattr(bootstrap_databricks, "creer_client", lambda env: SimpleNamespace(statement_execution=FauxStatements()))
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "entrepot-factice-456")
    assert main(["--controles"]) == 0
    assert capsys.readouterr().out.splitlines() == ["OK variables", "OK authentification", "OK entrepôt prêt"]


# --- construction du client : ce que reçoivent Config et WorkspaceClient --------------------------------------------------


def installer_faux_sdk(monkeypatch):
    """Faux modules databricks.sdk et databricks.sdk.core : enregistre les appels, ne contacte rien."""
    appels = {}

    class Config:
        def __init__(self, **kwargs):
            appels["config"] = kwargs

    class WorkspaceClient:
        def __init__(self, **kwargs):
            appels["client"] = kwargs
            self.current_user = SimpleNamespace(me=lambda: appels.setdefault("me", True))

    paquet, sdk, core = types.ModuleType("databricks"), types.ModuleType("databricks.sdk"), types.ModuleType("databricks.sdk.core")
    sdk.WorkspaceClient, core.Config = WorkspaceClient, Config
    paquet.sdk, sdk.core = sdk, core
    for nom, module in (("databricks", paquet), ("databricks.sdk", sdk), ("databricks.sdk.core", core)):
        monkeypatch.setitem(sys.modules, nom, module)
    return appels


def test_creer_client_passe_les_delais_a_config_et_seulement_config_au_client(monkeypatch):
    appels = installer_faux_sdk(monkeypatch)
    bootstrap_databricks.creer_client({**ENV, "DATABRICKS_HOST": ' "https://hote-factice.invalid" '})
    assert appels["config"] == {"host": "https://hote-factice.invalid", "token": "jeton-factice-123", "http_timeout_seconds": 20, "retry_timeout_seconds": 20}
    assert list(appels["client"]) == ["config"]  # WorkspaceClient n'accepte pas les délais : ils vivent dans Config


def test_le_vrai_sdk_n_accepte_pas_les_delais_sur_le_client_mais_sur_config():
    pytest.importorskip("databricks.sdk")
    import inspect

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config

    assert "config" in inspect.signature(WorkspaceClient.__init__).parameters
    assert not {"http_timeout_seconds", "retry_timeout_seconds"} & set(inspect.signature(WorkspaceClient.__init__).parameters)
    assert hasattr(Config, "http_timeout_seconds") and hasattr(Config, "retry_timeout_seconds")


def test_processus_enfant_d_authentification_utilise_creer_client_et_rend_0_ou_1(monkeypatch, capsys):
    appels = installer_faux_sdk(monkeypatch)
    for nom, valeur in ENV.items():
        monkeypatch.setenv(nom, valeur)
    assert main(["--auth-enfant"]) == 0 and appels.get("me") is True
    assert capsys.readouterr().out == ""  # rien d'affiché

    def casse(env):
        raise RuntimeError("401 jeton-factice-123")

    monkeypatch.setattr(bootstrap_databricks, "creer_client", casse)
    assert main(["--auth-enfant"]) == 1
    assert capsys.readouterr().out == ""


def test_commande_par_defaut_de_l_authentification_est_le_module_lui_meme(monkeypatch):
    vu = {}

    def faux_run(commande, **kwargs):
        vu["commande"], vu["cwd"] = commande, kwargs.get("cwd")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", faux_run)
    assert controler_authentification(ENV) == "OK authentification"
    assert vu["commande"][1:] == ["-m", "ingestion.bootstrap_databricks", "--auth-enfant"]
    assert vu["cwd"] == str(bootstrap_databricks.RACINE)


def test_volumes_avant_et_apres_sur_un_catalogue_vierge_le_schema_etant_absent_au_depart():
    client, se = faux_client()  # carburant_bronze n'existe pas encore : pas de SHOW VOLUMES avant
    code, sorties = lancer(client)
    assert code == 0
    assert [a[0] for a in se.appels if a[0].startswith("SHOW VOLUMES")] == ["SHOW VOLUMES IN workspace.carburant_bronze"] * 2
    texte = "\n".join(sorties)
    assert "Volumes avant : (schéma carburant_bronze absent)" in texte
    assert "Volumes après le run 1 : depot" in texte and "Volumes après le run 2 : depot" in texte
    assert "OK : Volume carburant_bronze.depot présent" in texte


def test_volumes_avant_apres_run_1_et_apres_run_2_sur_un_catalogue_deja_amorce():
    client, se = faux_client(schemas=EXISTANTS + NOUVEAUX)
    se.volumes.add("workspace.carburant_bronze.depot")
    code, sorties = lancer(client)
    assert code == 0
    assert [a[0] for a in se.appels if a[0].startswith("SHOW VOLUMES")] == ["SHOW VOLUMES IN workspace.carburant_bronze"] * 3
    texte = "\n".join(sorties)
    assert "Volumes avant : depot" in texte and "Volumes après le run 1 : depot" in texte and "Volumes après le run 2 : depot" in texte
    assert "OK : 0 schémas créés (déjà présents)" in texte and "OK : run 2 identique au run 1" in texte


def test_extraire_colonne_choisit_la_colonne_par_son_nom_puis_la_derniere_par_defaut():
    def avec_colonnes(noms, lignes):
        colonnes = [SimpleNamespace(name=n) for n in noms]
        return SimpleNamespace(result=SimpleNamespace(data_array=lignes), manifest=SimpleNamespace(schema=SimpleNamespace(columns=colonnes)))

    assert extraire_colonne(avec_colonnes(["namespace", "volume_name"], [["carburant_bronze", "depot"]]), "volume_name") == ["depot"]
    assert extraire_colonne(avec_colonnes(["volume_name", "namespace"], [["depot", "carburant_bronze"]]), "volume_name") == ["depot"]
    sans_manifest = SimpleNamespace(result=SimpleNamespace(data_array=[["carburant_bronze", "depot"]]))
    assert extraire_colonne(sans_manifest, "volume_name") == ["depot"]  # repli : dernière colonne
    assert extraire_colonne(SimpleNamespace(result=None), "volume_name") == []


def test_lister_volumes_interroge_le_schema_carburant_bronze_du_catalogue():
    client, se = faux_client()
    se.volumes.add("workspace.carburant_bronze.depot")
    assert lister_volumes(client, "entrepot", "workspace", dormir=lambda s: None) == ["depot"]
    assert se.appels[0][0] == "SHOW VOLUMES IN workspace.carburant_bronze"


def test_le_vrai_sdk_expose_les_appels_et_les_etats_utilises_par_le_bootstrap():
    pytest.importorskip("databricks.sdk")
    import inspect

    from databricks.sdk.service.sql import StatementExecutionAPI, StatementState

    parametres = inspect.signature(StatementExecutionAPI.execute_statement).parameters
    assert {"statement", "warehouse_id", "wait_timeout"} <= set(parametres)
    assert "statement_id" in inspect.signature(StatementExecutionAPI.get_statement).parameters
    assert {"PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELED", "CLOSED"} <= {etat.value for etat in StatementState}


def test_ko_si_le_volume_n_existe_pas_apres_le_bootstrap():
    client, se = faux_client(sans_volume=True)
    code, sorties = lancer(client)
    assert code == 1 and "KO : Volume carburant_bronze.depot absent" in "\n".join(sorties)
