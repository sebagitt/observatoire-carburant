"""Commande de chargement du bronze : faux traiter_table, faux client, aucun réseau, aucune base."""
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingestion.charger_bronze import NOMS_VARIABLES, main

UTC = timezone.utc
ENV = {
    "DATABRICKS_HOST": "https://hote-factice.invalid",
    "DATABRICKS_TOKEN": "jeton-factice-123",
    "DATABRICKS_CATALOG": "workspace",
    "DATABRICKS_WAREHOUSE_ID": "entrepot-factice-456",
}
SECRETS = ("hote-factice.invalid", "jeton-factice-123", "entrepot-factice-456")
T = datetime(2026, 11, 13, 14, 0, tzinfo=UTC)


def bilan(traitees=2, statut="ok", echec=None, table="prix_flux"):
    return {
        "table": table, "source": "instantane", "statut": statut, "raison": None,
        "fenetres_traitees": traitees, "fenetres_vides": 0, "fenetres_verifiees": traitees,
        "fichiers_envoyes": [f"f{i}.parquet" for i in range(traitees)],
        "curseur_avant": datetime(2026, 10, 23, 9, tzinfo=UTC), "curseur_apres": datetime(2026, 10, 23, 11, tzinfo=UTC),
        "echecs": [echec] if echec else [], "echec": echec,
    }


class Banc:
    """Remplace toutes les dépendances de main et enregistre l'ordre des appels."""

    def __init__(self, scenario=None, defaut=None, erreur_client=None, erreur_traiter=None):
        self.appels, self.sortie = [], []
        self.scenario = {t: list(v) for t, v in (scenario or {}).items()}
        self.defaut = defaut
        self.erreur_client, self.erreur_traiter = erreur_client, erreur_traiter

    def connecter(self):
        self.appels.append("connecter")
        return self

    def close(self):
        self.appels.append("close")

    def creer_table(self, conn):
        self.appels.append("creer_table")

    def creer_client(self, env):
        self.appels.append("creer_client")
        if self.erreur_client:
            raise self.erreur_client
        return object()

    def dependances(self, client, catalogue, entrepot, source):
        self.appels.append(("dependances", catalogue, source))
        return ("envoyer", "charger", "compter")

    def traiter(self, conn, table, source, maintenant, envoyer, charger, compter, max_fenetres=48, **kw):
        self.appels.append(("traiter", table, source, max_fenetres))
        if self.erreur_traiter:
            raise self.erreur_traiter
        file = self.scenario.get(table)
        if file:
            return file.pop(0)
        return self.defaut(table) if self.defaut else bilan(0, table=table)

    def lancer(self, *argv, env=ENV):
        return main(list(argv), env=env, sortie=self.sortie.append, connecter=self.connecter,
                    creer_table=self.creer_table, creer_client=self.creer_client,
                    construire=self.dependances, traiter=self.traiter, maintenant=lambda: T)

    def traitements(self):
        return [a for a in self.appels if isinstance(a, tuple) and a[0] == "traiter"]

    def texte(self):
        return "\n".join(self.sortie)


# --- options -------------------------------------------------------------------------------------------------------


def test_options_par_defaut_toutes_les_tables_48_fenetres_un_appel():
    banc = Banc()
    assert banc.lancer() == 0
    assert banc.traitements() == [("traiter", "prix_flux", "instantane", 48), ("traiter", "station_etat_flux", "instantane", 48)]


@pytest.mark.parametrize("table", ["prix_flux", "station_etat_flux"])
def test_une_seule_table(table):
    banc = Banc()
    banc.lancer("--table", table, "--max-fenetres", "7")
    assert banc.traitements() == [("traiter", table, "instantane", 7)]


@pytest.mark.parametrize(
    "options",
    [["--max-fenetres", "0"], ["--max-fenetres", "1001"], ["--max-fenetres", "-1"], ["--max-fenetres", "abc"],
     ["--max-appels", "0"], ["--max-appels", "51"], ["--max-appels", "x"], ["--table", "passage_flux"], ["--table", "x; DROP"]],
)
def test_options_hors_bornes_refusees_sans_rien_ecrire(options):
    banc = Banc()
    with pytest.raises(SystemExit) as info:
        banc.lancer(*options)
    assert info.value.code == 2 and banc.appels == []


@pytest.mark.parametrize("options", [["--max-fenetres", "1"], ["--max-fenetres", "1000"], ["--max-appels", "1"], ["--max-appels", "50"]])
def test_bornes_incluses_acceptees(options):
    assert Banc().lancer(*options) == 0


# --- variables et ordre --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("nom", NOMS_VARIABLES)
def test_variable_absente_code_2_sans_aucune_ecriture(nom):
    env = {k: v for k, v in ENV.items() if k != nom}
    banc = Banc()
    assert banc.lancer(env=env) == 2
    assert banc.appels == []
    assert nom in banc.texte() and not any(s in banc.texte() for s in SECRETS)


def test_variable_vide_ou_blanche_compte_comme_absente():
    banc = Banc()
    assert banc.lancer(env={**ENV, "DATABRICKS_TOKEN": "   "}) == 2 and banc.appels == []


def test_ordre_curseur_puis_client_puis_dependances_puis_traitement():
    banc = Banc()
    banc.lancer("--table", "prix_flux")
    assert banc.appels == ["connecter", "creer_table", "creer_client", ("dependances", "workspace", "instantane"),
                           ("traiter", "prix_flux", "instantane", 48), "close"]


# --- boucle ---------------------------------------------------------------------------------------------------------


def test_arret_des_que_aucune_fenetre_n_est_traitee():
    banc = Banc(scenario={"prix_flux": [bilan(2), bilan(2), bilan(0)]}, defaut=lambda t: bilan(0, table=t))
    assert banc.lancer("--table", "prix_flux", "--max-appels", "10") == 0
    assert len(banc.traitements()) == 3


def test_arret_au_plafond_max_appels():
    banc = Banc(defaut=lambda t: bilan(2, table=t))
    assert banc.lancer("--table", "prix_flux", "--max-appels", "3") == 0
    assert len(banc.traitements()) == 3


def test_le_plafond_s_applique_par_table():
    banc = Banc(defaut=lambda t: bilan(2, table=t))
    banc.lancer("--max-appels", "2")
    assert [a[1] for a in banc.traitements()] == ["prix_flux", "prix_flux", "station_etat_flux", "station_etat_flux"]


def test_arret_sur_echec_code_1_et_aucune_autre_table():
    echec = {"phase": "comptage", "fenetre": (T, T), "erreur": None, "compte_postgres": 3, "compte_destination": 1}
    banc = Banc(scenario={"prix_flux": [bilan(2), bilan(1, statut="echec", echec=echec)]}, defaut=lambda t: bilan(2, table=t))
    assert banc.lancer("--max-appels", "10") == 1
    assert [a[1] for a in banc.traitements()] == ["prix_flux", "prix_flux"]  # ni 3e appel, ni station_etat_flux
    assert "comptage" in banc.texte() and "3" in banc.texte()


def test_rien_a_faire_est_un_succes():
    assert Banc(defaut=lambda t: {**bilan(0, table=t), "statut": "rien_a_faire", "raison": "aucune fenêtre close"}).lancer() == 0


def test_la_connexion_est_fermee_meme_apres_un_echec():
    banc = Banc(erreur_traiter=RuntimeError("x"))
    banc.lancer("--table", "prix_flux")
    assert banc.appels[-1] == "close"


# --- sortie ---------------------------------------------------------------------------------------------------------


def test_bilan_par_appel_nombres_dates_et_types_seulement():
    banc = Banc(defaut=lambda t: bilan(2, table=t))
    banc.lancer("--table", "prix_flux")
    texte = banc.texte()
    assert "prix_flux" in texte and "2026-10-23T09:00:00+00:00" in texte and "2026-10-23T11:00:00+00:00" in texte
    assert not any(s in texte for s in SECRETS)


@pytest.mark.parametrize("erreur_client,erreur_traiter", [(RuntimeError, None), (None, RuntimeError)])
def test_exception_inattendue_code_1_sans_aucune_valeur_sensible(erreur_client, erreur_traiter):
    message = f"https://{SECRETS[0]} {SECRETS[1]} {SECRETS[2]}"
    banc = Banc(erreur_client=erreur_client(message) if erreur_client else None,
                erreur_traiter=erreur_traiter(message) if erreur_traiter else None)
    assert banc.lancer("--table", "prix_flux") == 1
    assert "RuntimeError" in banc.texte() and not any(s in banc.texte() for s in SECRETS)


def test_une_exception_d_ecriture_de_curseur_est_signalee_sans_message():
    banc = Banc()
    banc.creer_table = lambda conn: (_ for _ in ()).throw(ValueError(SECRETS[1]))
    assert banc.lancer() == 1 and SECRETS[1] not in banc.texte() and "ValueError" in banc.texte()


# --- module ---------------------------------------------------------------------------------------------------------


def racine():
    return Path(__file__).resolve().parents[2]


def test_aucun_import_du_sdk_au_chargement_du_module():
    code = "import sys\nimport ingestion.charger_bronze\nassert 'databricks' not in sys.modules\nprint('ok')\n"
    fini = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=str(racine()))
    assert fini.stdout.strip() == "ok", fini.stderr[-300:]


def test_lancement_en_module_sans_variables_code_2_avant_tout_acces():
    env = {k: v for k, v in os.environ.items() if not k.startswith("DATABRICKS_") and k != "COLLECTE_DSN"}
    fini = subprocess.run([sys.executable, "-m", "ingestion.charger_bronze", "--table", "prix_flux"],
                          capture_output=True, text=True, cwd=str(racine()), env=env)
    assert fini.returncode == 2
    assert "DATABRICKS_HOST" in fini.stdout + fini.stderr
