"""Couche Databricks du chargement : faux client uniquement, aucun réseau, aucun jeton, aucun .env."""
import inspect
import io
import socket
import subprocess
import sys
from types import SimpleNamespace

import pytest

from ingestion.bootstrap_databricks import ErreurBootstrap
from ingestion.chargement_databricks import (
    chemin_volume,
    envoyer_parquet,
    executer,
    sql_copy_into,
    sql_creer_table,
)

NOM = "prix_flux/instantane/20261112T100000Z__20261112T110000Z.parquet"
F1 = "20261112T100000Z__20261112T110000Z.parquet"
F2 = "20261112T110000Z__20261112T120000Z.parquet"
SECRETS = ("hote-factice.invalid", "jeton-factice-123", "entrepot-factice-456")


class FauxClient:
    """Enregistre tous les appels : c'est le seul chemin vers 'Databricks' dans ces tests."""

    def __init__(self, etats=("SUCCEEDED",), erreur=None):
        self.appels = []
        self.etats = list(etats)
        self.erreur = erreur
        self.files = SimpleNamespace(upload=self._upload)
        self.statement_execution = SimpleNamespace(
            execute_statement=self._execute, get_statement=self._get
        )

    def _upload(self, **kwargs):  # mots-clés seulement : on vérifie ainsi les noms transmis
        if self.erreur:
            raise self.erreur
        contenu = kwargs["contents"].read()
        self.appels.append(("upload", {**kwargs, "contents": contenu}))

    def _reponse(self):
        etat = self.etats.pop(0) if len(self.etats) > 1 else self.etats[0]
        return SimpleNamespace(status=SimpleNamespace(state=etat), statement_id="s-1")

    def _execute(self, **kwargs):
        if self.erreur:
            raise self.erreur
        self.appels.append(("execute", kwargs))
        return self._reponse()

    def _get(self, statement_id):
        self.appels.append(("get", statement_id))
        return self._reponse()


# --- chemins -------------------------------------------------------------------------------------------------------


def test_chemin_volume_exact():
    assert chemin_volume("workspace", NOM) == (
        "/Volumes/workspace/carburant_bronze/depot/prix_flux/instantane/" + F1
    )


def test_le_catalogue_vient_du_parametre():
    assert chemin_volume("autre_cat", NOM).startswith("/Volumes/autre_cat/carburant_bronze/depot/")


# --- COPY INTO -----------------------------------------------------------------------------------------------------


def test_copy_into_prix_exact():
    assert sql_copy_into("workspace", "prix_flux", "instantane", [F1, F2]) == (
        "COPY INTO workspace.carburant_bronze.prix_flux FROM (SELECT station_id, carburant, maj, prix, collecte_ts, "
        "_metadata.file_path AS _fichier_source, current_timestamp() AS _charge_ts "
        "FROM '/Volumes/workspace/carburant_bronze/depot/prix_flux/instantane/') "
        f"FILEFORMAT = PARQUET FILES = ('{F1}', '{F2}')"
    )


def test_copy_into_etats_exact():
    assert sql_copy_into("workspace", "station_etat_flux", "instantane", [F1]) == (
        "COPY INTO workspace.carburant_bronze.station_etat_flux FROM (SELECT station_id, collecte_ts, etat_hash, etat, "
        "_metadata.file_path AS _fichier_source, current_timestamp() AS _charge_ts "
        "FROM '/Volumes/workspace/carburant_bronze/depot/station_etat_flux/instantane/') "
        f"FILEFORMAT = PARQUET FILES = ('{F1}')"
    )


def test_la_liste_de_fichiers_est_toujours_exacte_jamais_le_dossier():
    sql = sql_copy_into("workspace", "prix_flux", "instantane", [F1, F2])
    assert sql.endswith(f"FILES = ('{F1}', '{F2}')")
    assert "PATTERN" not in sql and "*" not in sql
    source = sql.split(" FROM '", 1)[1].split("'", 1)[0]
    assert source.endswith("/") and ".parquet" not in source  # dossier seul : les fichiers sont dans FILES


def test_liste_vide_refusee():
    with pytest.raises(ValueError):
        sql_copy_into("workspace", "prix_flux", "instantane", [])


def test_limite_de_1000_fichiers():
    def noms(n):
        return [f"20260101T0000{i:04d}Z__20260101T0000{i:04d}Z.parquet" for i in range(n)]

    assert sql_copy_into("workspace", "prix_flux", "instantane", noms(1000)).count(".parquet'") == 1000
    with pytest.raises(ValueError):
        sql_copy_into("workspace", "prix_flux", "instantane", noms(1001))


@pytest.mark.parametrize("nom", ["dossier/", "a/b__c.parquet", "*.parquet", "a__b", "a__b.csv", "a.parquet", "x__y.parquet\n"])
def test_nom_de_fichier_qui_n_est_pas_un_fichier_de_fenetre_refuse(nom):
    with pytest.raises(ValueError):
        sql_copy_into("workspace", "prix_flux", "instantane", [F1, nom])


# --- noms sûrs ------------------------------------------------------------------------------------------------------

MAUVAIS = ["a'b", "a;b", "a b", "a..b", "", "a\n", "../x", "a/b", "a-b", 'a"b', "a`b"]


@pytest.mark.parametrize("mauvais", MAUVAIS)
def test_injection_refusee_partout(mauvais):
    with pytest.raises(ValueError):
        sql_copy_into(mauvais, "prix_flux", "instantane", [F1])
    with pytest.raises(ValueError):
        sql_copy_into("workspace", "prix_flux", mauvais, [F1])
    if mauvais:  # une chaîne vide devant F1 laisserait un nom valide : ce n'est pas un cas d'injection de nom
        with pytest.raises(ValueError):
            sql_copy_into("workspace", "prix_flux", "instantane", [mauvais + F1])
    with pytest.raises(ValueError):
        sql_creer_table(mauvais, "prix_flux")
    with pytest.raises(ValueError):
        chemin_volume(mauvais, NOM)
    with pytest.raises(ValueError):
        chemin_volume("workspace", f"prix_flux/{mauvais}/{F1}")
    with pytest.raises(ValueError):
        chemin_volume("workspace", f"prix_flux/instantane/{mauvais}")


@pytest.mark.parametrize("table", ["passage_flux", "export_curseur", "prix_flux; DROP TABLE x", "PRIX_FLUX", ""])
def test_table_hors_liste_blanche_refusee(table):
    with pytest.raises(ValueError):
        sql_copy_into("workspace", table, "instantane", [F1])
    with pytest.raises(ValueError):
        sql_creer_table("workspace", table)
    with pytest.raises(ValueError):
        chemin_volume("workspace", f"{table}/instantane/{F1}")


@pytest.mark.parametrize("nom", ["prix_flux", "prix_flux/instantane", "prix_flux/instantane/" + F1 + "/x", "/prix_flux/instantane/" + F1])
def test_nom_complet_mal_forme_refuse(nom):
    with pytest.raises(ValueError):
        chemin_volume("workspace", nom)


def test_les_messages_d_erreur_ne_repetent_pas_la_valeur_refusee():
    for secret in SECRETS:
        for appel in (
            lambda s=secret: sql_copy_into(s, "prix_flux", "instantane", [F1]),
            lambda s=secret: sql_copy_into("workspace", s, "instantane", [F1]),
            lambda s=secret: sql_copy_into("workspace", "prix_flux", s + "!", [F1]),
            lambda s=secret: chemin_volume("workspace", f"prix_flux/instantane/{s}.parquet"),
        ):
            with pytest.raises(ValueError) as info:
                appel()
            assert secret not in str(info.value)


# --- CREATE TABLE --------------------------------------------------------------------------------------------------


def test_creer_table_prix_exact():
    assert sql_creer_table("workspace", "prix_flux") == (
        "CREATE TABLE IF NOT EXISTS workspace.carburant_bronze.prix_flux (station_id BIGINT, carburant STRING, "
        "maj TIMESTAMP, prix DECIMAL(6,3), collecte_ts TIMESTAMP, _fichier_source STRING, _charge_ts TIMESTAMP)"
    )


def test_creer_table_etats_exact():
    assert sql_creer_table("workspace", "station_etat_flux") == (
        "CREATE TABLE IF NOT EXISTS workspace.carburant_bronze.station_etat_flux (station_id BIGINT, "
        "collecte_ts TIMESTAMP, etat_hash STRING, etat STRING, _fichier_source STRING, _charge_ts TIMESTAMP)"
    )


# --- envoi ----------------------------------------------------------------------------------------------------------


def test_envoi_avec_overwrite_et_le_bon_chemin():
    client = FauxClient()
    chemin = chemin_volume("workspace", NOM)
    envoyer_parquet(client, chemin, b"PAR1-octets")
    assert client.appels == [("upload", {"file_path": chemin, "contents": b"PAR1-octets", "overwrite": True})]


def test_les_parametres_d_upload_existent_dans_le_vrai_sdk():
    files = pytest.importorskip("databricks.sdk.service.files")
    client = FauxClient()
    envoyer_parquet(client, "/Volumes/c/carburant_bronze/depot/t/s/a__b.parquet", b"x")
    (_, kwargs), = client.appels
    signature = inspect.signature(files.FilesAPI.upload)
    parametres = [p for p in signature.parameters if p != "self"]
    assert set(kwargs) == {"file_path", "contents", "overwrite"} <= set(parametres)
    signature.bind(None, file_path="/x", contents=io.BytesIO(b"x"), overwrite=True)  # lève TypeError si un nom diffère


def test_echec_d_envoi_ne_laisse_sortir_aucun_secret():
    client = FauxClient(erreur=RuntimeError(f"https://{SECRETS[0]} {SECRETS[1]} {SECRETS[2]}"))
    with pytest.raises(ErreurBootstrap) as info:
        envoyer_parquet(client, chemin_volume("workspace", NOM), b"x")
    assert not any(s in str(info.value) for s in SECRETS)
    assert info.value.__cause__ is None


# --- exécution ------------------------------------------------------------------------------------------------------


def test_executer_transmet_l_instruction_et_l_entrepot_avec_l_attente_de_50_s():
    client = FauxClient()
    sql = sql_creer_table("workspace", "prix_flux")
    executer(client, "entrepot-x", sql, dormir=lambda s: None)
    assert client.appels == [("execute", {"statement": sql, "warehouse_id": "entrepot-x", "wait_timeout": "50s"})]


def test_executer_scrute_au_plus_5_fois_puis_leve():
    client = FauxClient(etats=("RUNNING",))
    pauses = []
    with pytest.raises(ErreurBootstrap, match="délai dépassé"):
        executer(client, "e", "SELECT 1", dormir=pauses.append)
    assert [a[0] for a in client.appels].count("get") == 5 and len(pauses) == 5


def test_executer_echec_de_l_instruction():
    with pytest.raises(ErreurBootstrap, match="échec"):
        executer(FauxClient(etats=("FAILED",)), "e", "SELECT 1", dormir=lambda s: None)


def test_executer_n_expose_aucun_secret_d_une_erreur_du_sdk():
    client = FauxClient(erreur=RuntimeError(f"https://{SECRETS[0]} {SECRETS[1]} {SECRETS[2]}"))
    with pytest.raises(ErreurBootstrap) as info:
        executer(client, "e", "SELECT 1", dormir=lambda s: None)
    assert not any(s in str(info.value) for s in SECRETS)
    assert info.value.__cause__ is None


# --- aucun réseau ---------------------------------------------------------------------------------------------------


def test_aucun_appel_reseau_ni_import_du_sdk_au_chargement_du_module():
    code = (
        "import sys, socket\n"
        "def interdit(*a, **k): raise AssertionError('reseau')\n"
        "socket.socket.connect = interdit; socket.getaddrinfo = interdit\n"
        "import ingestion.chargement_databricks as m\n"
        "assert 'databricks' not in sys.modules, 'SDK importe au niveau du module'\n"
        "m.sql_creer_table('c', 'prix_flux'); m.sql_copy_into('c', 'prix_flux', 's', ['a__b.parquet'])\n"
        "print('ok')\n"
    )
    fini = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=str(__import__("pathlib").Path(__file__).resolve().parents[2]))
    assert fini.stdout.strip() == "ok", fini.stderr[-300:]


def test_chemin_complet_avec_faux_client_sans_reseau(monkeypatch):
    def interdit(*a, **k):
        raise AssertionError("appel réseau")

    monkeypatch.setattr(socket.socket, "connect", interdit)
    monkeypatch.setattr(socket, "getaddrinfo", interdit)
    client = FauxClient()
    envoyer_parquet(client, chemin_volume("workspace", NOM), b"x")
    executer(client, "e", sql_copy_into("workspace", "prix_flux", "instantane", [F1]), dormir=lambda s: None)
    assert [a[0] for a in client.appels] == ["upload", "execute"]
