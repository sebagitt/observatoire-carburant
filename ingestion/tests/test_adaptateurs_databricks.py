"""Adaptateurs Databricks pour l'orchestration : faux client uniquement, aucun réseau, aucun jeton.

Ces tests ne dépendent que de la bibliothèque standard : ils tournent sur l'hôte (.venv, où le SDK 0.156.0 est installé,
donc le test de signatures du vrai SDK s'y exécute) et dans le conteneur (où ce seul test est ignoré par importorskip).
"""
import inspect
import socket
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from ingestion.adaptateurs_databricks import assurer_entrepot, construire_dependances
from ingestion.bootstrap_databricks import ErreurBootstrap

UTC = timezone.utc
CAT = "workspace"
ENTREPOT = "entrepot-factice-456"
SECRETS = ("hote-factice.invalid", "jeton-factice-123", ENTREPOT)
NOM = "prix_flux/instantane/20261109T100000Z__20261109T110000Z.parquet"
FICHIER = "20261109T100000Z__20261109T110000Z.parquet"


class Horloge:
    """Horloge et sommeil factices : dormir fait avancer le temps sans attendre."""

    def __init__(self):
        self.t, self.pauses = 0.0, []

    def __call__(self):
        return self.t

    def dormir(self, secondes):
        self.pauses.append(secondes)
        self.t += secondes


class FauxClient:
    def __init__(self, etats=("RUNNING",), nombre="806", erreur_get=None, erreur_start=None, etat_requete="SUCCEEDED", lignes="defaut"):
        self.appels = []
        self.etats = list(etats)
        self.erreur_get, self.erreur_start = erreur_get, erreur_start
        self.nombre, self.etat_requete = nombre, etat_requete
        self.lignes = [[nombre]] if lignes == "defaut" else lignes
        self.warehouses = SimpleNamespace(get=self._get, start=self._start)
        self.statement_execution = SimpleNamespace(execute_statement=self._execute, get_statement=self._get_statement)
        self.files = SimpleNamespace(upload=self._upload)

    def _get(self, **kwargs):
        self.appels.append(("get", kwargs))
        if self.erreur_get:
            raise self.erreur_get
        etat = self.etats.pop(0) if len(self.etats) > 1 else self.etats[0]
        return SimpleNamespace(state=etat if isinstance(etat, str) is False else SimpleNamespace(value=etat))

    def _start(self, **kwargs):
        self.appels.append(("start", kwargs))
        if self.erreur_start:
            raise self.erreur_start

    def _execute(self, **kwargs):
        self.appels.append(("execute", kwargs))
        return SimpleNamespace(
            status=SimpleNamespace(state=self.etat_requete),
            statement_id="s-1",
            result=SimpleNamespace(data_array=self.lignes) if self.lignes is not None else None,
            manifest=None,
        )

    def _get_statement(self, statement_id):
        raise AssertionError("pas de scrutation attendue")

    def _upload(self, **kwargs):
        self.appels.append(("upload", {**kwargs, "contents": kwargs["contents"].read()}))

    def genres(self):
        return [a[0] for a in self.appels]


def assurer(client, **kw):
    h = Horloge()
    duree = assurer_entrepot(client, ENTREPOT, dormir=h.dormir, horloge=h, **kw)
    return duree, h


# --- machine à états de l'entrepôt --------------------------------------------------------------------------------


def test_running_retour_immediat_sans_start():
    client = FauxClient(["RUNNING"])
    duree, h = assurer(client)
    assert duree == 0 and h.pauses == [] and client.genres() == ["get"]


def test_stopped_puis_starting_puis_running_start_exactement_une_fois():
    client = FauxClient(["STOPPED", "STARTING", "RUNNING"])
    duree, h = assurer(client)
    assert client.genres() == ["get", "start", "get", "get"]
    assert client.appels[1] == ("start", {"id": ENTREPOT})
    assert h.pauses == [10, 10] and duree == 20


def test_starting_attend_sans_jamais_appeler_start():
    client = FauxClient(["STARTING", "STARTING", "RUNNING"])
    duree, _ = assurer(client)
    assert "start" not in client.genres() and duree == 20


def test_stopping_attend_stopped_puis_demarre_une_fois():
    client = FauxClient(["STOPPING", "STOPPING", "STOPPED", "STARTING", "RUNNING"])
    duree, _ = assurer(client)
    assert client.genres() == ["get", "get", "get", "start", "get", "get"]
    assert client.genres().count("start") == 1 and duree == 40


def test_stopped_persistant_apres_le_start_ne_relance_pas_start():
    client = FauxClient(["STOPPED", "STOPPED", "STOPPED", "RUNNING"])
    assurer(client)
    assert client.genres().count("start") == 1


@pytest.mark.parametrize("etat", ["FAILED", "DELETED", "DELETING"])
def test_etat_inutilisable_leve_sans_start(etat):
    client = FauxClient([etat])
    with pytest.raises(ErreurBootstrap):
        assurer(client)
    assert "start" not in client.genres()


def test_etat_inconnu_leve_sans_repeter_la_valeur():
    client = FauxClient(["jeton-factice-123"])
    with pytest.raises(ErreurBootstrap) as info:
        assurer(client)
    assert "jeton-factice-123" not in str(info.value) and "start" not in client.genres()


def test_depassement_apres_le_nombre_maximal_d_interrogations():
    client = FauxClient(["STARTING"])
    with pytest.raises(ErreurBootstrap, match="non prêt"):
        _, _ = assurer(client, max_interrogations=5)
    assert client.genres().count("get") == 5 and "start" not in client.genres()


def test_depassement_apres_un_start_reste_a_un_seul_start():
    client = FauxClient(["STOPPED"])
    with pytest.raises(ErreurBootstrap):
        assurer(client, max_interrogations=4)
    assert client.genres().count("start") == 1 and client.genres().count("get") == 4


def test_par_defaut_pas_de_10_s_et_12_interrogations():
    client = FauxClient(["STARTING"])
    h = Horloge()
    with pytest.raises(ErreurBootstrap):
        assurer_entrepot(client, ENTREPOT, dormir=h.dormir, horloge=h)
    assert client.genres().count("get") == 12 and h.pauses == [10] * 11


@pytest.mark.parametrize("quoi", ["get", "start"])
def test_erreur_du_sdk_sans_valeur_sensible(quoi):
    erreur = RuntimeError(f"https://{SECRETS[0]} {SECRETS[1]} {SECRETS[2]}")
    client = FauxClient(["STOPPED"], erreur_get=erreur if quoi == "get" else None, erreur_start=erreur if quoi == "start" else None)
    with pytest.raises(ErreurBootstrap) as info:
        assurer(client)
    assert not any(s in str(info.value) for s in SECRETS) and info.value.__cause__ is None


def test_les_parametres_des_appels_existent_dans_le_vrai_sdk():
    sql = pytest.importorskip("databricks.sdk.service.sql")
    client = FauxClient(["STOPPED", "RUNNING"])
    assurer(client)
    for nom, methode in (("get", sql.WarehousesAPI.get), ("start", sql.WarehousesAPI.start)):
        (kwargs,) = [a[1] for a in client.appels if a[0] == nom][:1]
        inspect.signature(methode).bind(None, **kwargs)  # lève TypeError si un nom de paramètre diffère


# --- dépendances pour traiter_table --------------------------------------------------------------------------------


def dependances(client, **kw):
    h = Horloge()
    return construire_dependances(client, CAT, ENTREPOT, "instantane", dormir=h.dormir, horloge=h, **kw), h


def test_envoyer_calcule_le_chemin_du_volume_et_force_overwrite():
    client = FauxClient()
    (envoyer, _, _), _ = dependances(client)
    envoyer(NOM, b"octets")
    assert client.appels == [
        ("upload", {
            "file_path": f"/Volumes/{CAT}/carburant_bronze/depot/prix_flux/instantane/{FICHIER}",
            "contents": b"octets",
            "overwrite": True,
        })
    ]


def test_envoyer_refuse_une_source_differente_de_celle_liee():
    client = FauxClient()
    (envoyer, _, _), _ = dependances(client)
    with pytest.raises(ValueError):
        envoyer(f"prix_flux/annuel/{FICHIER}", b"x")
    assert client.appels == []


def test_charger_assure_l_entrepot_puis_execute_le_copy_into_exact():
    client = FauxClient(["STOPPED", "RUNNING"])
    (_, charger, _), _ = dependances(client)
    charger("prix_flux", [FICHIER])
    assert client.genres() == ["get", "start", "get", "execute"]
    assert client.appels[-1][1] == {
        "statement": (
            f"COPY INTO {CAT}.carburant_bronze.prix_flux FROM (SELECT station_id, carburant, maj, prix, collecte_ts, "
            "_metadata.file_path AS _fichier_source, current_timestamp() AS _charge_ts "
            f"FROM '/Volumes/{CAT}/carburant_bronze/depot/prix_flux/instantane/') "
            f"FILEFORMAT = PARQUET FILES = ('{FICHIER}')"
        ),
        "warehouse_id": ENTREPOT,
        "wait_timeout": "50s",
    }


def t(heure, minute=0, seconde=0, micro=0):
    return datetime(2026, 11, 9, heure, minute, seconde, micro, tzinfo=UTC)


def sql_compte(client):
    return [a[1]["statement"] for a in client.appels if a[0] == "execute"][-1]


def test_compter_sql_exact_avec_bornes_utc_et_decalage_explicite():
    client = FauxClient(["RUNNING"])
    (_, _, compter), _ = dependances(client)
    assert compter("prix_flux", t(10), t(11)) == 806
    assert sql_compte(client) == (
        f"SELECT count(*) FROM {CAT}.carburant_bronze.prix_flux "
        "WHERE collecte_ts >= TIMESTAMP '2026-11-09 10:00:00.000000+00:00' "
        "AND collecte_ts < TIMESTAMP '2026-11-09 11:00:00.000000+00:00'"
    )


def test_compter_convertit_une_entree_en_heure_de_paris_en_utc():
    paris = timezone(timedelta(hours=1))  # novembre : heure d'hiver, UTC+1
    client = FauxClient(["RUNNING"])
    (_, _, compter), _ = dependances(client)
    compter("station_etat_flux", datetime(2026, 11, 9, 11, 0, tzinfo=paris), datetime(2026, 11, 9, 12, 0, 0, 250, tzinfo=paris))
    assert sql_compte(client) == (
        f"SELECT count(*) FROM {CAT}.carburant_bronze.station_etat_flux "
        "WHERE collecte_ts >= TIMESTAMP '2026-11-09 10:00:00.000000+00:00' "
        "AND collecte_ts < TIMESTAMP '2026-11-09 11:00:00.000250+00:00'"
    )


def test_compter_assure_l_entrepot_avant_de_compter():
    client = FauxClient(["STARTING", "RUNNING"])
    (_, _, compter), _ = dependances(client)
    compter("prix_flux", t(10), t(11))
    assert client.genres() == ["get", "get", "execute"]


@pytest.mark.parametrize("borne", ["2026-11-09 10:00:00", datetime(2026, 11, 9, 10), None, 1762682400])
def test_compter_refuse_une_borne_qui_n_est_pas_un_datetime_avec_fuseau(borne):
    client = FauxClient(["RUNNING"])
    (_, _, compter), _ = dependances(client)
    with pytest.raises(ValueError):
        compter("prix_flux", borne, t(11))
    with pytest.raises(ValueError):
        compter("prix_flux", t(10), borne)
    assert client.appels == []


@pytest.mark.parametrize("table", ["passage_flux", "prix_flux; DROP TABLE x", "", "export_curseur"])
def test_noms_valides_pour_tous_les_adaptateurs(table):
    client = FauxClient(["RUNNING"])
    (_, charger, compter), _ = dependances(client)
    with pytest.raises(ValueError):
        compter(table, t(10), t(11))
    with pytest.raises(ValueError):
        charger(table, [FICHIER])
    assert client.appels == []


@pytest.mark.parametrize("mauvais", ["a'b", "a;b", "a b", "a..b", "", "a\n"])
def test_catalogue_source_et_entrepot_valides_a_la_construction(mauvais):
    client = FauxClient()
    with pytest.raises(ValueError):
        construire_dependances(client, mauvais, ENTREPOT, "instantane")
    with pytest.raises(ValueError):
        construire_dependances(client, CAT, ENTREPOT, mauvais)
    with pytest.raises(ValueError):
        construire_dependances(client, CAT, mauvais, "instantane")


@pytest.mark.parametrize("lignes", [[], None, [["1", "2"]], [["1"], ["2"]], [["abc-jeton-factice-123"]], [[None]], [["-3"]], [["1.5"]]])
def test_reponse_de_comptage_inattendue_leve_sans_repeter_la_valeur(lignes):
    client = FauxClient(["RUNNING"], lignes=lignes)
    (_, _, compter), _ = dependances(client)
    with pytest.raises(ErreurBootstrap) as info:
        compter("prix_flux", t(10), t(11))
    assert "jeton-factice-123" not in str(info.value)


def test_nombre_lu_dans_la_reponse_est_un_int():
    for brut, attendu in (("0", 0), ("806", 806), ("1234567", 1234567)):
        client = FauxClient(["RUNNING"], nombre=brut)
        (_, _, compter), _ = dependances(client)
        valeur = compter("prix_flux", t(10), t(11))
        assert valeur == attendu and type(valeur) is int


def test_echec_de_la_requete_sans_valeur_sensible():
    client = FauxClient(["RUNNING"], etat_requete="FAILED")
    (_, _, compter), _ = dependances(client)
    with pytest.raises(ErreurBootstrap) as info:
        compter("prix_flux", t(10), t(11))
    assert not any(s in str(info.value) for s in SECRETS)


# --- aucun réseau, aucun SDK au chargement ------------------------------------------------------------------------


def test_aucun_sdk_ni_reseau_au_chargement_du_module():
    code = (
        "import sys, socket\n"
        "def interdit(*a, **k): raise AssertionError('reseau')\n"
        "socket.socket.connect = interdit; socket.getaddrinfo = interdit\n"
        "import ingestion.adaptateurs_databricks as m\n"
        "assert 'databricks' not in sys.modules, 'SDK importe au niveau du module'\n"
        "print('ok')\n"
    )
    racine = Path(__file__).resolve().parents[2]
    fini = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=str(racine))
    assert fini.stdout.strip() == "ok", fini.stderr[-300:]


def test_chaine_complete_sans_reseau(monkeypatch):
    def interdit(*a, **k):
        raise AssertionError("appel réseau")

    monkeypatch.setattr(socket.socket, "connect", interdit)
    monkeypatch.setattr(socket, "getaddrinfo", interdit)
    client = FauxClient(["STOPPED", "RUNNING"])
    (envoyer, charger, compter), _ = dependances(client)
    envoyer(NOM, b"x")
    charger("prix_flux", [FICHIER])
    assert compter("prix_flux", t(10), t(11)) == 806
    assert client.genres() == ["upload", "get", "start", "get", "execute", "get", "execute"]
