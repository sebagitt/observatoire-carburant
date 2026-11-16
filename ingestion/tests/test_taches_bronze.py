"""Tâches du DAG chargement_bronze : faux client, faux main, aucun réseau, aucune base, aucun jeton.

Bibliothèque standard seulement : tourne dans le conteneur (make test) et sur l'hôte (.venv).
"""
from types import SimpleNamespace

import pytest

from ingestion.taches_bronze import ARGUMENTS_CHARGEMENT, ChargementEchoue, DoublonsBronze, tache_charger, tache_controler

ENV = {
    "DATABRICKS_HOST": "https://hote-factice.invalid",
    "DATABRICKS_TOKEN": "jeton-factice-123",
    "DATABRICKS_CATALOG": "workspace",
    "DATABRICKS_WAREHOUSE_ID": "entrepot-factice-456",
}
SECRETS = ("hote-factice.invalid", "jeton-factice-123", "entrepot-factice-456")


class FauxClient:
    """Entrepôt et API d'instructions factices ; chaque lecture rend la ligne suivante (total, min, max, doublons)."""

    def __init__(self, etats=("RUNNING",), lignes=(("10", "2026-11-16T09:45:02Z", "2026-11-16T09:45:02Z", "0"),)):
        self.appels = []
        self.etats, self.lignes = list(etats), [list(l) for l in lignes]
        self.warehouses = SimpleNamespace(get=self._get, start=lambda **k: self.appels.append("start"))
        self.statement_execution = SimpleNamespace(execute_statement=self._execute, get_statement=None)

    def _get(self, **kwargs):
        self.appels.append("get")
        etat = self.etats.pop(0) if len(self.etats) > 1 else self.etats[0]
        return SimpleNamespace(state=SimpleNamespace(value=etat))

    def _execute(self, **kwargs):
        self.appels.append(("execute", kwargs["statement"].split(" FROM ")[1]))
        ligne = self.lignes.pop(0) if len(self.lignes) > 1 else self.lignes[0]
        return SimpleNamespace(status=SimpleNamespace(state="SUCCEEDED"), statement_id="s", result=SimpleNamespace(data_array=[ligne]), manifest=None)


def bilan(table, statut="ok", traitees=3):
    return {"table": table, "statut": statut, "fenetres_traitees": traitees}


class FauxMain:
    """Remplace charger_bronze.main : appelle le traiter injecté avec des bilans prévus, rend un code."""

    def __init__(self, bilans, code=0):
        self.bilans, self.code, self.recu = bilans, code, None

    def __call__(self, argv, env, sortie, creer_client, traiter):
        self.recu = {"argv": argv, "client": creer_client(env)}
        for b in self.bilans:
            traiter(b)
            sortie(f"[{b['table']}] statut={b['statut']}")
        return self.code


def charger(main, client=None, env=ENV):
    client = client or FauxClient()
    sortie = []
    resultat = tache_charger(env=env, sortie=sortie.append, creer_client=lambda e: client, main=main,
                             traiter=lambda b: b, dormir=lambda s: None)
    return resultat, sortie, client


# --- charger --------------------------------------------------------------------------------------------------------


def test_arguments_toutes_les_tables_48_fenetres_3_appels():
    assert ARGUMENTS_CHARGEMENT == ["--table", "toutes", "--max-fenetres", "48", "--max-appels", "3"]


def test_entrepot_assure_avant_le_chargement_et_meme_client():
    client = FauxClient(etats=("STOPPED", "RUNNING"))
    main = FauxMain([bilan("prix_flux"), bilan("station_etat_flux")])
    _, sortie, _ = charger(main, client)
    assert client.appels == ["get", "start", "get"]
    assert main.recu == {"argv": ARGUMENTS_CHARGEMENT, "client": client}
    assert sortie[0].startswith("ENTREPOT pret en ")


def test_statuts_ok_et_rien_a_faire_acceptes():
    main = FauxMain([bilan("prix_flux"), bilan("prix_flux", "rien_a_faire", 0), bilan("station_etat_flux"),
                     bilan("station_etat_flux", "rien_a_faire", 0)])
    resultat, sortie, _ = charger(main)
    assert [b["statut"] for b in resultat] == ["ok", "rien_a_faire", "ok", "rien_a_faire"]
    assert "[prix_flux] statut=ok" in sortie


@pytest.mark.parametrize("statut", ["echec", "inconnu", None])
def test_statut_non_accepte_echoue(statut):
    with pytest.raises(ChargementEchoue):
        charger(FauxMain([bilan("prix_flux"), bilan("station_etat_flux", statut)]))


def test_code_retour_non_nul_echoue():
    with pytest.raises(ChargementEchoue):
        charger(FauxMain([bilan("prix_flux")], code=1))


def test_aucun_bilan_echoue():
    with pytest.raises(ChargementEchoue):
        charger(FauxMain([]))


@pytest.mark.parametrize("nom", list(ENV))
def test_variable_absente_echoue_sans_client(nom):
    appels = []
    with pytest.raises(ChargementEchoue) as info:
        tache_charger(env={k: v for k, v in ENV.items() if k != nom}, sortie=appels.append,
                      creer_client=lambda e: appels.append("client"), main=FauxMain([]), traiter=lambda b: b)
    assert "client" not in appels and nom in str(info.value)


def test_messages_sans_secret():
    with pytest.raises(ChargementEchoue) as info:
        charger(FauxMain([bilan("prix_flux", "echec")], code=1))
    assert not any(s in str(info.value) for s in SECRETS)


# --- controler ------------------------------------------------------------------------------------------------------


def controler(client):
    sortie = []
    resultats = tache_controler(env=ENV, sortie=sortie.append, creer_client=lambda e: client, dormir=lambda s: None)
    return resultats, sortie


def test_controler_lit_les_deux_tables_sans_doublon():
    client = FauxClient()
    resultats, sortie = controler(client)
    assert [r["table"] for r in resultats] == ["prix_flux", "station_etat_flux"]
    assert [a[1] for a in client.appels if isinstance(a, tuple)] == ["workspace.carburant_bronze.prix_flux",
                                                                     "workspace.carburant_bronze.station_etat_flux"]
    assert all(l.startswith("LECTURE table=") and "doublons=0" in l for l in sortie)
    assert not any(s in "\n".join(sortie) for s in SECRETS)


@pytest.mark.parametrize("position", [0, 1])
def test_controler_echoue_si_doublons(position):
    lignes = [["10", "2026-11-16T09:45:02Z", "2026-11-16T09:45:02Z", "0"] for _ in range(2)]
    lignes[position][3] = "1"
    with pytest.raises(DoublonsBronze) as info:
        controler(FauxClient(lignes=lignes))
    table = ("prix_flux", "station_etat_flux")[position]
    assert f"{table}=1" in str(info.value)


def test_controler_variable_absente_echoue():
    with pytest.raises(ChargementEchoue):
        tache_controler(env={k: v for k, v in ENV.items() if k != "DATABRICKS_TOKEN"}, sortie=print,
                        creer_client=lambda e: FauxClient())
