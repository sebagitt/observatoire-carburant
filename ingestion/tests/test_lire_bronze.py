"""Lecture du total des tables bronze : faux client uniquement, aucun réseau, aucune base, aucun jeton.

Bibliothèque standard seulement : ces tests tournent dans le conteneur (make test) et sur l'hôte (.venv).
"""
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from ingestion.bootstrap_databricks import ErreurBootstrap
from ingestion.lire_bronze import CLES, NOMS_VARIABLES, lire_table, main, sql_lecture

UTC = timezone.utc
CAT = "workspace"
ENV = {
    "DATABRICKS_HOST": "https://hote-factice.invalid",
    "DATABRICKS_TOKEN": "jeton-factice-123",
    "DATABRICKS_CATALOG": CAT,
    "DATABRICKS_WAREHOUSE_ID": "entrepot-factice-456",
}
SECRETS = ("hote-factice.invalid", "jeton-factice-123", "entrepot-factice-456")
DEBUT = datetime(2026, 11, 9, 10, tzinfo=UTC)
FIN = datetime(2026, 11, 9, 11, tzinfo=UTC)
INTERDITS = ("COPY", "INSERT", "CREATE", "DROP", "UPDATE", "MERGE", "DELETE", "ALTER", "TRUNCATE", "GRANT")
LIGNE = ["10171", "2026-10-23T08:09:01.000Z", "2026-11-09T10:45:02.000Z", "0"]


class FauxClient:
    """Entrepôt et API d'instructions factices ; enregistre l'ordre des appels."""

    def __init__(self, etats=("RUNNING",), lignes=None, etat_requete="SUCCEEDED", erreur_execute=None):
        self.appels, self.instructions = [], []
        self.etats = list(etats)
        self.lignes = [LIGNE] if lignes is None else lignes
        self.etat_requete, self.erreur_execute = etat_requete, erreur_execute
        self.warehouses = SimpleNamespace(get=self._get, start=self._start)
        self.statement_execution = SimpleNamespace(execute_statement=self._execute, get_statement=self._get_statement)

    def _get(self, **kwargs):
        self.appels.append("get")
        etat = self.etats.pop(0) if len(self.etats) > 1 else self.etats[0]
        return SimpleNamespace(state=SimpleNamespace(value=etat))

    def _start(self, **kwargs):
        self.appels.append("start")

    def _execute(self, **kwargs):
        self.appels.append("execute")
        self.instructions.append(kwargs["statement"])
        if self.erreur_execute:
            raise self.erreur_execute
        lignes = self.lignes.pop(0) if self.lignes and isinstance(self.lignes[0], list) and len(self.lignes) > 1 else self.lignes[0]
        return SimpleNamespace(
            status=SimpleNamespace(state=self.etat_requete),
            statement_id="s-1",
            result=SimpleNamespace(data_array=[lignes]),
            manifest=None,
        )

    def _get_statement(self, statement_id):
        raise AssertionError("pas de scrutation attendue")


def lancer(*argv, env=ENV, client=None):
    sortie = []
    client = client or FauxClient()
    code = main(list(argv), env=env, sortie=sortie.append, creer_client=lambda e: client, dormir=lambda s: None)
    return code, sortie, client


# --- liste blanche -------------------------------------------------------------------------------------------------


def test_cles_de_la_liste_blanche():
    assert CLES == {"prix_flux": "station_id, carburant, maj", "station_etat_flux": "station_id, collecte_ts"}


@pytest.mark.parametrize("table", ["passage_flux", "ref_cog_commune", "prix_flux; DROP TABLE x", "", "PRIX_FLUX"])
def test_table_hors_liste_refusee(table):
    with pytest.raises(ValueError, match="table non autorisée"):
        sql_lecture(CAT, table)


@pytest.mark.parametrize("catalogue", ["", "work space", "w;DROP", "a.b"])
def test_catalogue_invalide_refuse(catalogue):
    with pytest.raises(ValueError):
        sql_lecture(catalogue, "prix_flux")


# --- forme de l'instruction ----------------------------------------------------------------------------------------


@pytest.mark.parametrize("table", ["prix_flux", "station_etat_flux"])
@pytest.mark.parametrize("plage", [None, (DEBUT, FIN)])
def test_instruction_select_sans_mot_d_ecriture(table, plage):
    sql = sql_lecture(CAT, table, *(plage or (None, None)))
    assert sql.startswith("SELECT ")
    mots = set(re.findall(r"[A-Za-z_]+", sql.upper()))
    assert not mots & set(INTERDITS)
    assert ";" not in sql
    assert f"FROM {CAT}.carburant_bronze.{table}" in sql


@pytest.mark.parametrize(
    "table, distinct",
    [("prix_flux", "count(DISTINCT station_id, carburant, maj)"), ("station_etat_flux", "count(DISTINCT station_id, collecte_ts)")],
)
def test_cle_correcte_dans_le_distinct(table, distinct):
    sql = sql_lecture(CAT, table)
    assert f"count(*) - {distinct} AS doublons" in sql
    assert sql.count("DISTINCT") == 1


def test_total_min_max_sans_plage():
    sql = sql_lecture(CAT, "prix_flux")
    assert "count(*) AS total" in sql and "min(collecte_ts) AS min_ts" in sql and "max(collecte_ts) AS max_ts" in sql
    assert "FILTER" not in sql


def test_bornes_de_plage_semi_ouvertes():
    sql = sql_lecture(CAT, "station_etat_flux", DEBUT, FIN)
    assert (
        "count(*) FILTER (WHERE collecte_ts >= TIMESTAMP '2026-11-09 10:00:00.000000+00:00' "
        "AND collecte_ts < TIMESTAMP '2026-11-09 11:00:00.000000+00:00') AS lignes_plage"
    ) in sql
    assert "<=" not in sql and " > " not in sql


def test_bornes_converties_en_utc():
    paris = timezone(timedelta(hours=1))
    sql = sql_lecture(CAT, "prix_flux", DEBUT.astimezone(paris), FIN.astimezone(paris))
    assert "'2026-11-09 10:00:00.000000+00:00'" in sql and "'2026-11-09 11:00:00.000000+00:00'" in sql


@pytest.mark.parametrize(
    "debut, fin",
    [(DEBUT, None), (None, FIN), (FIN, DEBUT), (DEBUT, DEBUT), (DEBUT.replace(tzinfo=None), FIN), (DEBUT, FIN.replace(tzinfo=None))],
)
def test_plage_invalide_refusee(debut, fin):
    with pytest.raises(ValueError):
        sql_lecture(CAT, "prix_flux", debut, fin)


# --- lecture et lecture du résultat --------------------------------------------------------------------------------


def test_entrepot_assure_avant_la_requete():
    client = FauxClient(etats=("STOPPED", "STARTING", "RUNNING"))
    lire_table(client, CAT, "entrepot-factice-456", "prix_flux", dormir=lambda s: None)
    assert client.appels == ["get", "start", "get", "get", "execute"]


def test_resultat_lu():
    r = lire_table(FauxClient(), CAT, "e", "station_etat_flux", dormir=lambda s: None)
    assert r == {"table": "station_etat_flux", "total": 10171, "min": "2026-10-23T08:09:01.000Z",
                 "max": "2026-11-09T10:45:02.000Z", "doublons": 0, "plage": None, "lignes_plage": None}


def test_resultat_avec_plage():
    client = FauxClient(lignes=[LIGNE + ["168"]])
    r = lire_table(client, CAT, "e", "station_etat_flux", DEBUT, FIN, dormir=lambda s: None)
    assert r["lignes_plage"] == 168 and r["plage"] == (DEBUT, FIN)


def test_table_vide_min_max_absents():
    client = FauxClient(lignes=[["0", None, None, "0"]])
    r = lire_table(client, CAT, "e", "prix_flux", dormir=lambda s: None)
    assert r["total"] == 0 and r["min"] is None and r["max"] is None


@pytest.mark.parametrize(
    "ligne",
    [["abc", "2026-10-23T08:09:01Z", "2026-10-23T08:09:01Z", "0"], ["-1", None, None, "0"],
     ["1", "jeton-factice-123", "2026-10-23T08:09:01Z", "0"], ["1", "2026-10-23T08:09:01Z", "2026-10-23T08:09:01Z"],
     ["1", "2026-10-23T08:09:01Z", "2026-10-23T08:09:01Z", "0", "5"]],
)
def test_reponse_inattendue_refusee(ligne):
    with pytest.raises(ErreurBootstrap):
        lire_table(FauxClient(lignes=[ligne]), CAT, "e", "prix_flux", dormir=lambda s: None)


# --- commande ------------------------------------------------------------------------------------------------------


def test_sortie_une_ligne_par_table_format_attendu():
    code, sortie, client = lancer()
    assert code == 0
    assert sortie == [
        "LECTURE table=prix_flux total=10171 min=2026-10-23T08:09:01.000Z max=2026-11-09T10:45:02.000Z doublons=0",
        "LECTURE table=station_etat_flux total=10171 min=2026-10-23T08:09:01.000Z max=2026-11-09T10:45:02.000Z doublons=0",
    ]
    assert client.appels.count("execute") == 2


def test_sortie_avec_plage():
    code, sortie, _ = lancer("--table", "station_etat_flux", "--debut", "2026-11-09T10:00:00+00:00",
                             "--fin", "2026-11-09T11:00:00+00:00", client=FauxClient(lignes=[LIGNE + ["168"]]))
    assert code == 0
    assert sortie == [
        "LECTURE table=station_etat_flux total=10171 min=2026-10-23T08:09:01.000Z max=2026-11-09T10:45:02.000Z "
        "doublons=0 plage=2026-11-09T10:00:00+00:00/2026-11-09T11:00:00+00:00 lignes=168"
    ]


@pytest.mark.parametrize(
    "options",
    [["--table", "passage_flux"], ["--table", "x; DROP"], ["--debut", "2026-11-09T10:00:00+00:00"],
     ["--debut", "2026-11-09T10:00:00", "--fin", "2026-11-09T11:00:00"], ["--debut", "abc", "--fin", "def"],
     ["--debut", "2026-11-09T11:00:00+00:00", "--fin", "2026-11-09T10:00:00+00:00"]],
)
def test_options_invalides_code_2_sans_client(options):
    appels = []
    with pytest.raises(SystemExit) as info:
        main(options, env=ENV, sortie=appels.append, creer_client=lambda e: appels.append("client"))
    assert info.value.code == 2 and "client" not in appels


@pytest.mark.parametrize("nom", NOMS_VARIABLES)
def test_variable_absente_code_2_sans_client(nom):
    env = {k: v for k, v in ENV.items() if k != nom}
    appels = []
    code = main([], env=env, sortie=appels.append, creer_client=lambda e: appels.append("client"))
    assert code == 2 and "client" not in appels and nom in appels[0]


def test_aucune_valeur_secrete_dans_la_sortie():
    code, sortie, _ = lancer("--debut", "2026-11-09T10:00:00+00:00", "--fin", "2026-11-09T11:00:00+00:00",
                             client=FauxClient(lignes=[LIGNE + ["168"]]))
    texte = "\n".join(sortie)
    assert code == 0 and not any(s in texte for s in SECRETS)


def test_erreur_du_sdk_sans_message_ni_secret():
    client = FauxClient(erreur_execute=RuntimeError("echec sur hote-factice.invalid avec jeton-factice-123"))
    code, sortie, _ = lancer(client=client)
    texte = "\n".join(sortie)
    assert code == 1 and texte == "ERREUR ErreurBootstrap"
    assert not any(s in texte for s in SECRETS)


def test_requete_en_echec_code_1():
    code, sortie, _ = lancer(client=FauxClient(etat_requete="FAILED"))
    assert code == 1 and sortie == ["ERREUR ErreurBootstrap"]


def test_aucune_connexion_postgres_ni_creer_table():
    racine = Path(__file__).resolve().parents[2]
    source = (racine / "ingestion" / "lire_bronze.py").read_text(encoding="utf-8")
    assert "creer_table" not in source and "connecter" not in source and "psycopg" not in source
    fini = subprocess.run(
        [sys.executable, "-c", "import sys, ingestion.lire_bronze; print(sorted(m for m in sys.modules if m in "
         "('ingestion.db', 'psycopg2', 'ingestion.export_curseur', 'databricks.sdk')))"],
        cwd=racine, capture_output=True, text=True, timeout=60,
    )
    assert fini.returncode == 0 and fini.stdout.strip() == "[]"
