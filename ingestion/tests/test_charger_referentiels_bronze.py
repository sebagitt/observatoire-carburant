"""Chargement des référentiels vers carburant_bronze : faux adaptateurs et faux client, aucun réseau, aucun jeton.

Les tests d'export lisent un schéma Postgres temporaire (fixture conn), jamais les vraies tables.
"""
import datetime as dt
import io
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from ingestion.charger_referentiels_bronze import (
    TABLES,
    ErreurChargementReferentiel,
    chemin_referentiel,
    construire_dependances,
    exporter_referentiel,
    garde_schema,
    main,
    nom_fichier,
    sql_compter,
    sql_remplacer,
    traiter_referentiel,
)

CAT = "workspace"
ENTREPOT = "entrepot-factice-456"
HORO = dt.datetime(2026, 11, 17, 9, 5, 0, tzinfo=dt.timezone.utc)
SECRETS = ("hote-factice.invalid", "jeton-factice-123", ENTREPOT)
ENV = {"DATABRICKS_HOST": "https://hote-factice.invalid", "DATABRICKS_TOKEN": "jeton-factice-123", "DATABRICKS_CATALOG": CAT, "DATABRICKS_WAREHOUSE_ID": ENTREPOT}


def test_les_cinq_tables_dans_l_ordre():
    assert TABLES == ("ref_cog_commune", "ref_cog_departement", "ref_cog_region", "ref_filosofi_commune", "ref_station_commune")


# --- noms, chemins, SQL produit ---------------------------------------------------------------------------------------


def test_nom_de_fichier_avec_table_et_horodatage_utc():
    assert nom_fichier("ref_cog_region", HORO) == "ref_cog_region__20261117T090500Z.parquet"
    heure_paris = HORO.astimezone(dt.timezone(dt.timedelta(hours=1)))
    assert nom_fichier("ref_cog_region", heure_paris) == "ref_cog_region__20261117T090500Z.parquet"


def test_chemin_exact_dans_le_volume():
    assert chemin_referentiel(CAT, "ref_cog_commune", "ref_cog_commune__20261117T090500Z.parquet") == (
        "/Volumes/workspace/carburant_bronze/depot/referentiels/ref_cog_commune/ref_cog_commune__20261117T090500Z.parquet"
    )


def test_sql_remplacer_exact_sur_le_seul_fichier():
    assert sql_remplacer(CAT, "ref_station_commune", "ref_station_commune__20261117T090500Z.parquet") == (
        "CREATE OR REPLACE TABLE workspace.carburant_bronze.ref_station_commune AS "
        "SELECT *, _metadata.file_path AS _fichier_source, current_timestamp() AS _charge_ts "
        "FROM read_files('/Volumes/workspace/carburant_bronze/depot/referentiels/ref_station_commune/"
        "ref_station_commune__20261117T090500Z.parquet', format => 'parquet')"
    )


def test_sql_compter_exact():
    assert sql_compter(CAT, "ref_cog_region") == "SELECT count(*) FROM workspace.carburant_bronze.ref_cog_region"


# --- garde du schéma ---------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("table", TABLES)
def test_garde_accepte_le_sql_produit_pour_chaque_table(table):
    garde_schema(sql_remplacer(CAT, table, nom_fichier(table, HORO)))
    garde_schema(sql_compter(CAT, table))


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE OR REPLACE TABLE workspace.bronze.ref_cog_commune AS SELECT 1",  # schéma d'EnergiScope
        "CREATE OR REPLACE TABLE workspace.carburant_silver.ref_cog_commune AS SELECT 1",
        "CREATE OR REPLACE TABLE autre.carburant_bronze.ref_cog_commune AS SELECT 1",
        "CREATE OR REPLACE TABLE workspace.carburant_bronze.prix_flux AS SELECT 1",  # table hors liste
        "CREATE OR REPLACE TABLE workspace.carburant_bronze.ref_cog_commune AS SELECT * FROM read_files('/Volumes/workspace/bronze/x/y.parquet')",
        "DROP TABLE workspace.carburant_bronze.ref_cog_commune",
        "SELECT count(*) FROM workspace.carburant_gold.ref_cog_region",
    ],
)
def test_garde_refuse_tout_nom_hors_workspace_carburant_bronze(sql):
    with pytest.raises(ValueError, match="hors workspace.carburant_bronze"):
        garde_schema(sql)


def test_catalogue_autre_que_workspace_refuse_avant_tout_sql():
    # message du contrôle du catalogue, pas celui de la garde : les deux défenses sont prouvées séparément
    with pytest.raises(ValueError, match="catalogue non autorisé"):
        sql_remplacer("autre", "ref_cog_commune", nom_fichier("ref_cog_commune", HORO))
    with pytest.raises(ValueError, match="catalogue non autorisé"):
        sql_compter("autre", "ref_cog_commune")
    with pytest.raises(ValueError, match="catalogue non autorisé"):
        construire_dependances(object(), "autre", ENTREPOT)


@pytest.mark.parametrize("table", ["prix_flux", "ref_cog_commune; DROP TABLE x", "", None])
def test_table_hors_liste_refusee(table):
    with pytest.raises(ValueError):
        sql_compter(CAT, table)


# --- export Parquet (schéma Postgres temporaire) ------------------------------------------------------------------------


def _creer_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE ref_cog_region (millesime INTEGER, reg TEXT, cheflieu TEXT, tncc TEXT, ncc TEXT, nccenr TEXT, "
            "libelle TEXT, charge_ts TIMESTAMPTZ DEFAULT now(), PRIMARY KEY (millesime, reg))"
        )
        cur.execute("INSERT INTO ref_cog_region (millesime, reg, libelle) VALUES (2026, '94', 'Corse'), (2026, '84', 'Auvergne-Rhône-Alpes')")
        cur.execute(
            "CREATE TABLE ref_filosofi_commune (millesime INTEGER, geo_object TEXT, codgeo TEXT, med_sl NUMERIC, med_sl_statut TEXT, "
            "med_sl_conf TEXT, pr_md60 NUMERIC, pr_md60_statut TEXT, pr_md60_conf TEXT, charge_ts TIMESTAMPTZ DEFAULT now(), "
            "PRIMARY KEY (millesime, geo_object, codgeo))"
        )
        cur.execute(
            "INSERT INTO ref_filosofi_commune (millesime, geo_object, codgeo, med_sl, pr_md60, pr_md60_conf) "
            "VALUES (2023, 'COM', '01001', 26400, NULL, 'C'), (2023, 'ARM', '13201', 21300, 28.1, 'F')"
        )
        cur.execute("CREATE TABLE ref_station_commune (station_id BIGINT PRIMARY KEY, com_arm_code TEXT, fichier_date DATE, charge_ts TIMESTAMPTZ DEFAULT now())")
        cur.execute("INSERT INTO ref_station_commune (station_id, com_arm_code, fichier_date) VALUES (2, NULL, '2026-11-16'), (1, '2A004', '2026-11-16')")
    conn.commit()


def test_export_trie_par_cle_types_conserves_et_compte(conn):
    _creer_tables(conn)
    octets, n = exporter_referentiel(conn, "ref_station_commune")
    table = pq.read_table(io.BytesIO(octets))
    assert n == 2 and table.num_rows == 2
    assert table.column_names == ["station_id", "com_arm_code", "fichier_date", "charge_ts"]
    assert table.column("station_id").to_pylist() == [1, 2]
    assert table.column("com_arm_code").to_pylist() == ["2A004", None]
    assert table.column("fichier_date").to_pylist() == [dt.date(2026, 11, 16)] * 2
    assert str(table.schema.field("charge_ts").type) == "timestamp[us, tz=UTC]"


def test_export_filosofi_numeric_en_double_et_nul_garde(conn):
    _creer_tables(conn)
    octets, _ = exporter_referentiel(conn, "ref_filosofi_commune")
    table = pq.read_table(io.BytesIO(octets))
    assert str(table.schema.field("med_sl").type) == "double"
    assert table.column("codgeo").to_pylist() == ["13201", "01001"]  # tri par (millesime, geo_object, codgeo)
    assert table.column("pr_md60").to_pylist() == [28.1, None]


def test_export_d_une_table_hors_liste_refuse(conn):
    with pytest.raises(ValueError):
        exporter_referentiel(conn, "prix_flux")


# --- traitement d'une table : compte Postgres, envoi, remplacement, contrôle ---------------------------------------------------


class Faux:
    def __init__(self, compte_destination):
        self.appels, self.compte = [], compte_destination

    def envoyer(self, chemin, octets):
        self.appels.append(("envoyer", chemin, len(octets) > 0))

    def executer(self, sql):
        self.appels.append(("executer", sql))

    def compter(self, table):
        self.appels.append(("compter", table))
        return self.compte


def test_traiter_ordre_des_appels_et_bilan(conn):
    _creer_tables(conn)
    faux = Faux(2)
    bilan = traiter_referentiel(conn, CAT, "ref_cog_region", HORO, faux.envoyer, faux.executer, faux.compter)
    fichier = "ref_cog_region__20261117T090500Z.parquet"
    assert faux.appels == [
        ("envoyer", chemin_referentiel(CAT, "ref_cog_region", fichier), True),
        ("executer", sql_remplacer(CAT, "ref_cog_region", fichier)),
        ("compter", "ref_cog_region"),
    ]
    assert bilan == {"table": "ref_cog_region", "fichier": fichier, "compte_postgres": 2, "compte_destination": 2}


def test_traiter_ecart_de_compte_arrete(conn):
    _creer_tables(conn)
    with pytest.raises(ErreurChargementReferentiel, match="ref_cog_region.*attendu 2.*obtenu 3"):
        traiter_referentiel(conn, CAT, "ref_cog_region", HORO, Faux(3).envoyer, Faux(3).executer, Faux(3).compter)


def test_traiter_attendu_lu_dans_postgres_avant_l_export(conn, monkeypatch):
    """L'attendu est le COUNT Postgres ; un export qui n'a pas le même nombre de lignes arrête avant tout envoi."""
    import ingestion.charger_referentiels_bronze as module

    _creer_tables(conn)
    monkeypatch.setattr(module, "exporter_referentiel", lambda conn, table: (b"x", 1))
    faux = Faux(2)
    with pytest.raises(ErreurChargementReferentiel, match="export"):
        traiter_referentiel(conn, CAT, "ref_cog_region", HORO, faux.envoyer, faux.executer, faux.compter)
    assert faux.appels == []


# --- main : option --table, arrêt au premier échec ------------------------------------------------------------------------


def lancer(argv, comptes, env=ENV):
    traitees, sorties = [], []

    def traiter(conn, catalogue, table, horodatage, envoyer, executer, compter):
        traitees.append(table)
        if comptes.get(table, 0) != 0:
            raise ErreurChargementReferentiel(f"{table} : écart de compte, attendu 5, obtenu {comptes[table]}")
        return {"table": table, "fichier": nom_fichier(table, horodatage), "compte_postgres": 5, "compte_destination": 5}

    fermee = []
    code = main(
        argv,
        env=env,
        sortie=sorties.append,
        connecter=lambda: SimpleNamespace(close=lambda: fermee.append(True)),
        creer_client=lambda env: object(),
        construire=lambda client, catalogue, entrepot: (None, None, None),
        traiter=traiter,
        maintenant=lambda: HORO,
    )
    return code, traitees, sorties, fermee


def test_main_toutes_les_tables_dans_l_ordre():
    code, traitees, sorties, fermee = lancer(["--table", "toutes"], {})
    assert code == 0 and traitees == list(TABLES) and fermee == [True]
    assert sorties[0] == "[ref_cog_commune] fichier=ref_cog_commune__20261117T090500Z.parquet compte_postgres=5 compte_destination=5 OK"


def test_main_une_seule_table():
    code, traitees, _, _ = lancer(["--table", "ref_cog_region"], {})
    assert code == 0 and traitees == ["ref_cog_region"]


def test_main_arret_au_premier_ecart():
    code, traitees, sorties, fermee = lancer(["--table", "toutes"], {"ref_cog_departement": 4})
    assert code == 1 and traitees == ["ref_cog_commune", "ref_cog_departement"] and fermee == [True]
    assert sorties[-1] == "ECHEC ref_cog_departement : écart de compte, attendu 5, obtenu 4"


@pytest.mark.parametrize("argv", [[], ["--table", "prix_flux"]])
def test_main_option_table_obligatoire_et_dans_la_liste(argv):
    with pytest.raises(SystemExit) as sortie:
        lancer(argv, {})
    assert sortie.value.code == 2


def test_main_variable_manquante_aucune_connexion():
    env = {k: v for k, v in ENV.items() if k != "DATABRICKS_TOKEN"}
    code, traitees, sorties, fermee = lancer(["--table", "toutes"], {}, env=env)
    assert code == 2 and traitees == [] and fermee == []
    assert sorties == ["KO variable manquante : DATABRICKS_TOKEN"]


def test_main_erreur_inattendue_sans_message():
    sorties = []

    def traiter(*args):
        raise RuntimeError("https://hote-factice.invalid jeton-factice-123")

    code = main(["--table", "ref_cog_region"], env=ENV, sortie=sorties.append, connecter=lambda: SimpleNamespace(close=lambda: None),
                creer_client=lambda env: object(), construire=lambda c, cat, e: (None, None, None), traiter=traiter, maintenant=lambda: HORO)
    assert code == 1 and sorties == ["ERREUR RuntimeError"]
    assert not any(s in " ".join(sorties) for s in SECRETS)


# --- adaptateurs réels sur un faux client ----------------------------------------------------------------------------------


class FauxClient:
    def __init__(self, nombre="5"):
        self.appels = []
        self.nombre = nombre
        self.warehouses = SimpleNamespace(get=self._get, start=None)
        self.statement_execution = SimpleNamespace(execute_statement=self._execute, get_statement=None)
        self.files = SimpleNamespace(upload=self._upload)

    def _get(self, **kwargs):
        self.appels.append(("get", kwargs))
        return SimpleNamespace(state=SimpleNamespace(value="RUNNING"))

    def _execute(self, **kwargs):
        self.appels.append(("execute", kwargs["statement"], kwargs["warehouse_id"]))
        return SimpleNamespace(status=SimpleNamespace(state="SUCCEEDED"), statement_id="s-1",
                               result=SimpleNamespace(data_array=[[self.nombre]]), manifest=None)

    def _upload(self, **kwargs):
        self.appels.append(("upload", kwargs["file_path"], kwargs["contents"].read(), kwargs["overwrite"]))


def test_adaptateurs_reels_envoi_entrepot_pret_puis_sql_avec_garde():
    client = FauxClient()
    envoyer, executer, compter = construire_dependances(client, CAT, ENTREPOT)
    chemin = chemin_referentiel(CAT, "ref_cog_region", nom_fichier("ref_cog_region", HORO))
    envoyer(chemin, b"pq")
    sql = sql_remplacer(CAT, "ref_cog_region", nom_fichier("ref_cog_region", HORO))
    executer(sql)
    assert compter("ref_cog_region") == 5
    assert client.appels == [
        ("upload", chemin, b"pq", True),
        ("get", {"id": ENTREPOT}),
        ("execute", sql, ENTREPOT),
        ("get", {"id": ENTREPOT}),
        ("execute", "SELECT count(*) FROM workspace.carburant_bronze.ref_cog_region", ENTREPOT),
    ]


def test_adaptateurs_reels_refusent_sql_et_chemin_hors_bronze():
    client = FauxClient()
    envoyer, executer, _ = construire_dependances(client, CAT, ENTREPOT)
    with pytest.raises(ValueError, match="hors workspace.carburant_bronze"):
        executer("CREATE OR REPLACE TABLE workspace.bronze.ref_cog_region AS SELECT 1")
    with pytest.raises(ValueError):
        envoyer("/Volumes/workspace/bronze/depot/referentiels/ref_cog_region/x.parquet", b"pq")
    assert client.appels == []
