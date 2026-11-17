"""Garde des schémas du projet dbt.

Les schémas bronze, silver, gold, ci_silver, ci_gold et default du catalogue appartiennent à un autre
projet (EnergiScope) : aucun nœud de ce projet ne doit y écrire ni y lire.

Quatre tests :
- projet réel : après dbt parse, chaque nœud et chaque source a un schéma qui commence par carburant_ ;
- projet réel : tout modèle sous models/staging/ est privé (access: private, groupe carburants) ;
- projet réel : tout modèle sous models/marts/ est dans carburant_gold, sous models/intermediate/ dans
  carburant_silver (J6) ;
- sonde : copie du projet avec cinq modèles factices, schémas attendus exacts (protège la macro
  generate_schema_name : sans elle, dbt produirait carburant_silver_carburant_silver ; et le +schema
  des dossiers marts et intermediate).

dbt parse ne se connecte pas : les variables DATABRICKS_* reçoivent des valeurs factices, .env n'est jamais lu.
Le projet testé est dbt/ par défaut ; GARDE_PROJET_DIR permet de viser une copie (mutation hors dépôt).

Commande : make test-dbt-garde
"""

import json
import os
import shutil
from pathlib import Path

import pytest
from dbt.cli.main import dbtRunner

PROJET = Path(os.environ.get("GARDE_PROJET_DIR", Path(__file__).resolve().parents[1]))
PREFIXE = "carburant_"
SCHEMAS_INTERDITS = {"bronze", "silver", "gold", "ci_silver", "ci_gold", "default", "information_schema"}
TYPES_GARDES = {"model", "seed", "snapshot", "test", "unit_test", "analysis"}

VARIABLES_FACTICES = {
    "DATABRICKS_HOST": "garde.invalid",
    "DATABRICKS_TOKEN": "jeton-factice",
    "DATABRICKS_CATALOG": "workspace",
    "DATABRICKS_WAREHOUSE_ID": "entrepot-factice",
}

SONDES = {
    "models/staging/garde_sonde_staging.sql": "select 1 as x",
    "models/autres/garde_sonde_defaut.sql": "select 1 as x",
    "models/staging/garde_sonde_gold.sql": "{{ config(schema='carburant_gold') }}\nselect 1 as x",
    "models/marts/garde_sonde_marts.sql": "select 1 as x",
    "models/intermediate/garde_sonde_intermediate.sql": "select 1 as x",
}
SCHEMAS_ATTENDUS = {
    "garde_sonde_staging": "carburant_silver",
    "garde_sonde_defaut": "carburant_silver",
    "garde_sonde_gold": "carburant_gold",
    "garde_sonde_marts": "carburant_gold",
    "garde_sonde_intermediate": "carburant_silver",
}
SCHEMA_PAR_DOSSIER = {"models/marts/": "carburant_gold", "models/intermediate/": "carburant_silver"}


@pytest.fixture(autouse=True)
def variables_factices(monkeypatch):
    for nom, valeur in VARIABLES_FACTICES.items():
        monkeypatch.setenv(nom, valeur)


def parser(projet: Path, sortie: Path) -> dict:
    """dbt parse du projet, manifest écrit dans sortie (jamais dans dbt/target)."""
    resultat = dbtRunner().invoke([
        "parse",
        "--project-dir", str(projet),
        "--profiles-dir", str(projet),
        "--target-path", str(sortie / "target"),
        "--log-path", str(sortie / "logs"),
        "--quiet",
    ])
    assert resultat.success, f"dbt parse en échec : {resultat.exception}"
    return json.loads((sortie / "target" / "manifest.json").read_text(encoding="utf-8"))


def schemas_par_noeud(manifest: dict) -> dict:
    noeuds = {
        cle: n["schema"]
        for cle, n in manifest["nodes"].items()
        if n["resource_type"] in TYPES_GARDES and n["package_name"] == manifest["metadata"]["project_name"]
    }
    noeuds.update({cle: s["schema"] for cle, s in manifest["sources"].items()})
    return noeuds


def test_projet_reel_tous_les_schemas_commencent_par_carburant(tmp_path):
    manifest = parser(PROJET, tmp_path)
    fautifs = {
        cle: schema
        for cle, schema in schemas_par_noeud(manifest).items()
        if not schema.startswith(PREFIXE) or schema in SCHEMAS_INTERDITS
    }
    assert fautifs == {}


def test_projet_reel_staging_prive(tmp_path):
    """Tout modèle sous models/staging/ a access == private (convention du projet)."""
    manifest = parser(PROJET, tmp_path)
    staging = {
        n["name"]: n["access"]
        for n in manifest["nodes"].values()
        if n["resource_type"] == "model"
        and n["original_file_path"].replace("\\", "/").startswith("models/staging/")
    }
    assert staging, "aucun modèle de staging trouvé : la garde passerait à vide"
    assert {nom: acces for nom, acces in staging.items() if acces != "private"} == {}


def test_projet_reel_schema_par_dossier(tmp_path):
    """Tout modèle sous models/marts/ est dans carburant_gold, sous models/intermediate/ dans carburant_silver."""
    manifest = parser(PROJET, tmp_path)
    vus = {dossier: {} for dossier in SCHEMA_PAR_DOSSIER}
    for n in manifest["nodes"].values():
        if n["resource_type"] != "model":
            continue
        chemin = n["original_file_path"].replace("\\", "/")
        for dossier in SCHEMA_PAR_DOSSIER:
            if chemin.startswith(dossier):
                vus[dossier][n["name"]] = n["schema"]
    for dossier, modeles in vus.items():
        assert modeles, f"aucun modèle sous {dossier} : la garde passerait à vide"
    fautifs = {
        nom: schema
        for dossier, modeles in vus.items()
        for nom, schema in modeles.items()
        if schema != SCHEMA_PAR_DOSSIER[dossier]
    }
    assert fautifs == {}


def test_sonde_schemas_exacts(tmp_path):
    copie = tmp_path / "projet"
    shutil.copytree(PROJET, copie, ignore=shutil.ignore_patterns("target", "logs", "garde", "__pycache__"))
    for chemin, sql in SONDES.items():
        fichier = copie / chemin
        fichier.parent.mkdir(parents=True, exist_ok=True)
        fichier.write_text(sql, encoding="utf-8")

    manifest = parser(copie, tmp_path)
    obtenus = {
        n["name"]: n["schema"]
        for n in manifest["nodes"].values()
        if n["resource_type"] == "model" and n["name"] in SCHEMAS_ATTENDUS
    }
    assert obtenus == SCHEMAS_ATTENDUS
