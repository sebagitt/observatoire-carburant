import pytest

from ingestion.bootstrap_databricks import SCHEMAS_AUTORISES, lire_instructions, sans_secret, verifier_perimetre


def test_le_sql_cree_les_trois_schemas_et_un_volume_sans_catalogue():
    instructions = lire_instructions()
    assert len(instructions) == 4
    assert all("IF NOT EXISTS" in i.upper() for i in instructions)
    assert not any("CATALOG" in i.upper() for i in instructions)
    cree = verifier_perimetre(instructions)
    assert sorted(cree["schemas"]) == sorted(SCHEMAS_AUTORISES)
    assert cree["volumes"] == ["carburant_bronze.depot"]


@pytest.mark.parametrize("autre", ["ci_silver", "ci_gold", "default", "information_schema", "autre_projet"])
def test_aucun_schema_hors_carburant_n_est_touche(autre):
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
