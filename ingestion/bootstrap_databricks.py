"""Lecture et contrôle de databricks/bootstrap.sql, sans connexion à Databricks.

Le bootstrap ne doit créer que les schémas carburant_bronze, carburant_silver, carburant_gold et un Volume dans
carburant_bronze. verifier_perimetre refuse toute autre instruction (DROP, ALTER, GRANT, CREATE CATALOG, schéma
inconnu) avant tout envoi. Les schémas bronze, silver, gold, ci_silver, ci_gold et default appartiennent à un autre
projet (EnergiScope) : ils ne sont jamais touchés. L'exécution contre Databricks n'est pas écrite ici.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

SQL_BOOTSTRAP = Path(__file__).resolve().parents[1] / "databricks" / "bootstrap.sql"
SCHEMAS_AUTORISES = frozenset({"carburant_bronze", "carburant_silver", "carburant_gold"})
VARIABLES_SECRETES = ("DATABRICKS_HOST", "DATABRICKS_TOKEN")
DELAI_CONNEXION = 30  # secondes : délai dur du test de connexion, processus enfant tué au-delà
# Exécuté dans un processus enfant : le SDK peut se bloquer (hôte injoignable), seul un processus se tue de façon fiable.
CODE_VERIFICATION = (
    "import os; from databricks.sdk import WorkspaceClient; "
    "WorkspaceClient(host=os.environ['DATABRICKS_HOST'], token=os.environ['DATABRICKS_TOKEN']).current_user.me()"
)

_SCHEMA = re.compile(r"^CREATE SCHEMA IF NOT EXISTS (\w+)$", re.IGNORECASE)
_VOLUME = re.compile(r"^CREATE VOLUME IF NOT EXISTS (\w+)\.(\w+)$", re.IGNORECASE)


def lire_instructions(chemin: Path = SQL_BOOTSTRAP) -> list[str]:
    """Instructions du fichier SQL : commentaires `--` retirés, découpe sur `;`, espaces normalisés."""
    sans_commentaires = "\n".join(ligne.split("--", 1)[0] for ligne in chemin.read_text(encoding="utf-8").splitlines())
    return [" ".join(bloc.split()) for bloc in sans_commentaires.split(";") if bloc.strip()]


def verifier_perimetre(instructions: list[str]) -> dict[str, list[str]]:
    """Refuse toute instruction hors CREATE SCHEMA/VOLUME IF NOT EXISTS sur les schémas du projet. Rend ce qui est créé."""
    cree: dict[str, list[str]] = {"schemas": [], "volumes": []}
    for instruction in instructions:
        schema = _SCHEMA.match(instruction)
        volume = _VOLUME.match(instruction)
        if schema and schema.group(1).lower() in SCHEMAS_AUTORISES:
            cree["schemas"].append(schema.group(1).lower())
        elif volume and volume.group(1).lower() in SCHEMAS_AUTORISES:
            cree["volumes"].append(f"{volume.group(1).lower()}.{volume.group(2).lower()}")
        else:
            raise ValueError(f"instruction hors périmètre du bootstrap : {instruction[:80]}")
    return cree


def sans_secret(message: str, environnement: dict[str, str]) -> str:
    """Remplace dans un message toute valeur des variables secrètes (hôte, jeton) par ***."""
    for nom in VARIABLES_SECRETES:
        valeur = environnement.get(nom)
        if valeur:
            message = message.replace(valeur, "***")
    return message


def controler_connexion(environnement: dict[str, str], commande: list[str] | None = None, delai: float = DELAI_CONNEXION) -> str:
    """Rend "OK" ou "KO", rien d'autre. Une variable manquante donne KO sans rien lancer.

    La vérification tourne dans un processus enfant avec un délai dur (30 s) : au-delà, il est tué et le résultat est KO.
    Les sorties de l'enfant sont jetées, donc aucun secret ne peut sortir (ni hôte, ni jeton, ni message d'erreur).
    """
    if not all(environnement.get(nom) for nom in VARIABLES_SECRETES):
        return "KO"
    try:
        fini = subprocess.run(
            commande or [sys.executable, "-c", CODE_VERIFICATION],
            env=dict(environnement),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=delai,
            check=False,
        )
    except (subprocess.TimeoutExpired, OSError):
        return "KO"
    return "OK" if fini.returncode == 0 else "KO"


def main(argv: list[str] | None = None) -> None:
    parseur = argparse.ArgumentParser(description="Contrôles du bootstrap Databricks (la connexion est en option).")
    parseur.add_argument("--connexion", action="store_true", help="teste la connexion, n'affiche que OK ou KO")
    arguments = parseur.parse_args(argv)
    if arguments.connexion:
        print(controler_connexion(dict(os.environ)))


if __name__ == "__main__":
    main()
