"""Bootstrap Databricks : contrôles, puis création des schémas du projet et du Volume de dépôt.

Trois contrôles distincts, chacun avec son propre résultat (jamais de valeur affichée : ni hôte, ni jeton, ni entrepôt) :
  1. variables : KO nommant la variable manquante, ou KO si le catalogue n'a pas la forme attendue ;
  2. authentification : appel rapide à l'API du workspace, dans un processus enfant tué après 30 s ;
  3. entrepôt prêt : un SELECT 1, scruté au plus 5 fois (un entrepôt serverless arrêté démarre à la première requête,
     l'état PENDING signifie « en attente de l'entrepôt »).
Puis le bootstrap : uniquement CREATE SCHEMA / CREATE VOLUME IF NOT EXISTS sur <catalogue>.carburant_bronze,
carburant_silver, carburant_gold et carburant_bronze.depot, exécutés deux fois, avec SHOW SCHEMAS avant, après le run 1
et après le run 2. Les schémas bronze, silver, gold, ci_silver, ci_gold et default appartiennent à un autre projet
(EnergiScope) : ils ne sont jamais touchés. Arrêt à la première erreur, au plus 5 interrogations par requête, aucune relance.

Le SDK est importé seulement dans creer_client : le conteneur n'en a pas besoin. Les variables viennent de l'environnement
du processus (jamais d'un fichier lu ici). Rejeu : make bootstrap-controles puis make bootstrap.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path

SQL_BOOTSTRAP = Path(__file__).resolve().parents[1] / "databricks" / "bootstrap.sql"
SCHEMAS_AUTORISES = frozenset({"carburant_bronze", "carburant_silver", "carburant_gold"})
VARIABLES_SECRETES = ("DATABRICKS_HOST", "DATABRICKS_TOKEN")
NOMS_VARIABLES = ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_CATALOG", "DATABRICKS_WAREHOUSE_ID")
DELAI_CONNEXION = 30  # secondes : délai dur du test d'authentification, processus enfant tué au-delà
MAX_TENTATIVES = 5  # interrogations au plus par requête ou par contrôle (quota Free : aucune boucle ouverte)
PAUSE_ENTREPOT = 60  # secondes entre deux interrogations de l'entrepôt (5 x 60 s = 5 minutes au plus)
PAUSE_REQUETE = 10
ATTENTE_REQUETE = "50s"  # attente maximale autorisée par l'API avant de rendre la main
ETATS_ECHEC = ("FAILED", "CANCELED", "CLOSED")
# Exécuté dans un processus enfant : le SDK peut se bloquer (hôte injoignable), seul un processus se tue de façon fiable.
CODE_VERIFICATION = (
    "import os; from databricks.sdk import WorkspaceClient; "
    "WorkspaceClient(host=os.environ['DATABRICKS_HOST'], token=os.environ['DATABRICKS_TOKEN'], "
    "http_timeout_seconds=20, retry_timeout_seconds=20).current_user.me()"
)

_CATALOGUE = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,62}")
_SCHEMA_SIMPLE = re.compile(r"CREATE SCHEMA IF NOT EXISTS (\w+)", re.IGNORECASE)
_VOLUME_SIMPLE = re.compile(r"CREATE VOLUME IF NOT EXISTS (\w+)\.(\w+)", re.IGNORECASE)


class ErreurBootstrap(Exception):
    """Échec d'une requête : le message ne contient jamais de valeur sensible."""


# --- SQL et périmètre ------------------------------------------------------------------------------------------------


def lire_instructions(chemin: Path = SQL_BOOTSTRAP) -> list[str]:
    """Instructions du fichier SQL : commentaires `--` retirés, découpe sur `;`, espaces normalisés."""
    sans_commentaires = "\n".join(ligne.split("--", 1)[0] for ligne in chemin.read_text(encoding="utf-8").splitlines())
    return [" ".join(bloc.split()) for bloc in sans_commentaires.split(";") if bloc.strip()]


def catalogue_valide(nom: str) -> bool:
    return bool(_CATALOGUE.fullmatch(nom or ""))


def verifier_perimetre(instructions: list[str], catalogue: str | None = None) -> dict[str, list[str]]:
    """Refuse toute instruction hors CREATE SCHEMA/VOLUME IF NOT EXISTS sur les schémas du projet.

    Sans catalogue : noms simples (le fichier SQL). Avec catalogue : noms qualifiés <catalogue>.<schéma>, et seul ce
    catalogue est accepté. Rend ce qui est créé (schémas, volumes) sans le catalogue.
    """
    if catalogue is not None:
        if not catalogue_valide(catalogue):
            raise ValueError("catalogue invalide")
        prefixe = re.escape(catalogue) + r"\."
        schema_re = re.compile(rf"CREATE SCHEMA IF NOT EXISTS {prefixe}(\w+)", re.IGNORECASE)
        volume_re = re.compile(rf"CREATE VOLUME IF NOT EXISTS {prefixe}(\w+)\.(\w+)", re.IGNORECASE)
    else:
        schema_re, volume_re = _SCHEMA_SIMPLE, _VOLUME_SIMPLE
    cree: dict[str, list[str]] = {"schemas": [], "volumes": []}
    for instruction in instructions:
        schema = schema_re.fullmatch(instruction)
        volume = volume_re.fullmatch(instruction)
        if schema and schema.group(1).lower() in SCHEMAS_AUTORISES:
            cree["schemas"].append(schema.group(1).lower())
        elif volume and volume.group(1).lower() in SCHEMAS_AUTORISES:
            cree["volumes"].append(f"{volume.group(1).lower()}.{volume.group(2).lower()}")
        else:
            raise ValueError(f"instruction hors périmètre du bootstrap : {instruction[:80]}")
    return cree


def qualifier(instructions: list[str], catalogue: str) -> list[str]:
    """Noms qualifiés <catalogue>.<schéma> à partir des instructions simples, après contrôle du périmètre."""
    if not catalogue_valide(catalogue):
        raise ValueError("catalogue invalide")
    verifier_perimetre(instructions)
    qualifiees = []
    for instruction in instructions:
        debut, _, nom = instruction.rpartition(" ")
        qualifiees.append(f"{debut} {catalogue}.{nom}")
    return qualifiees


# --- variables et secrets --------------------------------------------------------------------------------------------


def lire_variables(environnement) -> dict[str, str]:
    """Les quatre variables Databricks présentes et non vides, espaces et guillemets de bord retirés."""
    lues = {}
    for nom in NOMS_VARIABLES:
        valeur = (environnement.get(nom) or "").strip()
        if len(valeur) >= 2 and valeur[0] == valeur[-1] and valeur[0] in "\"'":
            valeur = valeur[1:-1].strip()
        if valeur:
            lues[nom] = valeur
    return lues


def variables_manquantes(environnement, noms=NOMS_VARIABLES) -> list[str]:
    presentes = lire_variables(environnement)
    return [nom for nom in noms if nom not in presentes]


def sans_secret(message: str, environnement: dict[str, str]) -> str:
    """Remplace dans un message toute valeur des variables secrètes (hôte, jeton) par ***."""
    for nom in VARIABLES_SECRETES:
        valeur = environnement.get(nom)
        if valeur:
            message = message.replace(valeur, "***")
    return message


# --- contrôles -------------------------------------------------------------------------------------------------------


def controler_variables(environnement) -> str:
    """Contrôle 1 : toutes les variables présentes (la manquante est nommée) et un catalogue de forme valide."""
    manquantes = variables_manquantes(environnement)
    if manquantes:
        return "KO variable manquante : " + ", ".join(manquantes)
    if not catalogue_valide(lire_variables(environnement)["DATABRICKS_CATALOG"]):
        return "KO catalogue invalide"
    return "OK variables"


def controler_authentification(environnement, commande: list[str] | None = None, delai: float = DELAI_CONNEXION) -> str:
    """Contrôle 2, rapide : "OK authentification" ou "KO authentification" (ou la variable manquante nommée).

    La vérification tourne dans un processus enfant avec un délai dur : au-delà, il est tué et le résultat est KO.
    Les sorties de l'enfant sont jetées, donc aucun secret ne peut sortir.
    """
    manquantes = variables_manquantes(environnement, VARIABLES_SECRETES)
    if manquantes:
        return "KO variable manquante : " + ", ".join(manquantes)
    try:
        fini = subprocess.run(
            commande or [sys.executable, "-c", CODE_VERIFICATION],
            env={**environnement, **lire_variables(environnement)},
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=delai,
            check=False,
        )
    except (subprocess.TimeoutExpired, OSError):
        return "KO authentification"
    return "OK authentification" if fini.returncode == 0 else "KO authentification"


def creer_client(environnement):
    """Client du SDK avec des délais courts (aucune relance longue). Non testé hors connexion : aucun appel réseau ici."""
    from databricks.sdk import WorkspaceClient

    v = lire_variables(environnement)
    return WorkspaceClient(host=v["DATABRICKS_HOST"], token=v["DATABRICKS_TOKEN"], http_timeout_seconds=20, retry_timeout_seconds=20)


def _etat(reponse) -> str:
    etat = reponse.status.state
    return str(getattr(etat, "value", etat))


def controler_entrepot(client, entrepot_id: str, dormir=time.sleep, tentatives: int = MAX_TENTATIVES, pause: float = PAUSE_ENTREPOT) -> str:
    """Contrôle 3 : un SELECT 1, soumis une seule fois, puis scrutation bornée (PENDING = entrepôt en démarrage)."""
    api = client.statement_execution
    try:
        reponse = api.execute_statement(statement="SELECT 1", warehouse_id=entrepot_id, wait_timeout="0s")
        etat = _etat(reponse)
        if etat == "SUCCEEDED":
            return "OK entrepôt prêt"
        if etat in ETATS_ECHEC:
            return "KO entrepôt (échec)"
        for _ in range(tentatives):
            dormir(pause)
            reponse = api.get_statement(reponse.statement_id)
            etat = _etat(reponse)
            if etat == "SUCCEEDED":
                return "OK entrepôt prêt"
            if etat in ETATS_ECHEC:
                return "KO entrepôt (échec)"
    except Exception:  # noqa: BLE001 - le message du SDK peut contenir l'hôte ou le jeton : on ne le propage pas
        return "KO entrepôt (erreur)"
    return "KO entrepôt (délai dépassé)"


# --- exécution -------------------------------------------------------------------------------------------------------


def _executer(client, entrepot_id: str, instruction: str, dormir, pause: float):
    api = client.statement_execution
    reponse = api.execute_statement(statement=instruction, warehouse_id=entrepot_id, wait_timeout=ATTENTE_REQUETE)
    etat = _etat(reponse)
    for _ in range(MAX_TENTATIVES):
        if etat not in ("PENDING", "RUNNING"):
            break
        dormir(pause)
        reponse = api.get_statement(reponse.statement_id)
        etat = _etat(reponse)
    if etat in ("PENDING", "RUNNING"):
        raise ErreurBootstrap("délai dépassé")
    if etat != "SUCCEEDED":
        raise ErreurBootstrap("échec")
    return reponse


def lister_schemas(client, entrepot_id: str, catalogue: str, dormir, pause: float = PAUSE_REQUETE) -> list[str]:
    reponse = _executer(client, entrepot_id, f"SHOW SCHEMAS IN {catalogue}", dormir, pause)
    lignes = (reponse.result.data_array if reponse.result is not None else None) or []
    return sorted(str(ligne[0]) for ligne in lignes)


def lancer_bootstrap(client, entrepot_id: str, catalogue: str, sortie=print, dormir=time.sleep, pause: float = PAUSE_REQUETE) -> int:
    """Deux passages du SQL qualifié, SHOW SCHEMAS avant / après le run 1 / après le run 2. Rend 0 si tout est conforme."""
    try:
        instructions = qualifier(lire_instructions(), catalogue)
        verifier_perimetre(instructions, catalogue)
    except ValueError:
        sortie("KO : catalogue invalide ou instruction hors périmètre")
        return 1
    sortie(f"Instructions, exécutées deux fois, dans cet ordre (catalogue {catalogue}) :")
    for instruction in instructions:
        sortie(f"  {instruction}")
    nouveaux = set(SCHEMAS_AUTORISES)
    try:
        avant = lister_schemas(client, entrepot_id, catalogue, dormir, pause)
        sortie(f"Schémas avant : {', '.join(avant)}")
        apres: list[list[str]] = []
        for passage in (1, 2):
            for rang, instruction in enumerate(instructions, 1):
                try:
                    _executer(client, entrepot_id, instruction, dormir, pause)
                except ErreurBootstrap as erreur:
                    sortie(f"KO instruction {rang} ({instruction}) : {erreur}")
                    return 1
            apres.append(lister_schemas(client, entrepot_id, catalogue, dormir, pause))
            sortie(f"Schémas après le run {passage} : {', '.join(apres[-1])}")
            if passage == 1:
                if set(avant) - set(apres[0]):
                    sortie("KO : schémas existants modifiés (certains ont disparu)")
                    return 1
                crees = sorted(set(apres[0]) - set(avant))
                if set(crees) - nouveaux:
                    sortie("KO : schémas inattendus créés")
                    return 1
                sortie(f"OK : {len(crees)} schémas créés ({', '.join(crees) or 'déjà présents'}), les {len(avant)} existants inchangés")
    except ErreurBootstrap as erreur:
        sortie(f"KO SHOW SCHEMAS : {erreur}")
        return 1
    except Exception:  # noqa: BLE001 - jamais le message du SDK (hôte, jeton)
        sortie("KO : erreur du SDK pendant le bootstrap")
        return 1
    if apres[1] != apres[0]:
        sortie("KO : le run 2 diffère du run 1")
        return 1
    sortie("OK : run 2 identique au run 1")
    return 0


# --- ligne de commande -----------------------------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parseur = argparse.ArgumentParser(description="Contrôles et bootstrap Databricks (variables lues dans l'environnement).")
    parseur.add_argument("--controles", action="store_true", help="variables, authentification, entrepôt prêt")
    parseur.add_argument("--executer", action="store_true", help="les trois contrôles, puis le bootstrap (deux passages)")
    arguments = parseur.parse_args(argv)
    if not (arguments.controles or arguments.executer):
        parseur.print_usage()
        return 2
    environnement = dict(os.environ)
    resultat = controler_variables(environnement)
    print(resultat)
    if resultat.startswith("KO"):
        return 1
    resultat = controler_authentification(environnement)
    print(resultat)
    if resultat.startswith("KO"):
        return 1
    variables = lire_variables(environnement)
    client = creer_client(environnement)
    resultat = controler_entrepot(client, variables["DATABRICKS_WAREHOUSE_ID"])
    print(resultat)
    if resultat.startswith("KO"):
        return 1
    if arguments.executer:
        return lancer_bootstrap(client, variables["DATABRICKS_WAREHOUSE_ID"], variables["DATABRICKS_CATALOG"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
