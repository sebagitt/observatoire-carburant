"""Tâches dbt du futur DAG transformation_dbt (logique testable sans Airflow ni dbt ; le DAG ne fera que les appeler).

commande_dbt : `dbt build --select` des quatre modèles de la chaîne (parents en table et enfants ensemble, J9), sans
  aucune option de chemin : les chemins passent par l'environnement.
environnement_dbt : copie de l'environnement reçu (DATABRICKS_* gardées, dbt les lit par env_var) plus les chemins dbt ;
  target et logs sous /tmp, jamais dans le projet monté en lecture seule.
masquer : remplace par *** les valeurs non vides de l'hôte (avec et sans https://), du jeton et de l'entrepôt.
lancer_dbt : exécute la commande sans shell, passe chaque ligne de sortie par masquer ; ErreurDbt si le code est non nul,
  avec le seul code dans le message (aucune ligne de sortie). Délai DELAI_DBT_S (25 min, sous l'execution_timeout de
  30 min du DAG) : au-delà, ErreurDbt avec le seul délai dans le message ; la sortie partielle n'est ni affichée ni gardée.
"""
from __future__ import annotations

import subprocess

SELECTION_CHAINE = ("dim_station", "fct_ruptures", "fct_prix", "mart_prix_commune_jour")
DBT_EXE = "/opt/dbt-venv/bin/dbt"
PROJET_DBT = "/opt/airflow/dbt"
RACINE_TMP = "/tmp/dbt"
DELAI_DBT_S = 25 * 60
NOMS_SECRETS = ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID")


class ErreurDbt(RuntimeError):
    """dbt a rendu un code non nul."""


def commande_dbt(selection=SELECTION_CHAINE, executable=DBT_EXE) -> list[str]:
    return [executable, "build", "--select", *selection]


def environnement_dbt(base, projet=PROJET_DBT, racine_tmp=RACINE_TMP) -> dict[str, str]:
    env = dict(base)
    env["DBT_PROJECT_DIR"] = projet
    env["DBT_PROFILES_DIR"] = projet
    env["DBT_TARGET_PATH"] = racine_tmp + "/target"
    env["DBT_LOG_PATH"] = racine_tmp + "/logs"
    env["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    return env


def _valeurs_a_masquer(base) -> list[str]:
    valeurs = [base.get(nom, "") for nom in NOMS_SECRETS]
    hote = base.get("DATABRICKS_HOST", "")
    valeurs.append(hote.removeprefix("https://").rstrip("/"))
    # Les plus longues d'abord : l'hôte complet est remplacé en entier avant sa forme courte.
    return sorted({v for v in valeurs if v}, key=len, reverse=True)


def masquer(ligne, base) -> str:
    for valeur in _valeurs_a_masquer(base):
        ligne = ligne.replace(valeur, "***")
    return ligne


def lancer_dbt(commande, env, executer=subprocess.run, sortie=print, delai_s=DELAI_DBT_S) -> None:
    try:
        resultat = executer(commande, env=env, capture_output=True, text=True, check=False, timeout=delai_s)
    except subprocess.TimeoutExpired:
        raise ErreurDbt(f"dbt dépassé ({delai_s} s)") from None
    for flux in (resultat.stdout, resultat.stderr):
        for ligne in (flux or "").splitlines():
            sortie(masquer(ligne, env))
    code = resultat.returncode
    if code != 0:
        raise ErreurDbt(f"dbt en échec (code {code})") from None
