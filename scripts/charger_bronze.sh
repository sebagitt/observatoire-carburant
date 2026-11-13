#!/usr/bin/env bash
# Lancé dans un conteneur jetable (voir make charger-bronze) : installe le SDK Databricks à la volée dans la couche
# jetable du conteneur (l'image et le conteneur de collecte ne sont pas touchés), vérifie sa version, puis lance
# python -m ingestion.charger_bronze avec les arguments reçus. Les variables DATABRICKS_* viennent de l'environnement
# du conteneur (passées par nom) : ce script ne les lit ni ne les affiche.
set -u
t0=$(date +%s)
pip install -q --no-cache-dir databricks-sdk==0.156.0 -c https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.12.txt || exit 10
echo "INSTALLATION_OK $(( $(date +%s) - t0 )) s"
python -c 'import sys, databricks.sdk as s; v = s.version.__version__; print(v, sys.prefix); sys.exit(0 if v == "0.156.0" else 1)' || exit 11
t1=$(date +%s)
python -m ingestion.charger_bronze "$@"
r=$?
echo "CHARGEMENT $(( $(date +%s) - t1 )) s code $r"
exit $r
