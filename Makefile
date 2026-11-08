.PHONY: up down pause reprise test test-tout test-ingestion test-bootstrap bootstrap bootstrap-controles charger-cog collecte-run rehash-etats mesure-carburants

up:
	docker compose up -d --build

down:
	docker compose down

# Coupure normale : arrête les conteneurs sans les supprimer (métadonnées Airflow et volume conservés).
pause:
	docker compose stop

# Relance sans reconstruire ni supprimer, puis verdict (voir scripts/reprise.sh).
# Le bash de Git for Windows est déduit de git : depuis PowerShell, "bash" du PATH est le lanceur WSL.
GIT_BASH = "$(shell git --exec-path)/../../../bin/bash.exe"
reprise:
	$(GIT_BASH) scripts/reprise.sh

# make test : tests du conteneur. make test-tout : conteneur et hôte (bootstrap Databricks) = critère vert de J3.
test: test-ingestion

test-tout: test-ingestion test-bootstrap

# Dans le conteneur. Les tests du bootstrap Databricks tournent sur l'hôte (make test-bootstrap) : databricks/ n'est pas monté.
test-ingestion:
	docker compose exec -w /opt/airflow/project airflow python -m pytest -q -p no:cacheprovider --ignore=ingestion/tests/test_bootstrap_databricks.py ingestion quality

# Sur l'hôte, dans le venv .venv (Python 3.14.3) : python -m venv .venv puis .venv/Scripts/pip install -r requirements-databricks.txt
VENV_PYTHON = .venv/Scripts/python.exe
test-bootstrap:
	"$(VENV_PYTHON)" -m pytest -q -p no:cacheprovider ingestion/tests/test_bootstrap_databricks.py

# Bootstrap Databricks, sur l'hôte. Les quatre variables DATABRICKS_* viennent de l'environnement du processus : pour ces deux
# cibles seulement, make inclut .env et exporte ces quatre noms (le script Python ne lit jamais de fichier).
#   make bootstrap-controles   variables, authentification, entrepôt prêt (aucune écriture)
#   make bootstrap             les trois contrôles, puis le SQL deux fois avec SHOW SCHEMAS avant, après le run 1 et le run 2
ifneq ($(filter bootstrap bootstrap-controles,$(MAKECMDGOALS)),)
-include .env
export DATABRICKS_HOST DATABRICKS_TOKEN DATABRICKS_CATALOG DATABRICKS_WAREHOUSE_ID
# Sortie en UTF-8 (sinon Python sous Windows écrit en cp1252 et les accents s'affichent mal dans un terminal ou un tube UTF-8).
export PYTHONUTF8 := 1
endif

bootstrap-controles:
	"$(VENV_PYTHON)" -m ingestion.bootstrap_databricks --controles

bootstrap:
	"$(VENV_PYTHON)" -m ingestion.bootstrap_databricks --executer

collecte-run:
	docker compose exec airflow airflow dags test collecte_flux

# Dans le conteneur airflow (Postgres n'est pas exposé sur l'hôte). Simulation par défaut :
#   make rehash-etats                   simulation, n'écrit rien
#   make rehash-etats ARGS=--confirmer  écrit le recalcul
rehash-etats:
	docker compose exec -w /opt/airflow/project airflow python -m ingestion.rehash $(ARGS)

# Référentiel COG 2026 : télécharge les 3 CSV de l'INSEE dans data/referentiels/cog/2026, valide (GX) puis charge ref_cog_*
# dans la base collecte (une transaction, rejouable sans effet). Dans le conteneur airflow.
charger-cog:
	docker compose exec -w /opt/airflow/project airflow python -m ingestion.referentiels cog --telecharger

# Mesures de la fiche carburants, dans le conteneur : make mesure-carburants M=m1 (m1 à m5, m8, m9, avec m4b et m4c)
mesure-carburants:
	docker compose exec -w /opt/airflow/project airflow python -m ingestion.mesures_carburants $(M)
