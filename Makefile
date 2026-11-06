.PHONY: up down pause reprise test test-ingestion test-bootstrap collecte-run rehash-etats mesure-carburants

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

test: test-ingestion

# Dans le conteneur. Les tests du bootstrap Databricks tournent sur l'hôte (make test-bootstrap) : databricks/ n'est pas monté.
test-ingestion:
	docker compose exec -w /opt/airflow/project airflow python -m pytest -q -p no:cacheprovider --ignore=ingestion/tests/test_bootstrap_databricks.py ingestion quality

# Sur l'hôte, dans le venv .venv (Python 3.14.3) : python -m venv .venv puis .venv/Scripts/pip install -r requirements-databricks.txt
VENV_PYTHON = .venv/Scripts/python.exe
test-bootstrap:
	"$(VENV_PYTHON)" -m pytest -q -p no:cacheprovider ingestion/tests/test_bootstrap_databricks.py

collecte-run:
	docker compose exec airflow airflow dags test collecte_flux

# Dans le conteneur airflow (Postgres n'est pas exposé sur l'hôte). Simulation par défaut :
#   make rehash-etats                   simulation, n'écrit rien
#   make rehash-etats ARGS=--confirmer  écrit le recalcul
rehash-etats:
	docker compose exec -w /opt/airflow/project airflow python -m ingestion.rehash $(ARGS)

# Mesures de la fiche carburants, dans le conteneur : make mesure-carburants M=m1 (m1 à m5, m8, avec m4b et m4c)
mesure-carburants:
	docker compose exec -w /opt/airflow/project airflow python -m ingestion.mesures_carburants $(M)
