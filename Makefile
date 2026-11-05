.PHONY: up down test test-ingestion collecte-run rehash-etats

up:
	docker compose up -d --build

down:
	docker compose down

test: test-ingestion

test-ingestion:
	docker compose exec -w /opt/airflow/project airflow python -m pytest -q -p no:cacheprovider ingestion quality

collecte-run:
	docker compose exec airflow airflow dags test collecte_flux

# Dans le conteneur airflow (Postgres n'est pas exposé sur l'hôte). Simulation par défaut :
#   make rehash-etats                   simulation, n'écrit rien
#   make rehash-etats ARGS=--confirmer  écrit le recalcul
rehash-etats:
	docker compose exec -w /opt/airflow/project airflow python -m ingestion.rehash $(ARGS)
