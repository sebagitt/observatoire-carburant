.PHONY: up down test test-ingestion collecte-run

up:
	docker compose up -d --build

down:
	docker compose down

test: test-ingestion

test-ingestion:
	docker compose exec -w /opt/airflow/project airflow python -m pytest -q -p no:cacheprovider ingestion quality

collecte-run:
	docker compose exec airflow airflow dags test collecte_flux
