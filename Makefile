.PHONY: up down pause reprise test test-ingestion collecte-run rehash-etats mesure-carburants

up:
	docker compose up -d --build

down:
	docker compose down

# Coupure normale : arrête les conteneurs sans les supprimer (métadonnées Airflow et volume conservés).
pause:
	docker compose stop

# Relance sans reconstruire ni supprimer, puis verdict (voir scripts/reprise.sh).
reprise:
	bash scripts/reprise.sh

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

# Mesures de la fiche carburants, dans le conteneur : make mesure-carburants M=m1 (m1 à m4)
mesure-carburants:
	docker compose exec -w /opt/airflow/project airflow python -m ingestion.mesures_carburants $(M)
