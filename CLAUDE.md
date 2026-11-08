# Observatoire carburant

## Contexte
Pipeline analytique sur les prix carburants (flux open data) et leur exposition territoriale.
Stack : Python, PostgreSQL, Airflow (Docker), Great Expectations, Databricks Free (Unity Catalog), dbt Core (dbt-databricks), GitHub Actions, Qlik, agent text-to-SQL.

## Règles
- Réponses, docs et commits en français. Jamais de tirets cadratin.
- Commits conventionnels, petits, un sujet par commit.
- Aucun secret dans le repo. Variables dans .env (ignoré par git).
- Toute fonctionnalité arrive avec ses tests et la commande pour les rejouer.
- Pas de code avant que la tâche du jour soit lue dans docs/journal ou HANDOFF.md.

## Architecture
collecte (Airflow) -> GX -> Postgres collecte -> chargement -> carburant_bronze -> carburant_silver -> carburant_gold -> Qlik et agent.
Databricks Free : serverless uniquement, 1 SQL warehouse 2X-Small, dbt threads entre 2 et 4.

## dbt
- Couches : staging (private), intermediate, marts (public, contract enforced).
- Tests minimum : unique et not_null sur chaque clé, relationships sur chaque jointure.
- Unit tests sur toute logique métier. Voir le skill dbt-conventions.

## Qualité
- GX valide chaque lot avant chargement. dbt valide les transformations. Les contracts valident l'interface.

## Définition de terminé
- Tests verts (make test), debrief du jour écrit, HANDOFF.md à jour, commit poussé.

## Compact Instructions
Conserver : décisions d'architecture, schémas de tables, commandes qui marchent, erreurs résolues, tâche en cours.
Oublier : sorties de logs longues, essais abandonnés.

## Pédagogie et leçons (ajout du 08/11)
- Avant une commande Docker, Airflow ou dbt qui modifie un état (up, restart, stop, exec avec écriture, dbt run ou build, dags unpause), ou à la première apparition d'une commande dans la séance : une à deux lignes en français qui disent ce qu'elle fait et pourquoi. Pas pour les lectures répétées (ps, logs, stats, SELECT).
- Après une correction dont la cause est comprise : une entrée de trois lignes dans
  docs/journal/learnings.md (titre : date, thème ; symptôme ; cause racine étiquetée établie, hypothèse ou inconnue ; correction et test). Jamais une cause présentée comme établie sans preuve.
- Avant d'exécuter les consignes d'un message qui cite des étapes déjà faites, recouper avec git log et l'état réel ; en cas de doute, demander.
- Aucun commit ni push de docs/fiches-source sans accord explicite.