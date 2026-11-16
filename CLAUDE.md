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

## Exécution et rapports (ajout du 16/11)
- Rapports : sorties brutes sans valeur sensible ; étiquettes [fait] avec la preuve collée, [hypothèse], [non vérifié] ; liste finale de ce qui n'a pas pu être vérifié.
- Un attendu donné par le chat n'est jamais recopié : le recalculer à partir d'une mesure.
- Accord groupé : une liste fermée d'actions vaut accord pour ces actions seulement, dans l'ordre. Arrêt immédiat à un échec ou à un comptage qui diffère.
- Mémoire du conteneur airflow, deux seuils distincts : alerte à 3,5 Gio (relever et chercher la cause) ; arrêt à 4 Gio (aucun nouveau lot charger-bronze).
- Lancements réels vers Databricks : make charger-bronze seulement. Avant chaque lot : lire le total de la table, calculer l'attendu (total lu plus COUNT Postgres de la plage).
- Aucun commit sans accord après diff montré. Aucun push sans accord explicite ; avant un push, contrôles de la section 1 du HANDOFF.
- Un test vert ne prouve pas qu'il protège une règle : mutation d'une ligne dans une copie hors dépôt, le test doit passer du vert au rouge.