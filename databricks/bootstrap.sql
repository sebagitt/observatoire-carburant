-- Bootstrap Unity Catalog : schémas du projet et zone de dépôt.
-- Le catalogue (workspace) n'est jamais créé ici : il est fixé par DATABRICKS_CATALOG au moment de l'exécution.
-- Idempotent : relançable sans erreur et sans effet. Ne touche à aucun autre schéma (ci_silver, ci_gold, default...).
CREATE SCHEMA IF NOT EXISTS carburant_bronze;
CREATE SCHEMA IF NOT EXISTS carburant_silver;
CREATE SCHEMA IF NOT EXISTS carburant_gold;
CREATE VOLUME IF NOT EXISTS carburant_bronze.depot;
