-- Curseur d'export vers Databricks : une ligne par table exportée et par source.
-- Hors de ingestion/sql : ensure_schema exécute tout ce dossier sur la vraie base à chaque run de collecte.
-- Cette table n'est créée que par creer_table_curseur, appelée à la demande.
CREATE TABLE IF NOT EXISTS export_curseur (
    table_source TEXT        NOT NULL,
    source       TEXT        NOT NULL,
    curseur      TIMESTAMPTZ NOT NULL,
    mis_a_jour   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_source, source)
);
