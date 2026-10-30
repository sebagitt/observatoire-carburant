CREATE TABLE IF NOT EXISTS passage_flux (
    collecte_ts      TIMESTAMPTZ PRIMARY KEY,
    run_planifie     TIMESTAMPTZ,
    lignes_lues      INTEGER     NOT NULL,
    lignes_inserees  INTEGER     NOT NULL,
    etats_changes    INTEGER
);
