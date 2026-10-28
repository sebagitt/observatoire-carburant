CREATE TABLE IF NOT EXISTS prix_flux (
    station_id  BIGINT        NOT NULL,
    carburant   TEXT          NOT NULL,
    maj         TIMESTAMPTZ   NOT NULL,
    prix        NUMERIC(6, 3) NOT NULL,
    collecte_ts TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (station_id, carburant, maj)
);

CREATE INDEX IF NOT EXISTS idx_prix_flux_collecte_ts ON prix_flux (collecte_ts);
