-- Versions successives de l'état d'une station (attributs, services, ruptures), hors prix.
-- Une ligne n'est écrite que si l'état diffère du dernier état connu de la station.
CREATE TABLE IF NOT EXISTS station_etat_flux (
    station_id  BIGINT      NOT NULL,
    collecte_ts TIMESTAMPTZ NOT NULL,
    etat_hash   TEXT        NOT NULL,
    etat        JSONB       NOT NULL,
    PRIMARY KEY (station_id, collecte_ts)
);
