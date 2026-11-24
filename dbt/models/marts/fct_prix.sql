-- Une version de prix par (station, carburant, maj) ; fin = maj de la version suivante.
with prix as (
    select prix_id, station_id, carburant, prix_maj_ts, prix_eur_litre, collecte_ts
    from {{ ref('stg_carburants__prix') }}
),

communes as (
    select station_id, commune_code
    from {{ ref('int_stations__commune') }}
),

versions as (
    select
        prix_id as prix_version_id,
        station_id,
        carburant,
        prix_eur_litre,
        prix_maj_ts as valide_depuis_ts,
        lead(prix_maj_ts) over (
            partition by station_id, carburant
            order by prix_maj_ts
        ) as valide_jusqu_ts,
        collecte_ts as premiere_observation_ts
    from prix
)

select
    v.prix_version_id,
    v.station_id,
    v.carburant,
    v.prix_eur_litre,
    v.valide_depuis_ts,
    v.valide_jusqu_ts,
    v.valide_jusqu_ts is null as est_courant,
    v.prix_eur_litre between {{ var('prix_plausible_min') }}
        and {{ var('prix_plausible_max') }} as est_plausible,
    v.premiere_observation_ts,
    c.commune_code
from versions as v
left join communes as c
    on v.station_id = c.station_id
