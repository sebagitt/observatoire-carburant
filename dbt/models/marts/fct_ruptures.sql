-- Événements de rupture : une ligne par (station, carburant, rupture_debut_ts), reconstruits depuis le journal des
-- versions. La fin observée est le collecte_ts de la première version de la station postérieure à la dernière
-- observation de l'événement : le journal crée une version à chaque changement d'état, ruptures comprises, donc
-- cette version ne porte plus l'événement. Précision au quart d'heure, sauf pendant les trous de collecte.
-- rupture_type : dernier type observé (0 changement de type mesuré, J8-M4 ; test singulier en warn).
with observations as (

    select * from {{ ref('int_ruptures__observations') }}

),

versions as (

    select station_id, collecte_ts from {{ ref('stg_carburants__station_etats') }}

),

communes as (

    select station_id, commune_code from {{ ref('int_stations__commune') }}

),

evenements as (

    select
        station_id,
        carburant,
        rupture_debut_ts,
        max_by(rupture_type, collecte_ts) as rupture_type,
        min(collecte_ts) as premiere_observation_ts,
        max(collecte_ts) as derniere_observation_ts
    from observations
    group by station_id, carburant, rupture_debut_ts

),

fins as (

    select
        e.station_id,
        e.carburant,
        e.rupture_debut_ts,
        min(v.collecte_ts) as fin_observee_ts
    from evenements as e
    inner join versions as v
        on v.station_id = e.station_id
        and v.collecte_ts > e.derniere_observation_ts
    group by e.station_id, e.carburant, e.rupture_debut_ts

)

select
    {{ dbt_utils.generate_surrogate_key(['e.station_id', 'e.carburant', 'e.rupture_debut_ts']) }} as rupture_id,
    e.station_id,
    e.carburant,
    e.rupture_debut_ts,
    e.rupture_type,
    e.premiere_observation_ts,
    e.derniere_observation_ts,
    f.fin_observee_ts,
    f.fin_observee_ts is null as est_en_cours,
    c.commune_code
from evenements as e
left join fins as f
    on f.station_id = e.station_id
    and f.carburant = e.carburant
    and f.rupture_debut_ts = e.rupture_debut_ts
left join communes as c
    on c.station_id = e.station_id
