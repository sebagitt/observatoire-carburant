{{ config(group='carburants') }}
-- Exactement une version courante par station de dim_station, et autant de stations courantes que de stations
-- distinctes dans le staging (D9 : dernière version connue). Une ligne rendue = test en échec.
with courantes as (

    select station_id, count_if(est_courante) as nb_courantes
    from {{ ref('dim_station') }}
    group by station_id

),

comptes as (

    select
        (select count(*) from courantes where nb_courantes = 1) as stations_courantes,
        (select count(distinct station_id) from {{ ref('stg_carburants__station_etats') }}) as stations_staging

)

select 'station sans version courante unique' as anomalie, cast(station_id as string) as detail
from courantes
where nb_courantes <> 1

union all

select 'stations courantes différentes du staging' as anomalie,
    concat(cast(stations_courantes as string), ' contre ', cast(stations_staging as string)) as detail
from comptes
where stations_courantes <> stations_staging
