-- Couverture du rattachement : part des stations de stg_carburants__station_etats avec un commune_arm_code.
-- STOP 0 du 17/11 : 99,777 % ; seuil validé 99,27 % (mesure moins 0,5 point, arrondi vers le bas). Error en dessous.
-- Une ligne rendue = test en échec.
{{ config(severity='error') }}

with mesure as (

    select
        count(*) as stations,
        count(commune_arm_code) as couvertes,
        count(commune_arm_code) * 100.0 / count(*) as taux_pct
    from {{ ref('int_stations__commune') }}

)

select stations, couvertes, taux_pct
from mesure
where taux_pct < 99.27
