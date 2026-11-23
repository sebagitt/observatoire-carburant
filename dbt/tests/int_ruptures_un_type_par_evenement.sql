-- Un seul type (definitive ou temporaire) par événement de rupture (station, carburant, début) : 0 changement de
-- type mesuré au STOP 0 de J8 (J8-M4). Warn : fct_ruptures garde le dernier type observé. Une ligne par événement.
{{ config(severity='warn') }}

select station_id, carburant, rupture_debut_ts, count(distinct rupture_type) as nb_types
from {{ ref('int_ruptures__observations') }}
group by station_id, carburant, rupture_debut_ts
having count(distinct rupture_type) > 1
