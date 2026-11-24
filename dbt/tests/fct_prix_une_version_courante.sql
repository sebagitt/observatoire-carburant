-- Au plus une version courante par (station, carburant). Une ligne rendue = test en échec.
select station_id, carburant, count(*) as courantes from {{ ref('fct_prix') }}
where est_courant group by station_id, carburant having count(*) > 1
