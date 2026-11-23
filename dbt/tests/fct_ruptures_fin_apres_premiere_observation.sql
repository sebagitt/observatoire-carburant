-- La fin observée d'une rupture ne précède jamais sa première observation. Une ligne rendue = test en échec.
{{ config(severity='error') }}

select rupture_id, premiere_observation_ts, fin_observee_ts
from {{ ref('fct_ruptures') }}
where fin_observee_ts < premiere_observation_ts
