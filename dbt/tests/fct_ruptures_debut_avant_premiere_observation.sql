-- Test de fuseau permanent (J8, 9b) : le début d'une rupture ne peut pas suivre sa première observation par la
-- collecte. Un début lu en UTC alors qu'il est en heure de Paris donnait 5 268 événements en écart (J8-M4).
-- Une ligne rendue = test en échec.
{{ config(severity='error') }}

select rupture_id, rupture_debut_ts, premiere_observation_ts
from {{ ref('fct_ruptures') }}
where rupture_debut_ts > premiere_observation_ts
