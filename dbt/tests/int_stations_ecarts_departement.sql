-- Cohérence des départements : station dont le département du dernier état du flux diffère de celui du COG.
-- STOP 0 du 17/11 : 0 écart sur 9 829 stations comparées. Warn dès un écart. Une ligne par station en écart.
{{ config(severity='warn') }}

select station_id, departement_code_flux, departement_code_cog, commune_arm_code
from {{ ref('int_stations__commune') }}
where departement_code_flux <> departement_code_cog
