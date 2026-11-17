-- Rattachement station commune (com_arm_code du flux quotidien) : renommer. Aucune jointure.
-- charge_ts : heure du chargement dans le bronze (même sens que stg_carburants__prix) ; postgres_charge_ts : base de collecte.
with source as (

    select * from {{ source('carburant_bronze', 'ref_station_commune') }}

)

select
    station_id,
    com_arm_code as commune_arm_code,
    fichier_date,
    charge_ts as postgres_charge_ts,
    _charge_ts as charge_ts
from source
