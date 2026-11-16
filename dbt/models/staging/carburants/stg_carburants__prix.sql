-- Prix du bronze : renommer, typer. Aucune jointure, aucune règle métier.
-- maj est déjà en UTC (corrigée à l'ingestion) : aucune conversion de fuseau.
with source as (

    select * from {{ source('carburant_bronze', 'prix_flux') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['station_id', 'carburant', 'maj']) }} as prix_id,
    station_id,
    carburant,
    maj as prix_maj_ts,
    cast(prix as decimal(6, 3)) as prix_eur_litre,
    collecte_ts,
    _fichier_source as fichier_source,
    _charge_ts as charge_ts
from source
