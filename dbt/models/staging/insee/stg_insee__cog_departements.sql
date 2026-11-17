-- COG des départements, millésime fixé par la variable cog_millesime.
with source as (

    select * from {{ source('insee', 'ref_cog_departement') }}
    where millesime = {{ var('cog_millesime') }}

)

select
    dep as departement_code,
    libelle as departement_nom,
    reg as region_code,
    cheflieu as cheflieu_commune_code,
    millesime as cog_millesime,
    charge_ts as postgres_charge_ts,
    _charge_ts as charge_ts
from source
