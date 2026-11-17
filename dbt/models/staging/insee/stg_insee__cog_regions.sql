-- COG des régions, millésime fixé par la variable cog_millesime.
with source as (

    select * from {{ source('insee', 'ref_cog_region') }}
    where millesime = {{ var('cog_millesime') }}

)

select
    reg as region_code,
    libelle as region_nom,
    cheflieu as cheflieu_commune_code,
    millesime as cog_millesime,
    charge_ts as postgres_charge_ts,
    _charge_ts as charge_ts
from source
