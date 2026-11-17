-- COG des communes, millésime fixé par la variable cog_millesime. Renommer, filtrer le millésime ; aucune jointure.
-- Grain (millesime, typecom, com) : un même code peut être COM et COMD (écart 5 du 17/11, 449 codes de stations).
with source as (

    select * from {{ source('insee', 'ref_cog_commune') }}
    where millesime = {{ var('cog_millesime') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['millesime', 'typecom', 'com']) }} as commune_cle,
    millesime as cog_millesime,
    com as commune_code,
    typecom as commune_type,
    comparent as commune_parent_code,
    libelle as commune_nom,
    dep as departement_code,
    reg as region_code,
    charge_ts as postgres_charge_ts,
    _charge_ts as charge_ts
from source
