-- Filosofi par commune ou arrondissement, millésime fixé par la variable filosofi_millesime.
-- Valeurs et statuts gardés tels quels : une valeur masquée (statut C) reste NULL.
with source as (

    select * from {{ source('insee', 'ref_filosofi_commune') }}
    where millesime = {{ var('filosofi_millesime') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['geo_object', 'codgeo']) }} as filosofi_cle,
    millesime as filosofi_millesime,
    geo_object,
    codgeo as commune_code,
    med_sl,
    med_sl_statut,
    med_sl_conf,
    pr_md60,
    pr_md60_statut,
    pr_md60_conf,
    charge_ts as postgres_charge_ts,
    _charge_ts as charge_ts
from source
