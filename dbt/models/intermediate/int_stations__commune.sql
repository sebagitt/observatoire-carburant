-- Rattachement des stations du flux instantané à une commune du COG (com_arm_code du flux quotidien, option D).
-- Grain : station_id distinct de stg_carburants__station_etats.
-- Résolution (écart 6) : code COM ou ARM trouvé tel quel ; sinon, parent d'un COMD ou d'un COMA. Un code à la fois COM
-- et COMD garde donc le COM. Lit stg_insee__cog_communes (écart 9) : dim_commune n'a ni COMD ni COMA.
with etats as (

    select * from {{ ref('stg_carburants__station_etats') }}

),

dernier_etat as (

    select station_id, code_departement as departement_code_flux
    from etats
    qualify row_number() over (partition by station_id order by collecte_ts desc) = 1

),

rattachements as (

    select * from {{ ref('stg_carburants__station_communes') }}

),

cog as (

    select * from {{ ref('stg_insee__cog_communes') }}

),

com_arm as (

    select commune_code, commune_type, commune_parent_code, departement_code
    from cog
    where commune_type in ('COM', 'ARM')

),

comd_coma as (

    select commune_code, commune_parent_code
    from cog
    where commune_type in ('COMD', 'COMA')

),

resolu as (

    select
        e.station_id,
        e.departement_code_flux,
        coalesce(direct.commune_code, delegue.commune_parent_code) as commune_arm_code
    from dernier_etat as e
    left join rattachements as r
        on r.station_id = e.station_id
    left join com_arm as direct
        on direct.commune_code = r.commune_arm_code
    left join comd_coma as delegue
        on delegue.commune_code = r.commune_arm_code
        and direct.commune_code is null

)

select
    r.station_id,
    ca.commune_code as commune_arm_code,
    case when ca.commune_type = 'ARM' then ca.commune_parent_code else ca.commune_code end as commune_code,
    case when ca.commune_code is not null then 'flux_quotidien' end as rattachement_source,
    r.departement_code_flux,
    ca.departement_code as departement_code_cog
from resolu as r
left join com_arm as ca
    on ca.commune_code = r.commune_arm_code
