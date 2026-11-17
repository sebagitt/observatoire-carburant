-- Une ligne par commune (COM) ou arrondissement municipal (ARM) du COG ; COMA et COMD exclus (ils se rattachent à leur
-- parent, géré dans int_stations__commune). Un code à la fois COM et COMD ne garde donc que sa ligne COM.
-- Filosofi joint sur (geo_object = commune_type, codgeo = commune_code) : référentiel géographique au 1er janvier 2026,
-- le même que le COG chargé (fiche filosofi). Sans ligne Filosofi ou valeur masquée (statut C) : NULL, statut gardé.
with communes as (

    select * from {{ ref('stg_insee__cog_communes') }}
    where commune_type in ('COM', 'ARM')

),

departements as (

    select * from {{ ref('stg_insee__cog_departements') }}

),

regions as (

    select * from {{ ref('stg_insee__cog_regions') }}

),

filosofi as (

    select * from {{ ref('stg_insee__filosofi_communes') }}

)

select
    c.commune_code,
    c.commune_type,
    c.commune_nom,
    case when c.commune_type = 'ARM' then c.commune_parent_code else c.commune_code end as commune_parent_code,
    c.departement_code,
    d.departement_nom,
    c.region_code,
    r.region_nom,
    f.med_sl as niveau_vie_median_eur,
    f.med_sl_statut as niveau_vie_median_statut,
    f.pr_md60 as taux_pauvrete_pct,
    f.pr_md60_statut as taux_pauvrete_statut,
    f.filosofi_millesime,
    c.cog_millesime
from communes as c
left join departements as d
    on d.departement_code = c.departement_code
left join regions as r
    on r.region_code = c.region_code
left join filosofi as f
    on f.geo_object = c.commune_type
    and f.commune_code = c.commune_code
