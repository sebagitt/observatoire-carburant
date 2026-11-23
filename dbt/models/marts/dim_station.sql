-- SCD2 des attributs descriptifs des stations (D5 à D10 de J7), reconstruit à chaque build depuis le journal des
-- versions du staging, par îlots : une nouvelle version quand l'empreinte des attributs suivis change.
-- Ruptures, horaires et carburants disponibles n'entrent pas dans l'empreinte (événements, pas attributs).
-- services en forme canonique : tableau trié (D6) ; s'il n'est pas lisible en tableau, le texte brut entre dans
-- l'empreinte. NULL remplacé par la sentinelle '~'. Même empreinte que la mesure M4 (attendu du nombre de lignes).
with etats as (

    select
        station_id,
        collecte_ts,
        adresse,
        code_postal,
        ville,
        code_departement,
        departement_nom,
        code_region,
        region_nom,
        latitude,
        longitude,
        type_route_code,
        automate_24_24,
        services_json,
        array_sort(from_json(services_json, 'array<string>')) as services,
        charge_ts
    from {{ ref('stg_carburants__station_etats') }}

),

empreintes as (

    select
        *,
        sha2(
            concat_ws('#',
                concat_ws('|',
                    coalesce(cast(adresse as string), '~'),
                    coalesce(cast(code_postal as string), '~'),
                    coalesce(cast(ville as string), '~'),
                    coalesce(cast(code_departement as string), '~'),
                    coalesce(cast(departement_nom as string), '~'),
                    coalesce(cast(code_region as string), '~'),
                    coalesce(cast(region_nom as string), '~'),
                    coalesce(cast(latitude as string), '~'),
                    coalesce(cast(longitude as string), '~'),
                    coalesce(cast(type_route_code as string), '~'),
                    coalesce(cast(automate_24_24 as string), '~')
                ),
                coalesce(array_join(services, '|'), services_json, '~')
            ),
            256
        ) as empreinte
    from etats

),

marques as (

    select
        *,
        case
            when lag(empreinte) over (partition by station_id order by collecte_ts) is null then 1
            when lag(empreinte) over (partition by station_id order by collecte_ts) <> empreinte then 1
            else 0
        end as est_nouvelle_version
    from empreintes

),

numerotees as (

    select
        *,
        sum(est_nouvelle_version) over (
            partition by station_id order by collecte_ts rows between unbounded preceding and current row
        ) as version_num
    from marques

),

-- première ligne de chaque îlot : ses attributs, son collecte_ts comme début de validité
versions as (

    select * from numerotees
    where est_nouvelle_version = 1

),

bornees as (

    select
        *,
        lead(collecte_ts) over (partition by station_id order by collecte_ts) as fin_validite_ts
    from versions

)

select
    {{ dbt_utils.generate_surrogate_key(['station_id', 'collecte_ts']) }} as station_version_id,
    station_id,
    cast(version_num as bigint) as version_num,
    adresse,
    code_postal,
    ville,
    code_departement,
    departement_nom,
    code_region,
    region_nom,
    latitude,
    longitude,
    type_route_code,
    automate_24_24,
    services,
    collecte_ts as debut_validite_ts,
    fin_validite_ts,
    fin_validite_ts is null as est_courante,
    charge_ts
from bornees
