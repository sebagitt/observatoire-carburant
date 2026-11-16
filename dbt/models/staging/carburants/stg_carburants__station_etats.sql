-- États des stations du bronze : extraire les champs du JSON etat (chaîne), renommer, typer.
-- Aucune jointure, aucune règle métier. Une clé absente donne NULL (get_json_object).
-- Non repris (lisibles dans le bronze) : geom (égal à latitude et longitude), services (doublon de
-- services_service), horaires_jour (dérivé de horaires), les 12 clés <carburant>_rupture_* (J7).
with source as (

    select * from {{ source('carburant_bronze', 'station_etat_flux') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['station_id', 'collecte_ts']) }} as station_etat_id,
    station_id,
    collecte_ts,
    etat_hash,

    -- identification et localisation
    get_json_object(etat, '$.adresse') as adresse,
    get_json_object(etat, '$.cp') as code_postal,
    get_json_object(etat, '$.ville') as ville,
    get_json_object(etat, '$.code_departement') as code_departement,
    get_json_object(etat, '$.departement') as departement_nom,
    get_json_object(etat, '$.code_region') as code_region,
    get_json_object(etat, '$.region') as region_nom,
    -- chaînes multipliées par 100 000, entières ou décimales ; non numérique : NULL
    try_cast(get_json_object(etat, '$.latitude') as double) / 100000 as latitude,
    try_cast(get_json_object(etat, '$.longitude') as double) / 100000 as longitude,

    -- indicateurs scalaires
    get_json_object(etat, '$.pop') as type_route_code,
    case get_json_object(etat, '$.horaires_automate_24_24')
        when 'Oui' then true
        when 'Non' then false
    end as automate_24_24,

    -- JSON et listes bruts (exploités en J7)
    get_json_object(etat, '$.horaires') as horaires_json,
    get_json_object(etat, '$.services_service') as services_json,
    get_json_object(etat, '$.rupture') as ruptures_json,
    get_json_object(etat, '$.carburants_disponibles') as carburants_disponibles_json,
    get_json_object(etat, '$.carburants_indisponibles') as carburants_indisponibles_json,
    get_json_object(etat, '$.carburants_rupture_definitive') as carburants_rupture_definitive_liste,
    get_json_object(etat, '$.carburants_rupture_temporaire') as carburants_rupture_temporaire_liste,

    _fichier_source as fichier_source,
    _charge_ts as charge_ts
from source
