-- Prix par commune, carburant et jour : prix en vigueur à 12:00 de Paris, jours exploitables seulement.
with versions as (
    select
        station_id, carburant, prix_eur_litre, valide_depuis_ts, valide_jusqu_ts,
        premiere_observation_ts, commune_code, est_plausible
    from {{ ref('fct_prix') }}
),

ruptures as (
    select station_id, carburant, rupture_debut_ts, fin_observee_ts
    from {{ ref('fct_ruptures') }}
),

communes as (
    select commune_code, niveau_vie_median_eur
    from {{ ref('dim_commune') }}
),

-- Un passage de collecte = un instant où au moins une version a été observée.
passages as (
    select distinct
        premiere_observation_ts as collecte_ts,
        to_date(from_utc_timestamp(premiere_observation_ts, 'Europe/Paris')) as jour
    from versions
),

passages_midi as (
    select
        jour,
        collecte_ts,
        to_utc_timestamp(
            cast(concat(cast(jour as string), ' 12:00:00') as timestamp), 'Europe/Paris'
        ) as midi_utc
    from passages
),

encadrement as (
    select
        jour,
        midi_utc,
        max(case when collecte_ts <= midi_utc then collecte_ts end) as dernier_avant_midi,
        min(case when collecte_ts > midi_utc then collecte_ts end) as premier_apres_midi
    from passages_midi
    group by jour, midi_utc
),

jours_exploitables as (
    select jour, midi_utc
    from encadrement
    where dernier_avant_midi is not null
        and premier_apres_midi is not null
        and unix_timestamp(premier_apres_midi) - unix_timestamp(dernier_avant_midi)
            <= {{ var('ecart_midi_max_minutes') }} * 60
),

en_vigueur as (
    select j.jour, j.midi_utc, v.station_id, v.carburant, v.prix_eur_litre, v.commune_code
    from jours_exploitables as j
    inner join versions as v
        on v.valide_depuis_ts <= j.midi_utc
        and (v.valide_jusqu_ts is null or v.valide_jusqu_ts > j.midi_utc)
    where v.est_plausible
        and v.commune_code is not null
        and datediff(j.midi_utc, v.valide_depuis_ts) <= {{ var('prix_age_max_jours') }}
),

hors_rupture as (
    select e.*
    from en_vigueur as e
    left anti join ruptures as r
        on e.station_id = r.station_id
        and e.carburant = r.carburant
        and r.rupture_debut_ts <= e.midi_utc
        and (r.fin_observee_ts is null or r.fin_observee_ts > e.midi_utc)
),

agrege as (
    select
        commune_code,
        carburant,
        jour,
        count(*) as nb_stations,
        cast(percentile(prix_eur_litre, 0.5) as decimal(7, 4)) as prix_median_eur,
        min(prix_eur_litre) as prix_min_eur,
        max(prix_eur_litre) as prix_max_eur
    from hors_rupture
    group by commune_code, carburant, jour
)

select
    a.commune_code,
    a.carburant,
    a.jour,
    a.nb_stations,
    a.prix_median_eur,
    a.prix_min_eur,
    a.prix_max_eur,
    c.niveau_vie_median_eur
from agrege as a
left join communes as c
    on a.commune_code = c.commune_code
