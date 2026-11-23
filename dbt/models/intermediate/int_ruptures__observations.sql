-- Ruptures dépliées : une ligne par (station, collecte_ts, carburant) quand la version de l'état porte une rupture
-- pour ce carburant (rupture_debut_ts non NULL ; le type est alors toujours présent, J8-M1). Passe de 6 carburants
-- en colonnes à des lignes avant l'agrégation en événements (fct_ruptures).
with etats as (

    select * from {{ ref('stg_carburants__station_etats') }}

)

{% for carburant in ['e10', 'e85', 'gazole', 'gplc', 'sp95', 'sp98'] %}
select
    station_id,
    collecte_ts,
    '{{ carburant }}' as carburant,
    {{ carburant }}_rupture_debut_ts as rupture_debut_ts,
    {{ carburant }}_rupture_type as rupture_type
from etats
where {{ carburant }}_rupture_debut_ts is not null
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
