-- Taux de communes (COM et ARM) avec niveau_vie_median_eur renseigné. Mesure du 17/11 sur le staging : 88,31 %
-- (30 839 sur 34 920 ; 107 COM sans Filosofi, le reste masqué). Warn sous la mesure moins 1 point : 87,31 %.
-- Une ligne rendue = test en échec.
{{ config(severity='warn') }}

with mesure as (

    select count(niveau_vie_median_eur) * 100.0 / count(*) as taux_pct
    from {{ ref('dim_commune') }}

)

select taux_pct
from mesure
where taux_pct < 87.31
