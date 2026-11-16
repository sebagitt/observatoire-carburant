{#
  Schéma personnalisé utilisé tel quel (sans le préfixe du schéma de la cible).
  Sans cette macro, dbt produirait carburant_silver_carburant_silver.
  Sans schéma personnalisé : schéma de la cible (carburant_silver).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
