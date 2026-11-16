{#
  Lecture seule : colonnes et types réels de tables du projet (métadonnées, aucune écriture).
  Refuse tout schéma hors carburant_* (les autres schémas du catalogue appartiennent à un autre projet).
  make dbt ARGS="run-operation decrire_colonnes"
  make dbt ARGS="run-operation decrire_colonnes --args '{schema: carburant_bronze, tables: [prix_flux]}'"
#}
{% macro decrire_colonnes(schema='carburant_bronze', tables=['prix_flux', 'station_etat_flux']) %}
    {% if not schema.startswith('carburant_') %}
        {{ exceptions.raise_compiler_error('schéma refusé (hors carburant_*) : ' ~ schema) }}
    {% endif %}
    {% for table in tables %}
        {% set relation = adapter.get_relation(database=target.database, schema=schema, identifier=table) %}
        {% if relation is none %}
            {{ log('COLONNES ' ~ schema ~ '.' ~ table ~ ' : table absente', info=True) }}
        {% else %}
            {% for colonne in adapter.get_columns_in_relation(relation) %}
                {{ log('COLONNES ' ~ schema ~ '.' ~ table ~ ' | ' ~ colonne.name ~ ' | ' ~ colonne.dtype, info=True) }}
            {% endfor %}
        {% endif %}
    {% endfor %}
{% endmacro %}
