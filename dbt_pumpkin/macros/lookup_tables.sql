{% macro lookup_tables() %}
    {% for resource_id, database_schema_identifier in var('lookup_tables_args').items() %}
        {% set database, schema, identifier = database_schema_identifier %}
        {% set relation = adapter.get_relation(database, schema, identifier) %}

        {% set result = {
            'resource_id': resource_id,
            'columns': none
        } %}

        {% if relation is not none %}
            {% set columns = [] %}
            {% for column in adapter.get_columns_in_relation(relation) %}
                {% do columns.append({
                    'name': column.name,
                    'is_string': column.is_string(),
                    'is_numeric': column.is_numeric(),
                    'dtype': column.dtype,
                    'data_type': column.data_type,
                }) %}
            {% endfor %}
            {% do result.update({'columns': columns}) %}
        {% endif %}

        {{ log(tojson( {'lookup_tables': result} )) }}
    {% endfor %}
{% endmacro %}
