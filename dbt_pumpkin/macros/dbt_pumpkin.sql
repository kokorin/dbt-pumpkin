{% macro get_column_types() %}
    {% set result = {} %}
    {% for id, database_schema_identifier in var('get_column_types_args').items() %}
        {% set database, schema, identifier = database_schema_identifier %}
        {% set relation = adapter.get_relation(database, schema, identifier) %}
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
            {% do result.update({id: columns}) %}
        {% endif %}
    {% endfor %}
    {{ log(tojson(result)) }}
{% endmacro %}
