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
    {{ log(tojson(result), info=True) }}
{% endmacro %}

{% macro get_resource_root_paths() %}
    {% set result = {} %}
    {% for resource_type, resource_paths in var('get_resource_root_paths_args').items() %}
        {% do result.update({resource_type: resource_paths}) %}
    {% endfor %}
    {{ log(tojson(result), info=True) }}
{% endmacro %}
