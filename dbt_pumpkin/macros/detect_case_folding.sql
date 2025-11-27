{% macro detect_case_folding() %}
    {# Execute test query to detect database case folding behavior #}
    {% set query %}
        SELECT
            NULL AS test_lower,
            NULL AS TEST_UPPER
    {% endset %}

    {% set result_set = run_query(query) %}

    {# Extract column names from result metadata #}
    {% set columns = [] %}
    {% if result_set %}
        {% for column_name in result_set.column_names %}
            {% do columns.append(column_name) %}
        {% endfor %}
    {% endif %}

    {% set result = {
        'columns': columns
    } %}

    {{ log(tojson({'detect_case_folding': result})) }}
{% endmacro %}
