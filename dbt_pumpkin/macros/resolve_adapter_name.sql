{% macro resolve_adapter_name() %}
    {% set adapter_name = adapter.dispatch('resolve_adapter_name')() %}
    {{ log(tojson( {'resolve_adapter_name': adapter_name} )) }}
{% endmacro %}

{% macro default__resolve_adapter_name() -%}
    {{ return('default') }}
{%- endmacro %}

{% macro snowflake__resolve_adapter_name() %}
    {{ return('snowflake') }}
{% endmacro %}

{% macro duckdb__resolve_adapter_name() %}
    {{ return('duckdb') }}
{% endmacro %}
