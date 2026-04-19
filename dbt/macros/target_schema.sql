{% macro get_staging_schema() %}
    {{ var('target_schema') | replace('active', 'staging') }}
{% endmacro %}

{% macro get_snapshot_schema() %}
    {{ var('target_schema') | replace('active', 'snapshots') }}
{% endmacro %}