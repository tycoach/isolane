-- isolane/dbt/models/events.sql
-- SCD Type 1 incremental model for events.
{{
    config(
        materialized     = 'incremental',
        unique_key       = 'event_id',
        on_schema_change = 'append_new_columns',
    )
}}

{% set staging_schema = var('target_schema') | replace('active', 'staging') %}

SELECT
    event_id,
    event_type,
    user_id,
    occurred_at,
    CURRENT_TIMESTAMP AS processed_at
FROM {{ staging_schema }}.events_staged

{% if is_incremental() %}
WHERE event_id NOT IN (
    SELECT event_id FROM {{ this }}
)
{% endif %}