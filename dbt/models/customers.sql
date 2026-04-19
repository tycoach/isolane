-- ─────────────────────────────────────────────────────────────────
-- isolane/dbt/models/customers.sql
--
-- Incremental model for customer records.
{{
    config(
        materialized     = 'incremental',
        unique_key       = 'customer_id',
        on_schema_change = 'append_new_columns',
    )
}}

{% set staging_schema = var('target_schema') | replace('active', 'staging') %}

SELECT
    customer_id,
    email,
    city,
    created_at,
    CURRENT_TIMESTAMP AS updated_at,
    TRUE              AS is_current
FROM {{ staging_schema }}.customers_staged

{% if is_incremental() %}
WHERE customer_id NOT IN (
    SELECT customer_id FROM {{ this }}
)
{% endif %}