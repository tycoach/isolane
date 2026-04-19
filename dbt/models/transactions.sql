-- isolane/dbt/models/transactions.sql
-- SCD Type 1 incremental model for transactions.
{{
    config(
        materialized     = 'incremental',
        unique_key       = 'transaction_id',
        on_schema_change = 'append_new_columns',
    )
}}

{% set staging_schema = var('target_schema') | replace('active', 'staging') %}

SELECT
    transaction_id,
    amount,
    status,
    created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ staging_schema }}.transactions_staged

{% if is_incremental() %}
WHERE transaction_id NOT IN (
    SELECT transaction_id FROM {{ this }}
)
{% endif %}