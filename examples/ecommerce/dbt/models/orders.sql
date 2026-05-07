{{
    config(
        materialized     = 'incremental',
        unique_key       = 'order_id',
        on_schema_change = 'append_new_columns',
    )
}}

{% set staging_schema = var('target_schema') | replace('active', 'staging') %}

SELECT
    order_id,
    customer_id,
    total_amount,
    discount_amount,
    promo_code,
    status,
    category,
    created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ staging_schema }}.orders_staged

{% if is_incremental() %}
WHERE order_id NOT IN (SELECT order_id FROM {{ this }})
{% endif %}