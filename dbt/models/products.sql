{{
    config(
        materialized     = 'incremental',
        unique_key       = 'product_id',
        on_schema_change = 'append_new_columns',
    )
}}

{% set staging_schema = var('target_schema') | replace('active', 'staging') %}

SELECT
    product_id,
    name,
    category,
    price,
    stock_count,
    description,
    image_url,
    created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ staging_schema }}.products_staged

{% if is_incremental() %}
WHERE product_id NOT IN (SELECT product_id FROM {{ this }})
{% endif %}