{% snapshot customers_snapshot %}

{{
    config(
        target_schema = var('target_schema') | replace('active', 'snapshots'),
        unique_key    = 'customer_id',
        strategy      = 'timestamp',
        updated_at    = 'updated_at',
        invalidate_hard_deletes = true,
    )
}}

{% set staging_schema = var('target_schema') | replace('active', 'staging') %}

SELECT
    customer_id,
    email,
    city,
    created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ staging_schema }}.customers_staged

{% endsnapshot %}