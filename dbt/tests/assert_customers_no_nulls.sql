-- Test: no null customer_ids in the active table
SELECT customer_id
FROM {{ var('target_schema') }}.customers
WHERE customer_id IS NULL