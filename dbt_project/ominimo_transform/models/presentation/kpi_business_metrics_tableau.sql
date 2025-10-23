{{ config(
    materialized='table'
) }}

SELECT
    policy_id,
    quote_id,
    customer_id,
    policy_created_at,
    quote_created_at,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    policy_status,
    product,
    price,
    technical_price,
    price - technical_price AS price_markup,
    
    CASE WHEN policy_id IS NOT NULL THEN 1 ELSE 0 END AS converted_flag

FROM {{ ref('fct_policy_quotes') }}