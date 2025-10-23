{{ config(
    materialized='table'
) }}

WITH policy_quotes AS (
    SELECT * FROM {{ ref('fct_policy_quotes') }}
)

SELECT
    product,
    policy_status,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    COUNT(DISTINCT quote_id) AS total_quotes,
    COUNT(DISTINCT policy_id) AS total_policies,
    ROUND(AVG(price), 2) AS avg_premium,
    ROUND(AVG(technical_price), 2) AS avg_technical_price,
    ROUND(COUNT(DISTINCT policy_id)::DOUBLE / NULLIF(COUNT(DISTINCT quote_id), 0), 4) AS conversion_ratio,
    MIN(price) AS min_premium,
    MAX(price) AS max_premium,
    ROUND(SUM(price), 2) AS total_premium_value
    
FROM policy_quotes
GROUP BY product, policy_status, vehicle_make, vehicle_model, vehicle_year