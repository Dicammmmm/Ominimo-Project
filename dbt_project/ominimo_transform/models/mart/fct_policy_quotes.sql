WITH policies AS (
    SELECT * FROM {{ ref('stg_policies') }}
),

quotes AS (
    SELECT * FROM {{ ref('stg_quotes') }}
)

SELECT
    p.policy_id,
    p.quote_id,
    p.customer_id,
    p.created_at AS policy_created_at,
    p.vehicle_make,
    p.vehicle_model,
    p.vehicle_year,
    p.status AS policy_status,
    q.product,
    q.price,
    q.technical_price,
    q.created_at AS quote_created_at,
    p.source_file AS policy_source_file,
    q.source_file AS quote_source_file,
    p.loaded_at AS policy_loaded_at,
    q.loaded_at AS quote_loaded_at
    
FROM policies p
LEFT JOIN quotes q ON p.quote_id = q.quote_id