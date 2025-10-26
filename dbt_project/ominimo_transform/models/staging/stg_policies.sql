{{ 
    config(
        materialized='incremental',
        unique_key='policy_id'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'policies_raw') }}
)

SELECT
    policy_id::VARCHAR(16) AS policy_id,
    quote_id::VARCHAR(16) AS quote_id,
    customer_id::VARCHAR(16) AS customer_id,
    vehicle_make::VARCHAR(16) AS vehicle_make,
    vehicle_model::VARCHAR(16) AS vehicle_model,
    vehicle_year::INTEGER AS vehicle_year,
    status::VARCHAR(16) AS status,
    source_file::VARCHAR(64) AS source_file,
    created_at::TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS loaded_at
FROM
    source

{% if is_incremental() %}
    WHERE created_at::TIMESTAMP > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}