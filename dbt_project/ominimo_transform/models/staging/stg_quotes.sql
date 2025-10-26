{{
    config(
        materialized='incremental',
        unique_key='quote_id'
    )
}}
WITH source AS (
    SELECT * FROM {{ source('raw', 'quotes_raw') }}
)

SELECT
    quote_id::VARCHAR(16) AS quote_id,
    product::VARCHAR(16) AS product,
    price::DOUBLE PRECISION AS price,
    technical_price::DOUBLE PRECISION AS technical_price,
    'quotes_raw.parquet'::VARCHAR(64) AS source_file,
    created_at::TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS loaded_at
    'test' AS test
FROM
    source

{% if is_incremental() %}
    WHERE created_at::TIMESTAMP > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
