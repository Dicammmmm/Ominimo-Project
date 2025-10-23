WITH source AS (
    SELECT * FROM {{ source('raw', 'quotes_raw') }}
)

SELECT
    quote_id::VARCHAR(16) AS quote_id,
    product::VARCHAR(16) AS product,
    price::DOUBLE PRECISION AS price,
    technical_price::DOUBLE PRECISION AS technical_price,
    created_at::TIMESTAMP AS created_at,
    'quotes_raw.parquet'::VARCHAR(64) AS source_file,
    CURRENT_TIMESTAMP AS loaded_at
FROM
    source