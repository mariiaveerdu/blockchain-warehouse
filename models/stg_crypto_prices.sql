{{ config(materialized='table') }}

WITH base AS (
    -- Probamos llamando a la tabla directamente por su nombre en el esquema main
    SELECT * FROM main.raw_crypto 
)

SELECT
    UPPER(coin_id) AS coin_name,
    price_usd,
    epoch_ms(cast(last_updated_at * 1000 as BIGINT)) AS updated_at_timestamp,
    extracted_at AS ingestion_time
FROM base