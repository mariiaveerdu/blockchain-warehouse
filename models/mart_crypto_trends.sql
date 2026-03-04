{{ config(materialized='table') }}

WITH base AS (
    -- Traemos los datos limpios que ya tenías
    SELECT * FROM {{ ref('stg_crypto_prices') }}
),

calc_trends AS (
    SELECT
        coin_name,
        price_usd,
        updated_at_timestamp,
        -- LAG mira la fila de "atrás" (la anterior) para la misma moneda
        LAG(price_usd) OVER (PARTITION BY coin_name ORDER BY updated_at_timestamp) AS previous_price
    FROM base
)

SELECT
    coin_name,
    price_usd,
    previous_price,
    -- Calculamos la variación: ((Actual - Anterior) / Anterior) * 100
    ROUND(
        CASE 
            WHEN previous_price IS NULL THEN 0 
            ELSE ((price_usd - previous_price) / previous_price) * 100 
        END, 
        4
    ) AS pct_change,
    updated_at_timestamp
FROM calc_trends
ORDER BY updated_at_timestamp DESC