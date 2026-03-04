{{ config(materialized='table') }}

WITH prices AS (
    SELECT * FROM {{ ref('stg_crypto_prices') }}
),

totals AS (
    -- Calculamos el total de la "cartera" para sacar porcentajes
    SELECT 
        SUM(price_usd) AS total_market_value,
        MAX(price_usd) AS max_price,
        AVG(price_usd) AS average_price
    FROM prices
)

SELECT
    p.coin_name,
    p.price_usd,
    -- Calculamos qué porcentaje del valor total representa cada moneda
    ROUND((p.price_usd / t.total_market_value) * 100, 2) AS percent_of_portfolio,
    -- Etiquetamos si es una moneda "Cara" o "Barata" respecto al promedio
    CASE 
        WHEN p.price_usd > t.average_price THEN 'Above Average'
        ELSE 'Below Average'
    END AS price_category,
    p.updated_at_timestamp
FROM prices p
CROSS JOIN totals t