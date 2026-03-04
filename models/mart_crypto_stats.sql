{{ config(materialized='table') }}

-- Agrupamos todos los datos por moneda para sacar KPIs globales
SELECT
    coin_name,
    -- Contamos cuántas capturas tenemos en total
    COUNT(*) AS total_samples,
    -- Buscamos el precio mínimo registrado
    MIN(price_usd) AS min_price,
    -- Buscamos el precio máximo registrado
    MAX(price_usd) AS max_price,
    -- Calculamos el promedio de todos los precios
    ROUND(AVG(price_usd), 2) AS avg_price,
    -- Sacamos la última vez que se actualizó esta moneda
    MAX(updated_at_timestamp) AS last_updated
FROM {{ ref('stg_crypto_prices') }}
GROUP BY 1