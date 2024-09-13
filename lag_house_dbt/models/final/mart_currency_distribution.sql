{{ config(materialized='table') }}

WITH rental_currency AS (
    SELECT
        location,
        currency,
        COUNT(*) AS currency_count
    FROM {{ ref('int_rental_metrics') }}
    GROUP BY location, currency
),

sale_currency AS (
    SELECT
        location,
        currency,
        COUNT(*) AS currency_count
    FROM {{ ref('int_sale_metrics') }}
    GROUP BY location, currency
)

SELECT
    'Rental' AS listing_type,
    location,
    currency,
    currency_count
FROM rental_currency

UNION ALL

SELECT
    'Sale' AS listing_type,
    location,
    currency,
    currency_count
FROM sale_currency
