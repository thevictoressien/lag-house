{{ config(materialized='table') }}

WITH rental_prices AS (
    SELECT
        location,
        property_type,
        AVG(price) AS avg_rental_price
    FROM {{ ref('int_rental_metrics') }}
    GROUP BY location, property_type
),

sale_prices AS (
    SELECT
        location,
        property_type,
        AVG(price) AS avg_sale_price
    FROM {{ ref('int_sale_metrics') }}
    GROUP BY location, property_type
)

SELECT
    COALESCE(r.location, s.location) AS location,
    COALESCE(r.property_type, s.property_type) AS property_type,
    r.avg_rental_price,
    s.avg_sale_price
FROM rental_prices r
FULL OUTER JOIN sale_prices s
    ON r.location = s.location AND r.property_type = s.property_type
