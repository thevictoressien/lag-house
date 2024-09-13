{{ config(materialized='table') }}

WITH rental_affordability AS (
    SELECT
        location,
        AVG(price) AS avg_price,
        AVG(total_area) AS avg_total_area,
        AVG(covered_area) AS avg_covered_area,
        COUNT(*) AS property_count,
        STRING_AGG(DISTINCT property_type, ', ') AS property_types
    FROM {{ ref('int_rental_metrics') }}
    GROUP BY location
),

sale_affordability AS (
    SELECT
        location,
        AVG(price) AS avg_price,
        AVG(total_area) AS avg_total_area,
        AVG(covered_area) AS avg_covered_area,
        COUNT(*) AS property_count,
        STRING_AGG(DISTINCT property_type, ', ') AS property_types
    FROM {{ ref('int_sale_metrics') }}
    GROUP BY location
)

SELECT
    'Rental' AS listing_type,
    location,
    avg_price,
    avg_total_area,
    avg_covered_area,
    property_count,
    property_types,
    RANK() OVER (ORDER BY avg_price ASC) AS affordability_rank
FROM rental_affordability

UNION ALL

SELECT
    'Sale' AS listing_type,
    location,
    avg_price,
    avg_total_area,
    avg_covered_area,
    property_count,
    property_types,
    RANK() OVER (ORDER BY avg_price ASC) AS affordability_rank
FROM sale_affordability
