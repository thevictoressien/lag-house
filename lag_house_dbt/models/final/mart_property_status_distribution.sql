{{ config(materialized='table') }}

WITH rental_status AS (
    SELECT
        location,
        property_type,
        status,
        COUNT(*) AS status_count
    FROM {{ ref('int_rental_metrics') }}
    GROUP BY location, property_type, status
),

sale_status AS (
    SELECT
        location,
        property_type,
        status,
        COUNT(*) AS status_count
    FROM {{ ref('int_sale_metrics') }}
    GROUP BY location, property_type, status
)

SELECT
    'Rental' AS listing_type,
    location,
    property_type,
    status,
    status_count
FROM rental_status

UNION ALL

SELECT
    'Sale' AS listing_type,
    location,
    property_type,
    status,
    status_count
FROM sale_status
