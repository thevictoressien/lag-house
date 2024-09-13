{{ config(materialized='table') }}

SELECT
    'Rental' AS listing_type,
    location,
    total_area,
    covered_area,
    price
FROM {{ ref('int_rental_metrics') }}

UNION ALL

SELECT
    'Sale' AS listing_type,
    location,
    total_area,
    covered_area,
    price
FROM {{ ref('int_sale_metrics') }}
