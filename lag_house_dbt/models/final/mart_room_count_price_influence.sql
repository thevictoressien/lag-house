{{ config(materialized='table') }}

SELECT
    'Rental' AS listing_type,
    location,
    bedrooms,
    bathrooms,
    toilets,
    price
FROM {{ ref('int_rental_metrics') }}

UNION ALL

SELECT
    'Sale' AS listing_type,
    location,
    bedrooms,
    bathrooms,
    toilets,
    price
FROM {{ ref('int_sale_metrics') }}
