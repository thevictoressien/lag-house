{{ config(materialized='table') }}

SELECT
    location,
    property_type,
    status,
    bedrooms,
    bathrooms,
    toilets,
    is_furnished,
    is_serviced,
    is_shared,
    total_area,
    covered_area,
    price,
    currency,
    COUNT(*) OVER (PARTITION BY location) AS location_count
FROM {{ ref('stg_sale_listings') }}
