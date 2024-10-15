{{ config(materialized='table') }}

SELECT
    lga,
    ROUND(AVG(CASE WHEN listing_type = 'rent' THEN avg_price ELSE NULL END), 2) AS avg_rent_price,
    ROUND(AVG(CASE WHEN listing_type = 'sale' THEN avg_price ELSE NULL END), 2) AS avg_sale_price,
    SUM(listing_count) as num_listings
FROM {{ ref('int_property_metrics') }}
GROUP BY 1
