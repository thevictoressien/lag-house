{{ config(materialized='table') }}

SELECT
    lga,
    bedrooms,
    ROUND(AVG(CASE WHEN listing_type = 'rent' THEN avg_price ELSE NULL END), 2) AS avg_rent_price,
    ROUND(AVG(CASE WHEN listing_type = 'sale' THEN avg_price ELSE NULL END), 2) AS avg_sale_price,
    SUM(listing_count) AS num_listings,
FROM {{ ref('int_bedroom_bathroom_analysis') }}
GROUP BY 1,2
ORDER BY bedrooms
