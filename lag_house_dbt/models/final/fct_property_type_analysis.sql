{{ config(materialized='table') }}

SELECT
    property_type,
    ROUND(AVG(CASE WHEN listing_type = 'rent' THEN avg_price ELSE NULL END), 2) AS avg_rent_price,
    ROUND(AVG(CASE WHEN listing_type = 'sale' THEN avg_price ELSE NULL END), 2) AS avg_sale_price,
    SUM(listing_count) AS num_listings,
    -- AVG(median_price) AS avg_price,
FROM {{ ref('int_property_metrics') }}
GROUP BY 1
