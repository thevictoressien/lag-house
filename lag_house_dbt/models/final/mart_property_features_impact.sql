{{ config(materialized='table') }}

WITH rental_features AS (
    SELECT
        location,
        AVG(CASE WHEN is_furnished = 'yes' THEN price END) AS avg_furnished_price,
        AVG(CASE WHEN is_serviced = 'yes' THEN price END) AS avg_serviced_price,
        AVG(CASE WHEN is_shared = 'yes' THEN price END) AS avg_shared_price,
        COUNT(*) AS total_count,
        SUM(CASE WHEN is_furnished = 'yes' THEN 1 ELSE 0 END) AS furnished_count,
        SUM(CASE WHEN is_serviced = 'yes' THEN 1 ELSE 0 END) AS serviced_count,
        SUM(CASE WHEN is_shared = 'yes' THEN 1 ELSE 0 END) AS shared_count
    FROM {{ ref('int_rental_metrics') }}
    GROUP BY location
),

sale_features AS (
    SELECT
        location,
        AVG(CASE WHEN is_furnished = 'yes' THEN price END) AS avg_furnished_price,
        AVG(CASE WHEN is_serviced = 'yes' THEN price END) AS avg_serviced_price,
        AVG(CASE WHEN is_shared = 'yes' THEN price END) AS avg_shared_price,
        COUNT(*) AS total_count,
        SUM(CASE WHEN is_furnished = 'yes' THEN 1 ELSE 0 END) AS furnished_count,
        SUM(CASE WHEN is_serviced = 'yes' THEN 1 ELSE 0 END) AS serviced_count,
        SUM(CASE WHEN is_shared = 'yes' THEN 1 ELSE 0 END) AS shared_count
    FROM {{ ref('int_sale_metrics') }}
    GROUP BY location
)

SELECT
    'Rental' AS listing_type,
    location,
    avg_furnished_price,
    avg_serviced_price,
    avg_shared_price,
    furnished_count / total_count AS furnished_ratio,
    serviced_count / total_count AS serviced_ratio,
    shared_count / total_count AS shared_ratio
FROM rental_features

UNION ALL

SELECT
    'Sale' AS listing_type,
    location,
    avg_furnished_price,
    avg_serviced_price,
    avg_shared_price,
    furnished_count / total_count AS furnished_ratio,
    serviced_count / total_count AS serviced_ratio,
    shared_count / total_count AS shared_ratio
FROM sale_features
