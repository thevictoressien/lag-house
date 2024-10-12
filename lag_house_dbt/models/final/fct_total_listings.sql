{{ config(materialized='table') }}

SELECT
    bedrooms,
    sum(listing_count) AS total_listings,
FROM {{ ref('int_bedroom_bathroom_analysis') }}
GROUP BY 1