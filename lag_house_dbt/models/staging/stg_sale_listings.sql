{{ config(materialized='view') }}

SELECT
    location,
    status,
    CAST(bedrooms AS INT64) AS bedrooms,
    CAST(bathrooms AS INT64) AS bathrooms,
    CAST(toilets AS INT64) AS toilets,
    property_type,
    is_furnished,
    is_serviced,
    is_shared,
    CAST(total_area AS FLOAT64) AS total_area,
    CAST(covered_area AS FLOAT64) AS covered_area,
    CAST(price AS FLOAT64) AS price,
    currency
FROM {{ source('raw', 'lagos_for_sale_listings_raw') }}
